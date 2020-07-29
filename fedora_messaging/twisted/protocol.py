# This file is part of fedora_messaging.
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
"""
The core Twisted interface, a protocol represent a specific connection to the
AMQP broker.

The :class:`.FedoraMessagingProtocolV2` has replaced the deprecated
:class:`.FedoraMessagingProtocolV2`. This class inherits the
:class:`pika.adapters.twisted_connection.TwistedProtocolConnection` class and
adds a few additional methods.

When combined with the :class:`fedora_messaging.twisted.factory.FedoraMessagingFactory`
class, it's easy to create AMQP consumers that last across connections.

For an overview of Twisted clients, see the `Twisted client documentation
<https://twistedmatrix.com/documents/current/core/howto/clients.html#protocol>`_.
"""
from __future__ import absolute_import

from collections import namedtuple
import uuid
import warnings
import logging

import pika
from pika.adapters.twisted_connection import TwistedProtocolConnection
from pika.exceptions import ChannelClosedByClient
from twisted.internet import defer, error, threads, reactor
from twisted.python import log as _legacy_twisted_log
from twisted.python.failure import Failure

from .consumer import Consumer as ConsumerV2
from .. import config
from ..message import get_message
from ..exceptions import (
    Nack,
    Drop,
    HaltConsumer,
    ValidationError,
    NoFreeChannels,
    BadDeclaration,
    PublishReturned,
    ConnectionException,
    ConsumerCanceled,
    PermissionException,
)


_std_log = logging.getLogger(__name__)


def _add_timeout(deferred, timeout):
    """
    Add a timeout to the given deferred. This is designed to work with both old
    Twisted and versions of Twisted with the addTimeout API. This is
    exclusively to support EL7.

    The deferred will errback with a :class:`defer.CancelledError` if the
    version of Twisted being used doesn't have the
    ``defer.Deferred.addTimeout`` API, otherwise it will errback with the
    normal ``error.TimeoutError``
    """
    try:
        deferred.addTimeout(timeout, reactor)
    except AttributeError:
        # Twisted 12.2 (in EL7) does not have the addTimeout API, so make do with
        # the slightly more annoying approach of scheduling a call to cancel which
        # is then canceled if the deferred succeeds before the timeout is up.
        delayed_cancel = reactor.callLater(timeout, deferred.cancel)

        def cancel_cancel_call(result):
            """Halt the delayed call to cancel if the deferred fires before the timeout."""
            if not delayed_cancel.called:
                delayed_cancel.cancel()
            return result

        deferred.addBoth(cancel_cancel_call)


class FedoraMessagingProtocolV2(TwistedProtocolConnection):
    """A Twisted Protocol for the Fedora Messaging system.

    This protocol builds on the generic pika AMQP protocol to add calls
    specific to the Fedora Messaging implementation.

    Attributes:
        factory: The :class:`Factory` object that created this protocol. This
        is set by the factory that creates this protocol.

    Args:
        parameters (pika.ConnectionParameters): The connection parameters.
        confirms (bool): If True, all outgoing messages will require a
            confirmation from the server, and the Deferred returned from
            the publish call will wait for that confirmation.
    """

    name = u"FedoraMessaging:Protocol"

    def __init__(self, parameters, confirms=True):
        TwistedProtocolConnection.__init__(self, parameters)
        self._confirms = confirms
        self._channel = None
        self._publish_channel = None
        # Map queue names to fedora_messaging.twisted.consumer.Consumer objects
        self._consumers = {}
        self.factory = None

    @defer.inlineCallbacks
    def _allocate_channel(self):
        """
        Allocate a new AMQP channel.

        Raises:
            NoFreeChannels: If this connection has reached its maximum number of channels.
            ConncetionException: If this connection is already closed.
        """
        try:
            channel = yield self.channel()
        except pika.exceptions.NoFreeChannels:
            raise NoFreeChannels()
        except pika.exceptions.ConnectionWrongStateError as e:
            raise ConnectionException(reason=e)
        _std_log.debug("Created AMQP channel id %d", channel.channel_number)
        if self._confirms:
            yield channel.confirm_delivery()
        defer.returnValue(channel)

    @defer.inlineCallbacks
    def connectionReady(self, res=None):
        """
        Callback invoked when the AMQP connection is ready (when self.ready fires).

        This API is not meant for users.

        Args:
            res: This is an unused argument that provides compatibility with Pika
                versions lower than 1.0.0.
        """
        self._channel = yield self._allocate_channel()

    @defer.inlineCallbacks
    def _read(self, queue_object, consumer):
        """
        The loop that reads from the message queue and calls the consumer callback
        wrapper.

        Serialized Processing
        ---------------------
        This loop processes messages serially. This is because a second
        ``queue_object.get()`` operation can only occur after the Deferred from
        ``self._on_message`` completes. Thus, we can be sure that callbacks
        never run concurrently in two different threads.

        This is done rather than saturating the Twisted thread pool as the
        documentation for callbacks (in fedmsg and here) has never indicated
        that they are not thread-safe. In the future we can add a flag for users
        who are confident in their ability to write thread-safe code.


        Gracefully Halting
        ------------------
        This is a loop that only exits when the consumer._running variable is
        set to False. The call to cancel will set this to false, as will the
        call to :meth:`pauseProducing`. These calls will then wait for the
        Deferred from this function to call back in order to ensure the message
        finishes processing.

        The Deferred object only completes when this method returns, so we need
        to periodically check the status of consumer._running. That's why
        there's a short timeout on the call to ``queue_object.get``.

        queue_object (pika.adapters.twisted_connection.ClosableDeferredQueue):
            The AMQP queue the consumer is bound to.
        consumer (dict): A dictionary describing the consumer for the given
            queue_object.
        """
        while consumer._running:
            try:
                deferred_get = queue_object.get()
                _add_timeout(deferred_get, 1)
                channel, delivery_frame, properties, body = yield deferred_get
            except (defer.TimeoutError, defer.CancelledError):
                continue

            _std_log.debug(
                "Message arrived with delivery tag %s for %r",
                delivery_frame.delivery_tag,
                consumer._tag,
            )
            try:
                message = get_message(delivery_frame.routing_key, properties, body)
                message.queue = consumer.queue
            except ValidationError:
                _std_log.warning(
                    "Message id %s did not pass validation; ignoring message",
                    properties.message_id,
                )
                yield channel.basic_nack(
                    delivery_tag=delivery_frame.delivery_tag, requeue=False
                )
                continue

            try:
                _std_log.info(
                    "Consuming message from topic %s (message id %s)",
                    message.topic,
                    properties.message_id,
                )
                yield threads.deferToThread(consumer.callback, message)
            except Nack:
                _std_log.warning(
                    "Returning message id %s to the queue", properties.message_id
                )
                yield channel.basic_nack(
                    delivery_tag=delivery_frame.delivery_tag, requeue=True
                )
            except Drop:
                _std_log.warning(
                    "Consumer requested message id %s be dropped", properties.message_id
                )
                yield channel.basic_nack(
                    delivery_tag=delivery_frame.delivery_tag, requeue=False
                )
            except HaltConsumer as e:
                _std_log.info(
                    "Consumer indicated it wishes consumption to halt, shutting down"
                )
                if e.requeue:
                    yield channel.basic_nack(
                        delivery_tag=delivery_frame.delivery_tag, requeue=True
                    )
                else:
                    yield channel.basic_ack(delivery_tag=delivery_frame.delivery_tag)
                raise e
            except Exception as e:
                _std_log.exception(
                    "Received unexpected exception from consumer %r", consumer
                )
                yield channel.basic_nack(delivery_tag=0, multiple=True, requeue=True)
                raise e
            else:
                _std_log.info(
                    "Successfully consumed message from topic %s (message id %s)",
                    message.topic,
                    properties.message_id,
                )
                yield channel.basic_ack(delivery_tag=delivery_frame.delivery_tag)

    @defer.inlineCallbacks
    def publish(self, message, exchange):
        """
        Publish a :class:`fedora_messaging.message.Message` to an `exchange`_
        on the message broker.

        Args:
            message (message.Message): The message to publish.
            exchange (str): The name of the AMQP exchange to publish to

        Raises:
            NoFreeChannels: If there are no available channels on this connection.
                If this occurs, you can either reduce the number of consumers on this
                connection or create an additional connection.
            PublishReturned: If the broker rejected the message. This can happen if
                there are resource limits that have been reached (full disk, for example)
                or if the message will be routed to 0 queues and the exchange is set to
                reject such messages.

        .. _exchange: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges
        """
        message.validate()
        if self._publish_channel is None:
            self._publish_channel = yield self._allocate_channel()

        try:
            yield self._publish_channel.basic_publish(
                exchange=exchange,
                routing_key=message._encoded_routing_key,
                body=message._encoded_body,
                properties=message._properties,
            )
        except (pika.exceptions.NackError, pika.exceptions.UnroutableError) as e:
            _std_log.error("Message was rejected by the broker (%s)", str(e))
            raise PublishReturned(reason=e)
        except (pika.exceptions.ChannelClosed, pika.exceptions.ChannelWrongStateError):
            self._publish_channel = None
            yield self.publish(message, exchange)
        except (
            pika.exceptions.ConnectionClosed,
            error.ConnectionLost,
            error.ConnectionDone,
        ) as e:
            raise ConnectionException(reason=e)

    @defer.inlineCallbacks
    def consume(self, callback, queue, previous_consumer=None):
        """
        Register a message consumer that executes the provided callback when
        messages are received.

        The queue must exist prior to calling this method. If a consumer
        already exists for the given queue, the callback is simply updated and
        any new messages for that consumer use the new callback.

        Args:
            callback (callable): The callback to invoke when a message is received.
            queue (str): The name of the queue to consume from.
            previous_consumer (ConsumerV2): If this is the resumption of a prior
                consumer, you can provide the previous consumer so its result
                deferred can be re-used.

        Returns:
            Deferred: A Deferred that fires when the consumer is successfully
                registered with the message broker. The callback receives a
                :class:`.ConsumerV2` object that represents the AMQP consumer.
                The Deferred may error back with a :class:`PermissionException`
                if the user cannot read from the queue, a
                :class:`NoFreeChannels` if this connection has hit its channel
                limit, or a :class:`ConnectionException` if the connection dies
                before the consumer is successfully registered.

        NoFreeChannels: If there are no available channels on this connection.
            If this occurs, you can either reduce the number of consumers on this
            connection or create an additional connection.
        """
        if queue in self._consumers:
            self._consumers[queue].callback = callback
            defer.returnValue(self._consumers[queue])

        if previous_consumer is not None:
            consumer = previous_consumer
        else:
            consumer = ConsumerV2(queue=queue, callback=callback)
        consumer._protocol = self
        consumer._channel = yield self._allocate_channel()
        yield consumer._channel.basic_qos(
            prefetch_count=config.conf["qos"]["prefetch_count"],
            prefetch_size=config.conf["qos"]["prefetch_size"],
        )
        try:
            queue_object, _ = yield consumer._channel.basic_consume(
                queue=consumer.queue, consumer_tag=consumer._tag
            )
        except pika.exceptions.ChannelClosed as exc:
            if exc.args[0] == 403:
                raise PermissionException(
                    obj_type="queue", description=queue, reason=exc.args[1]
                )
            else:
                raise ConnectionException(reason=exc)

        def on_cancel_callback(frame):
            """
            Called when the consumer is canceled server-side.

            This can happen, for example, when the queue is deleted.
            To handle this, we do the necessary book-keeping to remove the consumer
            and then fire the errback on the consumer so the caller of
            :func:`fedora_messaging.api.consume` can decide what to do.

            Args:
                frame (pika.frame.Method): The cancel method from the server,
                    unused here because we already know what consumer is being
                    canceled.
            """
            _std_log.error("%r was canceled by the AMQP broker!", consumer)

            # If client and server are racing to cancel it might already be gone which
            # is why both are marked as no cover.
            try:
                del self._consumers[consumer.queue]
            except KeyError:  # pragma: no cover
                pass
            try:
                del self.factory._consumers[consumer.queue]
            except KeyError:  # pragma: no cover
                pass
            consumer._running = False
            consumer.result.errback(fail=ConsumerCanceled())

        try:
            consumer._channel.add_on_cancel_callback(on_cancel_callback)
        except AttributeError:
            pass  # pika 1.0.0+

        def read_loop_errback(failure):
            """
            Handle errors coming out of the read loop.

            There are two basic categories of errors: ones where the ``consumer.result``
            Deferred needs to be fired because the error is not recoverable, ones
            where we can recover from by letting the connection restart, and ones
            which are fatal for this consumer only (the queue was deleted by an
            administrator).

            Args:
                failure (twisted.python.failure.Failure): The exception raised by
                    the read loop encapsulated in a Failure.
            """
            exc = failure.value
            if failure.check(pika.exceptions.ConsumerCancelled):
                # Pika 1.0.0+ raises this exception. To support previous versions
                # we register a callback (called below) ourselves with the channel.
                on_cancel_callback(None)
            elif failure.check(pika.exceptions.ChannelClosed):
                if exc.args[0] == 403:
                    # This is a mis-configuration, the consumer can register itself,
                    # but it doesn't have permissions to read from the queue,
                    # so no amount of restarting will help.
                    e = PermissionException(
                        obj_type="queue",
                        description=queue,
                        reason=failure.value.args[1],
                    )
                    consumer.result.errback(Failure(e, PermissionException))
                    consumer.cancel()
                else:
                    _std_log.exception(
                        "Consumer halted (%r) unexpectedly; "
                        "the connection should restart.",
                        failure,
                    )
            elif failure.check(error.ConnectionDone, error.ConnectionLost):
                _std_log.warning(
                    "The connection to the broker was lost (%r), consumer halted; "
                    "the connection should restart and consuming will resume.",
                    exc,
                )
            elif failure.check(pika.exceptions.AMQPError):
                _std_log.exception(
                    "An unexpected AMQP error occurred; the connection should "
                    "restart, but please report this as a bug."
                )
            else:
                consumer.result.errback(failure)
                consumer.cancel()

        consumer._read_loop = self._read(queue_object, consumer)
        consumer._read_loop.addErrback(read_loop_errback)

        self._consumers[queue] = consumer
        _std_log.info("Successfully registered AMQP consumer %r", consumer)
        defer.returnValue(consumer)

    @defer.inlineCallbacks
    def declare_exchanges(self, exchanges):
        """
        Declare a number of exchanges at once.

        This simply wraps the :meth:`pika.channel.Channel.exchange_declare`
        method and deals with error handling and channel allocation.

        Args:
            exchanges (list of dict): A list of dictionaries, where each dictionary
                represents an exchange. Each dictionary can have the following keys:

                  * exchange (str): The exchange's name
                  * exchange_type (str): The type of the exchange ("direct", "topic", etc)
                  * passive (bool): If true, this will just assert that the exchange exists,
                    but won't create it if it doesn't. Defaults to the configuration value
                    :ref:`conf-passive-declares`
                  * durable (bool): Whether or not the exchange is durable
                  * arguments (dict): Extra arguments for the exchange's creation.
        Raises:
            NoFreeChannels: If there are no available channels on this connection.
                If this occurs, you can either reduce the number of consumers on this
                connection or create an additional connection.
            BadDeclaration: If an exchange could not be declared. This can occur
                if the exchange already exists, but does its type does not match
                (e.g. it is declared as a "topic" exchange, but exists as a "direct"
                exchange). It can also occur if it does not exist, but the current
                user does not have permissions to create the object.
        """
        channel = yield self._allocate_channel()
        try:
            for exchange in exchanges:
                args = exchange.copy()
                args.setdefault("passive", config.conf["passive_declares"])
                try:
                    yield channel.exchange_declare(**args)
                except pika.exceptions.ChannelClosed as e:
                    raise BadDeclaration("exchange", args, e)
        finally:
            try:
                channel.close()
            except pika.exceptions.AMQPError:
                pass  # pika doesn't handle repeated closes gracefully

    @defer.inlineCallbacks
    def declare_queues(self, queues):
        """
        Declare a list of queues.

        Args:
            queues (list of dict): A list of dictionaries, where each dictionary
                represents an exchange. Each dictionary can have the following keys:

                  * queue (str): The name of the queue
                  * passive (bool): If true, this will just assert that the queue exists,
                    but won't create it if it doesn't. Defaults to the configuration value
                    :ref:`conf-passive-declares`
                  * durable (bool): Whether or not the queue is durable
                  * exclusive (bool): Whether or not the queue is exclusive to this connection.
                  * auto_delete (bool): Whether or not the queue should be automatically
                    deleted once this connection ends.
                  * arguments (dict): Additional arguments for the creation of the queue.
        Raises:
            NoFreeChannels: If there are no available channels on this connection.
                If this occurs, you can either reduce the number of consumers on this
                connection or create an additional connection.
            BadDeclaration: If a queue could not be declared. This can occur
                if the queue already exists, but does its type does not match
                (e.g. it is declared as a durable queue, but exists as a non-durable
                queue). It can also occur if it does not exist, but the current
                user does not have permissions to create the object.
        """
        channel = yield self._allocate_channel()
        try:
            for queue in queues:
                args = queue.copy()
                args.setdefault("passive", config.conf["passive_declares"])
                try:
                    yield channel.queue_declare(**args)
                except pika.exceptions.ChannelClosed as e:
                    raise BadDeclaration("queue", args, e)
        finally:
            try:
                channel.close()
            except pika.exceptions.AMQPError:
                pass  # pika doesn't handle repeated closes gracefully

    @defer.inlineCallbacks
    def bind_queues(self, bindings):
        """
        Declare a set of bindings between queues and exchanges.

        Args:
            bindings (list of dict): A list of binding definitions. Each dictionary
                must contain the "queue" key whose value is the name of the queue
                to create the binding on, as well as the "exchange" key whose value
                should be the name of the exchange to bind to. Additional acceptable
                keys are any keyword arguments accepted by
                :meth:`pika.channel.Channel.queue_bind`.

        Raises:
            NoFreeChannels: If there are no available channels on this connection.
                If this occurs, you can either reduce the number of consumers on this
                connection or create an additional connection.
            BadDeclaration: If a binding could not be declared. This can occur if the
                queue or exchange don't exist, or if they do, but the current user does
                not have permissions to create bindings.
        """
        channel = yield self._allocate_channel()
        try:
            for binding in bindings:
                try:
                    yield channel.queue_bind(**binding)
                except pika.exceptions.ChannelClosed as e:
                    raise BadDeclaration("binding", binding, e)
        finally:
            try:
                channel.close()
            except pika.exceptions.AMQPError:
                pass  # pika doesn't handle repeated closes gracefully

    @defer.inlineCallbacks
    def halt(self):
        """
        Signal to consumers they should stop after finishing any messages
        currently being processed, then close the connection.

        Returns:
            defer.Deferred: fired when all consumers have successfully stopped
            and the connection is closed.
        """
        if self.is_closed:
            # We were asked to stop because the connection is already gone.
            # There's no graceful way to stop because we can't acknowledge
            # messages in the middle of being processed.
            _std_log.info("Disconnect requested, but AMQP connection already gone")
            self._channel = None
            return

        _std_log.info(
            "Waiting for %d consumer(s) to finish processing before halting",
            len(self._consumers),
        )
        pending_cancels = []
        for c in list(self._consumers.values()):
            pending_cancels.append(c.cancel())
        yield defer.gatherResults(pending_cancels)
        _std_log.info("Finished canceling %d consumers", len(self._consumers))

        try:
            yield self.close()
        except pika.exceptions.ConnectionWrongStateError:
            pass  # Already closing, not a problem since that's what we want.
        self._consumers = {}
        self._channel = None


#: A namedtuple that represents a AMQP consumer.
#:
#: This is deprecated. Use :class:`fedora_messaging.twisted.consumer.Consumer`.
#:
#: * The ``tag`` field is the consumer's AMQP tag (:class:`str`).
#: * The ``queue`` field is the name of the queue it's consuming from (:class:`str`).
#: * The ``callback`` field is the function called for each message (a callable).
#: * The ``channel`` is the AMQP channel used for the consumer
#:   (:class:`pika.adapters.twisted_connection.TwistedChannel`).
Consumer = namedtuple("Consumer", ["tag", "queue", "callback", "channel"])


class FedoraMessagingProtocol(FedoraMessagingProtocolV2):
    """A Twisted Protocol for the Fedora Messaging system.

    This protocol builds on the generic pika AMQP protocol to add calls
    specific to the Fedora Messaging implementation.

    .. warning:: This class is deprecated, use the :class:`FedoraMessagingProtocolV2`.

    Args:
        parameters (pika.ConnectionParameters): The connection parameters.
        confirms (bool): If True, all outgoing messages will require a
            confirmation from the server, and the Deferred returned from
            the publish call will wait for that confirmation.
    """

    name = u"FedoraMessaging:Protocol"

    def __init__(self, parameters, confirms=True):
        FedoraMessagingProtocolV2.__init__(self, parameters, confirms=confirms)
        self._running = False
        warnings.warn(
            "The FedoraMessagingProtocol class is deprecated and will be removed"
            " in fedora-messaging v2.0, please use FedoraMessagingProtocolV2 instead.",
            DeprecationWarning,
        )

    @defer.inlineCallbacks
    def _read(self, queue_object, consumer):
        """
        The loop that reads from the message queue and calls the consumer callback
        wrapper.

        queue_object (pika.adapters.twisted_connection.ClosableDeferredQueue):
            The AMQP queue the consumer is bound to.
        consumer (dict): A dictionary describing the consumer for the given
            queue_object.
        """
        while self._running:
            try:
                _channel, delivery_frame, properties, body = yield queue_object.get()
            except (error.ConnectionDone, ChannelClosedByClient):
                # This is deliberate.
                _legacy_twisted_log.msg(
                    "Stopping the AMQP consumer with tag {tag}".format(tag=consumer.tag)
                )
                break
            except pika.exceptions.ChannelClosed as e:
                _legacy_twisted_log.msg(
                    "Stopping AMQP consumer {tag} for queue {q}: {e}".format(
                        tag=consumer.tag, q=consumer.queue, e=str(e)
                    ),
                    logLevel=logging.ERROR,
                )
                break
            except pika.exceptions.ConsumerCancelled as e:
                _legacy_twisted_log.msg(
                    "The AMQP broker canceled consumer {tag} on queue {q}: {e}".format(
                        tag=consumer.tag, q=consumer.queue, e=str(e)
                    ),
                    logLevel=logging.ERROR,
                )
                break
            except Exception:
                _legacy_twisted_log.msg(
                    "An unexpected error occurred consuming from queue {q}".format(
                        q=consumer.queue
                    ),
                    logLevel=logging.ERROR,
                )
                break
            if body:
                yield self._on_message(delivery_frame, properties, body, consumer)

    @defer.inlineCallbacks
    def _on_message(self, delivery_frame, properties, body, consumer):
        """
        Callback when a message is received from the server.

        This method wraps a user-registered callback for message delivery. It
        decodes the message body, determines the message schema to validate the
        message with, and validates the message before passing it on to the
        user callback.

        This also handles acking, nacking, and rejecting messages based on
        exceptions raised by the consumer callback. For detailed documentation
        on the user-provided callback, see the user guide on consuming.

        Args:
            delivery_frame (pika.spec.Deliver): The delivery frame which
                includes details about the message like content encoding and
                its delivery tag.
            properties (pika.spec.BasicProperties): The message properties like
                the message headers.
            body (bytes): The message payload.
            consumer (dict): A dictionary describing the consumer of the message.

        Returns:
            Deferred: fired when the message has been handled.
        """
        _legacy_twisted_log.msg(
            "Message arrived with delivery tag {tag} for {consumer}".format(
                tag=delivery_frame.delivery_tag, consumer=consumer
            ),
            logLevel=logging.DEBUG,
        )
        try:
            message = get_message(delivery_frame.routing_key, properties, body)
            message.queue = consumer.queue
        except ValidationError:
            _legacy_twisted_log.msg(
                "Message id {msgid} did not pass validation; ignoring message".format(
                    msgid=properties.message_id
                ),
                logLevel=logging.WARNING,
            )
            yield consumer.channel.basic_nack(
                delivery_tag=delivery_frame.delivery_tag, requeue=False
            )
            return

        try:
            _legacy_twisted_log.msg(
                "Consuming message from topic {topic!r} (id {msgid})".format(
                    topic=message.topic, msgid=properties.message_id
                )
            )
            yield defer.maybeDeferred(consumer.callback, message)
        except Nack:
            _legacy_twisted_log.msg(
                "Returning message id {msgid} to the queue".format(
                    msgid=properties.message_id
                ),
                logLevel=logging.WARNING,
            )
            yield consumer.channel.basic_nack(
                delivery_tag=delivery_frame.delivery_tag, requeue=True
            )
        except Drop:
            _legacy_twisted_log.msg(
                "Consumer requested message id {msgid} be dropped".format(
                    msgid=properties.message_id
                ),
                logLevel=logging.WARNING,
            )
            yield consumer.channel.basic_nack(
                delivery_tag=delivery_frame.delivery_tag, requeue=False
            )
        except HaltConsumer:
            _legacy_twisted_log.msg(
                "Consumer indicated it wishes consumption to halt, shutting down"
            )
            yield consumer.channel.basic_ack(delivery_tag=delivery_frame.delivery_tag)
            yield self.cancel(consumer.queue)
        except Exception:
            _legacy_twisted_log.msg(
                "Received unexpected exception from consumer {c}".format(c=consumer),
                logLevel=logging.ERROR,
            )
            yield consumer.channel.basic_nack(
                delivery_tag=0, multiple=True, requeue=True
            )
            yield self.cancel(consumer.queue)
        else:
            yield consumer.channel.basic_ack(delivery_tag=delivery_frame.delivery_tag)

    @defer.inlineCallbacks
    def consume(self, callback, queue):
        """
        Register a message consumer that executes the provided callback when
        messages are received.

        The queue must exist prior to calling this method.  If a consumer
        already exists for the given queue, the callback is simply updated and
        any new messages for that consumer use the new callback.

        If :meth:`resumeProducing` has not been called when this method is called,
        it will be called for you.

        Args:
            callback (callable): The callback to invoke when a message is received.
            queue (str): The name of the queue to consume from.

        Returns:
            fedora_messaging.twisted.protocol.Consumer: A namedtuple that
            identifies this consumer.

        NoFreeChannels: If there are no available channels on this connection.
            If this occurs, you can either reduce the number of consumers on this
            connection or create an additional connection.
        """
        if queue in self._consumers and self._consumers[queue].channel.is_open:
            consumer = Consumer(
                tag=self._consumers[queue].tag,
                queue=queue,
                callback=callback,
                channel=self._consumers[queue].channel,
            )
            self._consumers[queue] = consumer
            defer.returnValue(consumer)

        channel = yield self._allocate_channel()
        yield channel.basic_qos(
            prefetch_count=config.conf["qos"]["prefetch_count"],
            prefetch_size=config.conf["qos"]["prefetch_size"],
        )
        consumer = Consumer(
            tag=str(uuid.uuid4()), queue=queue, callback=callback, channel=channel
        )

        self._consumers[queue] = consumer
        if not self._running:
            yield self.resumeProducing()
            defer.returnValue(consumer)

        queue_object, _ = yield consumer.channel.basic_consume(
            queue=consumer.queue, consumer_tag=consumer.tag
        )
        deferred = self._read(queue_object, consumer)
        deferred.addErrback(
            lambda f: _legacy_twisted_log.msg,
            "_read failed on consumer {c}".format(c=consumer),
            logLevel=logging.ERROR,
        )
        _legacy_twisted_log.msg(
            "Successfully registered AMQP consumer {c}".format(c=consumer)
        )
        defer.returnValue(consumer)

    @defer.inlineCallbacks
    def cancel(self, queue):
        """
        Cancel the consumer for a queue.

        Args:
            queue (str): The name of the queue the consumer is subscribed to.

        Returns:
            defer.Deferred: A Deferred that fires when the consumer
                is canceled, or None if the consumer was already canceled. Wrap
                the call in :func:`.defer.maybeDeferred` to always receive a Deferred.
        """
        try:
            consumer = self._consumers[queue]
            yield consumer.channel.basic_cancel(consumer_tag=consumer.tag)
        except pika.exceptions.AMQPChannelError:
            # Consumers are tied to channels, so if this channel is dead the
            # consumer should already be canceled (and we can't get to it anyway)
            pass
        except KeyError:
            defer.returnValue(None)

        try:
            yield consumer.channel.close()
        except pika.exceptions.AMQPChannelError:
            pass

        del self._consumers[queue]

    @defer.inlineCallbacks
    def resumeProducing(self):
        """
        Starts or resumes the retrieval of messages from the server queue.

        This method starts receiving messages from the server, they will be
        passed to the consumer callback.

        .. note:: This is called automatically when :meth:`.consume` is called,
            so users should not need to call this unless :meth:`.pauseProducing`
            has been called.

        Returns:
            defer.Deferred: fired when the production is ready to start
        """
        # Start consuming
        self._running = True
        for consumer in self._consumers.values():
            queue_object, _ = yield consumer.channel.basic_consume(
                queue=consumer.queue, consumer_tag=consumer.tag
            )
            deferred = self._read(queue_object, consumer)
            deferred.addErrback(
                lambda f: _legacy_twisted_log.msg,
                "_read failed on consumer {c}".format(c=consumer),
                logLevel=logging.ERROR,
            )
        _legacy_twisted_log.msg("AMQP connection successfully established")

    @defer.inlineCallbacks
    def pauseProducing(self):
        """
        Pause the reception of messages by canceling all existing consumers.
        This does not disconnect from the server.

        Message reception can be resumed with :meth:`resumeProducing`.

        Returns:
            Deferred: fired when the production is paused.
        """
        if not self._running:
            return
        # Exit the read loop and cancel the consumer on the server.
        self._running = False
        for consumer in self._consumers.values():
            yield consumer.channel.basic_cancel(consumer_tag=consumer.tag)
        _legacy_twisted_log.msg("Paused retrieval of messages for the server queue")

    @defer.inlineCallbacks
    def stopProducing(self):
        """
        Stop producing messages and disconnect from the server.
        Returns:
            Deferred: fired when the production is stopped.
        """
        _legacy_twisted_log.msg("Disconnecting from the AMQP broker")
        yield self.pauseProducing()
        yield self.close()
        self._consumers = {}
        self._channel = None
