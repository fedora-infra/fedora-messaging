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
AMQP broker. When automatic reconnecting is required, the
:class:`fedora_messaging.twisted.factory.FedoraMessagingFactory` class should
be used to produce configured instances of :class:`.FedoraMessagingProtocol`.

:class:`.FedoraMessagingProtocol` is based on pika's Twisted Protocol. It
implements message schema validation when publishing and subscribing, as well
as a few convenience methods for declaring multiple objects at once.

For an overview of Twisted clients, see the `Twisted client documentation
<https://twistedmatrix.com/documents/current/core/howto/clients.html#protocol>`_.
"""

from __future__ import absolute_import

from collections import namedtuple
import uuid

import pika
import pkg_resources
from pika.adapters.twisted_connection import TwistedProtocolConnection
from twisted.internet import defer, error

from twisted.logger import Logger

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
)


_log = Logger(__name__)

_pika_version = pkg_resources.get_distribution("pika").parsed_version
if _pika_version < pkg_resources.parse_version("1.0.0b1"):
    ChannelClosedByClient = pika.exceptions.ChannelClosed
else:
    ChannelClosedByClient = pika.exceptions.ChannelClosedByClient


#: A namedtuple that represents a AMQP consumer.
#:
#: * The ``tag`` field is the consumer's AMQP tag (:class:`str`).
#: * The ``queue`` field is the name of the queue it's consuming from (:class:`str`).
#: * The ``callback`` field is the function called for each message (a callable).
#: * The ``channel`` is the AMQP channel used for the consumer
#:   (:class:`pika.adapters.twisted_connection.TwistedChannel`).
Consumer = namedtuple("Consumer", ["tag", "queue", "callback", "channel"])


class FedoraMessagingProtocol(TwistedProtocolConnection):
    """A Twisted Protocol for the Fedora Messaging system.

    This protocol builds on the generic pika AMQP protocol to add calls
    specific to the Fedora Messaging implementation.

    Args:
        parameters (pika.ConnectionParameters): The connection parameters.
        confirms (bool): If True, all outgoing messages will require a
            confirmation from the server, and the Deferred returned from
            the publish call will wait for that confirmation.
    """

    name = u"FedoraMessaging:Protocol"

    def __init__(self, parameters, confirms=True):
        TwistedProtocolConnection.__init__(self, parameters)
        self._parameters = parameters
        if confirms and _pika_version < pkg_resources.parse_version("1.0.0b1"):
            _log.error("Message confirmation is only available with pika 1.0.0+")
            confirms = False
        self._confirms = confirms
        self._channel = None
        self._running = False
        # Map queue names to dictionaries representing consumers
        self._consumers = {}
        self.factory = None

    @defer.inlineCallbacks
    def _allocate_channel(self):
        """
        Allocate a new AMQP channel.

        Raises:
            NoFreeChannels: If this connection has reached its maximum number of channels.
        """
        try:
            channel = yield self.channel()
        except pika.exceptions.NoFreeChannels:
            raise NoFreeChannels()
        _log.info("Created AMQP channel {num}", num=channel.channel_number)
        if self._confirms:
            yield channel.confirm_delivery()
        defer.returnValue(channel)

    @defer.inlineCallbacks
    def connectionReady(self, res=None):
        """
        Callback invoked when the AMQP connection is ready.

        This API is not meant for users.
        """
        # The optional `res` argument is for compatibility with pika < 1.0.0
        # Create channel
        self._channel = yield self._allocate_channel()
        yield self._channel.basic_qos(
            prefetch_count=config.conf["qos"]["prefetch_count"],
            prefetch_size=config.conf["qos"]["prefetch_size"],
            all_channels=True,
        )
        if _pika_version < pkg_resources.parse_version("1.0.0b1"):
            TwistedProtocolConnection.connectionReady(self, res)

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
                _log.info("Stopping the AMQP consumer with tag {tag}", tag=consumer.tag)
                break
            except pika.exceptions.ChannelClosed as e:
                _log.error(
                    "Stopping AMQP consumer {tag} for queue {q}: {e}",
                    tag=consumer.tag,
                    q=consumer.queue,
                    e=str(e),
                )
                break
            except pika.exceptions.ConsumerCancelled as e:
                _log.error(
                    "The AMQP broker canceled consumer {tag} on queue {q}: {e}",
                    tag=consumer.tag,
                    q=consumer.queue,
                    e=str(e),
                )
                break
            except Exception as e:
                _log.failure(
                    "An unexpected error occurred consuming from queue {q}",
                    q=consumer.queue,
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
        _log.debug(
            "Message arrived with delivery tag {tag} for {consumer}",
            tag=delivery_frame.delivery_tag,
            consumer=consumer,
        )
        try:
            message = get_message(delivery_frame.routing_key, properties, body)
            message.queue = consumer.queue
        except ValidationError:
            _log.warn(
                "Message id {msgid} did not pass validation; ignoring message",
                msgid=properties.message_id,
            )
            yield consumer.channel.basic_nack(
                delivery_tag=delivery_frame.delivery_tag, requeue=False
            )
            return

        try:
            _log.info(
                "Consuming message from topic {topic!r} (id {msgid})",
                topic=message.topic,
                msgid=properties.message_id,
            )
            yield defer.maybeDeferred(consumer.callback, message)
        except Nack:
            _log.warn(
                "Returning message id {msgid} to the queue", msgid=properties.message_id
            )
            yield consumer.channel.basic_nack(
                delivery_tag=delivery_frame.delivery_tag, requeue=True
            )
        except Drop:
            _log.warn(
                "Consumer requested message id {msgid} be dropped",
                msgid=properties.message_id,
            )
            yield consumer.channel.basic_nack(
                delivery_tag=delivery_frame.delivery_tag, requeue=False
            )
        except HaltConsumer:
            _log.info("Consumer indicated it wishes consumption to halt, shutting down")
            yield consumer.channel.basic_ack(delivery_tag=delivery_frame.delivery_tag)
            yield self.cancel(consumer.queue)
        except Exception:
            _log.failure("Received unexpected exception from consumer {c}", c=consumer)
            yield consumer.channel.basic_nack(
                delivery_tag=0, multiple=True, requeue=True
            )
            yield self.cancel(consumer.queue)
        else:
            yield consumer.channel.basic_ack(delivery_tag=delivery_frame.delivery_tag)

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
        try:
            yield self._channel.basic_publish(
                exchange=exchange,
                routing_key=message._encoded_routing_key,
                body=message._encoded_body,
                properties=message._properties,
            )
        except (pika.exceptions.NackError, pika.exceptions.UnroutableError) as e:
            _log.error("Message was rejected by the broker ({reason})", reason=str(e))
            raise PublishReturned(reason=e)
        except pika.exceptions.ChannelClosed as e:
            self._channel = yield self._allocate_channel()
            yield self.publish(message, exchange)
        except pika.exceptions.ConnectionClosed as e:
            raise ConnectionException(reason=e)

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
            Consumer: A namedtuple that identifies this consumer.

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
            lambda f: _log.failure, "_read failed on consumer {c}", c=consumer
        )
        _log.info("Successfully registered AMQP consumer {c}", c=consumer)
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
                    but won't create it if it doesn't.
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
                try:
                    yield channel.exchange_declare(**exchange)
                except pika.exceptions.ChannelClosed as e:
                    raise BadDeclaration("exchange", exchange, e)
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
                    but won't create it if it doesn't.
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
                try:
                    yield channel.queue_declare(**queue)
                except pika.exceptions.ChannelClosed as e:
                    raise BadDeclaration("queue", queue, e)
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
                b = binding.copy()
                try:
                    queue = b.pop("queue")
                    exchange = b.pop("exchange")
                    yield channel.queue_bind(queue, exchange, **b)
                except pika.exceptions.ChannelClosed as e:
                    raise BadDeclaration("binding", binding, e)
        finally:
            try:
                channel.close()
            except pika.exceptions.AMQPError:
                pass  # pika doesn't handle repeated closes gracefully

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
                lambda f: _log.failure, "_read failed on consumer {c}", c=consumer
            )
        _log.info("AMQP connection successfully established")

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
        _log.info("Paused retrieval of messages for the server queue")

    @defer.inlineCallbacks
    def stopProducing(self):
        """
        Stop producing messages and disconnect from the server.

        Returns:
            Deferred: fired when the production is stopped.
        """
        _log.info("Disconnecting from the AMQP broker")
        yield self.pauseProducing()
        yield self.close()
        self._consumers = {}
        self._channel = None
