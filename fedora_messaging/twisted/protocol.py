# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

"""
The core Twisted interface, a protocol represent a specific connection to the
AMQP broker.

The :class:`.FedoraMessagingProtocolV2` inherits the
:class:`pika.adapters.twisted_connection.TwistedProtocolConnection` class and
adds a few additional methods.

When combined with the :class:`fedora_messaging.twisted.factory.FedoraMessagingFactoryV2`
class, it's easy to create AMQP consumers that last across connections.

For an overview of Twisted clients, see the `Twisted client documentation
<https://twistedmatrix.com/documents/current/core/howto/clients.html#protocol>`_.
"""


import logging
from collections.abc import Generator
from typing import Any, Optional, TYPE_CHECKING, Union

import pika
import pika.exceptions
import pika.frame
from pika.adapters.twisted_connection import TwistedChannel, TwistedProtocolConnection
from twisted.internet import defer, error

from .. import config
from ..exceptions import (
    BadDeclaration,
    ConnectionException,
    NoFreeChannels,
    PublishForbidden,
    PublishReturned,
)
from .consumer import Consumer


if TYPE_CHECKING:
    from ..message import Message
    from .factory import FedoraMessagingFactoryV2


_std_log = logging.getLogger(__name__)


class BindingArgument(config.BaseBinding):
    """The definition of a binding when used to bind queues."""

    routing_key: str


class FedoraMessagingProtocolV2(TwistedProtocolConnection):
    """A Twisted Protocol for the Fedora Messaging system.

    This protocol builds on the generic pika AMQP protocol to add calls
    specific to the Fedora Messaging implementation.

    Attributes:
        factory: The :class:`Factory` object that created this protocol. This
        is set by the factory that creates this protocol.

    Args:
        parameters: The connection parameters.
        confirms: If True, all outgoing messages will require a confirmation from the server,
            and the Deferred returned from the publish call will wait for that confirmation.
    """

    name = "FedoraMessaging:Protocol"

    def __init__(self, parameters: pika.connection.Parameters, confirms: bool = True):
        TwistedProtocolConnection.__init__(self, parameters)
        self._confirms = confirms
        self._channel: Union[TwistedChannel, None] = None
        self._publish_channel: Union[TwistedChannel, None] = None
        # Map queue names to fedora_messaging.twisted.consumer.Consumer objects
        self._consumers: dict[str, Consumer] = {}
        self.factory: Union[FedoraMessagingFactoryV2, None] = None  # pyright: ignore

    @defer.inlineCallbacks
    def _allocate_channel(self) -> Generator[defer.Deferred[Any], TwistedChannel, TwistedChannel]:
        """
        Allocate a new AMQP channel.

        Raises:
            NoFreeChannels: If this connection has reached its maximum number of channels.
            ConncetionException: If this connection is already closed.
        """
        try:
            channel: TwistedChannel = yield self.channel()
        except pika.exceptions.NoFreeChannels as e:
            raise NoFreeChannels() from e
        except pika.exceptions.ConnectionWrongStateError as e:
            raise ConnectionException(reason=e) from e
        _std_log.debug("Created AMQP channel id %d", channel.channel_number)
        if self._confirms:
            yield channel.confirm_delivery()
        defer.returnValue(channel)

    @defer.inlineCallbacks
    # TODO: bug in types-pika: this can also return a Deferred[TwistedProtocolConnection]
    # TODO: check what version of Pika is in EL7, we may not need the "res" argument
    def connectionReady(  # type: ignore
        self, res: Any = None
    ) -> Generator[defer.Deferred[Any], TwistedChannel, TwistedProtocolConnection]:
        """
        Callback invoked when the AMQP connection is ready (when self.ready fires).

        This API is not meant for users.

        Args:
            res: This is an unused argument that provides compatibility with Pika
                versions lower than 1.0.0.
        """
        self._channel = yield self._allocate_channel()
        defer.returnValue(self)

    @defer.inlineCallbacks
    def publish(
        self, message: "Message", exchange: str
    ) -> Generator[defer.Deferred[Any], TwistedChannel, None]:
        """
        Publish a :class:`fedora_messaging.message.Message` to an `exchange`_
        on the message broker.

        Args:
            message: The message to publish.
            exchange: The name of the AMQP exchange to publish to

        Raises:
            NoFreeChannels: If there are no available channels on this connection.
                If this occurs, you can either reduce the number of consumers on this
                connection or create an additional connection.
            PublishReturned: If the broker rejected the message. This can happen if
                there are resource limits that have been reached (full disk, for example)
                or if the message will be routed to 0 queues and the exchange is set to
                reject such messages.
            PublishForbidden: If the broker rejected the message because of permission
                issues (for example: the topic is not allowed).

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
            raise PublishReturned(reason=e) from e
        except pika.exceptions.ProbableAccessDeniedError as e:
            _std_log.error("Message was forbidden by the broker (%s)", str(e))
            raise PublishForbidden(reason=e) from e
        except (
            pika.exceptions.ChannelClosed,
            pika.exceptions.ChannelWrongStateError,
        ) as e:
            # Channel has been closed, we'll need to re-allocate it.
            self._publish_channel = None
            # Handle Forbidden errors
            if isinstance(e, pika.exceptions.ChannelClosed) and e.reply_code == 403:
                _std_log.error(
                    "Message was forbidden by the broker: %s",
                    e.reply_text,
                )
                raise PublishForbidden(reason=e) from e
            # In other cases, try to publish again
            yield self.publish(message, exchange)
        except (
            pika.exceptions.ConnectionClosed,
            error.ConnectionLost,
            error.ConnectionDone,
        ) as e:
            raise ConnectionException(reason=e) from e

    @defer.inlineCallbacks
    def consume(
        self,
        callback: config.CallbackType,
        queue: str,
        previous_consumer: Optional[Consumer] = None,
    ) -> Generator[defer.Deferred[Any], TwistedChannel, Consumer]:
        """
        Register a message consumer that executes the provided callback when
        messages are received.

        The queue must exist prior to calling this method. If a consumer
        already exists for the given queue, the callback is simply updated and
        any new messages for that consumer use the new callback.

        Args:
            callback: The callback to invoke when a message is received.
            queue: The name of the queue to consume from.
            previous_consumer: If this is the resumption of a prior
                consumer, you can provide the previous consumer so its result
                deferred can be re-used.

        Returns:
            A Deferred that fires when the consumer is successfully
            registered with the message broker. The callback receives a
            :class:`.Consumer` object that represents the AMQP consumer.
            The Deferred may error back with a :class:`PermissionException`
            if the user cannot read from the queue, a
            :class:`NoFreeChannels` if this connection has hit its channel
            limit, or a :class:`ConnectionException` if the connection dies
            before the consumer is successfully registered.

        Raises:
            NoFreeChannels: If there are no available channels on this connection.
                If this occurs, you can either reduce the number of consumers on this
                connection or create an additional connection.
        """
        if queue in self._consumers:
            self._consumers[queue].callback = callback
            defer.returnValue(self._consumers[queue])

        if previous_consumer is not None:
            consumer = previous_consumer
            # The queue name may have changed, especially in the case of server-generated queues.
            consumer.queue = queue
        else:
            consumer = Consumer(queue=queue, callback=callback)
        consumer._protocol = self
        consumer._channel = yield self._allocate_channel()

        yield consumer.consume()

        self._consumers[queue] = consumer
        _std_log.info("Successfully registered AMQP consumer %r", consumer)
        defer.returnValue(consumer)

    @defer.inlineCallbacks
    def declare_exchanges(
        self, exchanges: list[config.ExchangeDefinition]
    ) -> Generator[defer.Deferred[Any], Any]:
        """
        Declare a number of exchanges at once.

        This simply wraps the :meth:`pika.channel.Channel.exchange_declare`
        method and deals with error handling and channel allocation.

        Args:
            exchanges: A list of dictionaries, where each dictionary represents an exchange.
                Each dictionary can have the following keys:

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
        channel: TwistedChannel = yield self._allocate_channel()
        try:
            for exchange in exchanges:
                args = exchange.copy()
                args.setdefault("passive", config.conf["passive_declares"])
                try:
                    # TODO: Bug in types-pika: this actually takes a string for exchange_type
                    yield channel.exchange_declare(**args)  # type: ignore
                except pika.exceptions.ChannelClosed as e:
                    raise BadDeclaration("exchange", args, e) from e
        finally:
            try:
                channel.close()
            except pika.exceptions.AMQPError:
                pass  # pika doesn't handle repeated closes gracefully

    @defer.inlineCallbacks
    def declare_queues(
        self, queues: list[config.NamedQueueConfig]
    ) -> Generator[defer.Deferred[Any], Any, list[str]]:
        """
        Declare a list of queues.

        Args:
            queues: A list of dictionaries, where each dictionary represents a queue.
                Each dictionary can have the following keys:

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
        channel: TwistedChannel = yield self._allocate_channel()
        result_queues = []
        try:
            for queue in queues:
                args = queue.copy()
                args.setdefault("passive", config.conf["passive_declares"])
                try:
                    frame: pika.frame.Method[pika.spec.Queue.DeclareOk] = (
                        yield channel.queue_declare(**args)
                    )
                except pika.exceptions.ChannelClosed as e:
                    raise BadDeclaration("queue", args, e) from e
                result_queues.append(frame.method.queue)
        finally:
            try:
                channel.close()
            except pika.exceptions.AMQPError:
                pass  # pika doesn't handle repeated closes gracefully
        defer.returnValue(result_queues)

    @defer.inlineCallbacks
    def declare_queue(
        self, queue: config.NamedQueueConfig
    ) -> Generator[defer.Deferred[Any], list[str], str]:
        """
        Declare a queue. This is a convenience method to call :meth:`declare_queues` with a single
        argument.
        """
        names = yield self.declare_queues([queue])
        defer.returnValue(names[0])

    @defer.inlineCallbacks
    def bind_queues(self, bindings: list[BindingArgument]) -> Generator[defer.Deferred[Any], Any]:
        """
        Declare a set of bindings between queues and exchanges.

        Args:
            bindings: A list of binding definitions. Each dictionary must contain the "queue" key
                whose value is the name of the queue to create the binding on, as well as the
                "exchange" key whose value should be the name of the exchange to bind to.
                Additional acceptable keys are any keyword arguments accepted by
                :meth:`pika.channel.Channel.queue_bind`.

        Raises:
            NoFreeChannels: If there are no available channels on this connection.
                If this occurs, you can either reduce the number of consumers on this
                connection or create an additional connection.
            BadDeclaration: If a binding could not be declared. This can occur if the
                queue or exchange don't exist, or if they do, but the current user does
                not have permissions to create bindings.
        """
        channel: TwistedChannel = yield self._allocate_channel()
        try:
            for binding in bindings:
                try:
                    yield channel.queue_bind(**binding)
                except pika.exceptions.ChannelClosed as e:
                    raise BadDeclaration("binding", binding, e) from e
        finally:
            try:
                channel.close()
            except pika.exceptions.AMQPError:
                pass  # pika doesn't handle repeated closes gracefully

    @defer.inlineCallbacks
    def halt(self) -> Generator[defer.Deferred[Any]]:
        """
        Signal to consumers they should stop after finishing any messages
        currently being processed, then close the connection.

        Returns:
            A deferred fired when all consumers have successfully stopped
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

    def _forget_consumer(self, queue: Optional[str]) -> None:
        """Forget about a consumer so it does not restart later.

        Args:
            queue: Forget consumers consuming from this queue.
        """
        if queue is None:
            return
        # If client and server are racing to cancel it might already be gone which
        # is why both are marked as no cover.
        try:
            del self._consumers[queue]
        except KeyError:  # pragma: no cover
            pass
        if self.factory is not None:
            self.factory._forget_consumer(queue)
