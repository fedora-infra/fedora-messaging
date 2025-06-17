# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

"""
A Twisted Factory for creating and configuring instances of the
:class:`.FedoraMessagingProtocolV2`.

A factory is used to implement automatic re-connections by producing protocol
instances (connections) on demand. Twisted uses factories for its services APIs.

See the `Twisted client
<https://twistedmatrix.com/documents/current/core/howto/clients.html#clientfactory>`_
documentation for more information.
"""


import collections
import logging
from collections.abc import Generator
from typing import Any, NamedTuple, TYPE_CHECKING, Union

import pika
import pika.exceptions
from twisted.internet import defer, error, protocol
from twisted.python.failure import Failure

from ..exceptions import ConnectionException
from .consumer import Consumer
from .protocol import BindingArgument, FedoraMessagingProtocolV2
from .stats import ConsumerStatistics, FactoryStatistics


if TYPE_CHECKING:
    from ..config import BindingsType, CallbackType, NamedQueueConfig, QueueConfig
    from ..message import Message


_std_log = logging.getLogger(__name__)


def _remap_queue_name(bindings: list[BindingArgument], queue_name: str) -> None:
    """Reset the queue name in a binding, because the queue name might be server-generated.

    Args:
        bindings: A binding description with a ``queue`` attribute.
        queue_name: The name of the queue to use.
    """
    for binding in bindings:
        binding["queue"] = queue_name
    # The dicts are changed in-place, don't return anything to make that clear.


class ConsumerRecord(NamedTuple):
    """A namedtuple representing the record of an existing AMQP consumer with its config items.

    * The ``consumer`` field is the consumer object
      (:class:`fedora_messaging.twisted.consumer.Consumer`).
    * The ``queue`` field is the configuration dict for the queue it's consuming from.
    * The ``bindings`` field is the list of configuration dicts for the queue's bindings.
    """

    consumer: Consumer
    queue: "NamedQueueConfig"
    bindings: list[BindingArgument]


class FedoraMessagingFactoryV2(protocol.ReconnectingClientFactory):
    """Reconnecting factory for the Fedora Messaging protocol."""

    protocol: type[FedoraMessagingProtocolV2]

    def __init__(self, parameters: pika.connection.Parameters, confirms: bool = True):
        """
        Create a new factory for protocol objects.

        Any exchanges, queues, or bindings provided here will be declared and
        set up each time a new protocol instance is created. In other words,
        each time a new connection is set up to the broker, it will start with
        the declaration of these objects.

        Args:
            parameters: The connection parameters.
            confirms: If true, attempt to turn on publish confirms extension.
        """
        self.confirms = confirms
        self.protocol = (  # pyright: ignore [reportIncompatibleVariableOverride]
            FedoraMessagingProtocolV2
        )
        self._parameters = parameters
        # Used to implement the when_connected API
        self._client_deferred: defer.Deferred[FedoraMessagingProtocolV2] = defer.Deferred()
        self._client: Union[FedoraMessagingProtocolV2, None] = None
        self._consumers: list[ConsumerRecord] = []
        self._stats = FactoryStatistics()

    def __repr__(self) -> str:
        """Return the representation of the factory as a string"""
        return f"FedoraMessagingFactoryV2(parameters={self._parameters}, confirms={self.confirms})"

    def buildProtocol(self, addr: Any) -> FedoraMessagingProtocolV2:
        """Create the Protocol instance.

        See the documentation of
        `twisted.internet.protocol.ReconnectingClientFactory` for details.
        """
        client = self.protocol(self._parameters, confirms=self.confirms)
        client.factory = self

        @defer.inlineCallbacks
        def on_ready(unused_param: Any = None) -> Generator[defer.Deferred[Any], Any]:
            """Reset the connection delay when the AMQP handshake is complete."""
            _std_log.debug("AMQP handshake completed; connection ready for use")
            self.resetDelay()
            self._client = client
            self._client_deferred.callback(client)
            # Renew the deferred to handle reconnections.
            self._client_deferred = defer.Deferred()

            # Restart any consumer from previous connections that wasn't canceled
            # including queues and bindings, as the queue might not have been durable
            for record in self._consumers:
                _std_log.info("Re-registering the %r consumer", record.consumer)
                queue_name: str = yield client.declare_queue(record.queue)
                _remap_queue_name(record.bindings, queue_name)
                yield client.bind_queues(record.bindings)
                if record.consumer.callback is not None:
                    yield client.consume(record.consumer.callback, queue_name, record.consumer)

        def on_ready_connection_errback(failure: Failure) -> None:
            """If opening the connection fails or is lost, this errback is called."""
            r = failure.trap(
                pika.exceptions.AMQPConnectionError,
                error.ConnectionDone,
                error.ConnectionLost,
            )

            if r == error.ConnectionLost:
                msg = (
                    "The network connection to the broker was lost in a non-clean fashion (%r);"
                    " the connection should be restarted by Twisted."
                )
            else:
                # In this case the connection failed to open. This will be called
                # if the TLS handshake goes wrong (likely) and it may be called if
                # the AMQP handshake fails. It's *probably* a problem with the
                # credentials.
                msg = (
                    "The TCP connection appears to have started, but the TLS or AMQP handshake "
                    "with the broker failed; check your connection and authentication "
                    "parameters and ensure your user has permission to access the vhost"
                )

            wrapped_failure = Failure(
                exc_value=ConnectionException(reason=msg, original=failure),
                exc_type=ConnectionException,
            )
            self._client_deferred.errback(wrapped_failure)
            # Renew the deferred to handle reconnections.
            self._client_deferred = defer.Deferred()

        def general_errback(failure: Failure) -> None:
            _std_log.error(
                "The connection failed with an unexpected exception; please report this bug: %s",
                failure.getTraceback(),
            )
            self._client_deferred.errback(failure)
            # Renew the deferred to handle reconnections.
            self._client_deferred = defer.Deferred()

        client.ready.addCallback(on_ready)
        client.ready.addErrback(on_ready_connection_errback)
        client.ready.addErrback(general_errback)
        return client

    @defer.inlineCallbacks
    def stopFactory(  # pyright: ignore [reportIncompatibleMethodOverride]
        self,
    ) -> Generator[defer.Deferred[None]]:
        """Stop the factory.

        See the documentation of
        `twisted.internet.protocol.ReconnectingClientFactory` for details.
        """
        if self._client:
            yield self._client.halt()
        protocol.ReconnectingClientFactory.stopFactory(self)

    @defer.inlineCallbacks
    def when_connected(self) -> Generator[defer.Deferred[Any], None, FedoraMessagingProtocolV2]:
        """
        Retrieve the currently-connected Protocol, or the next one to connect.

        Returns:
            A Deferred that fires with a connected :class:`FedoraMessagingProtocolV2` instance.
            This is similar to the `whenConnected` method from the Twisted endpoints APIs, which
            is sadly isn't available before 16.1.0, which isn't available in EL7.
        """
        if self._client and not self._client.is_closed:
            _std_log.debug("Already connected with %r", self._client)
        else:
            self._client = None
            _std_log.debug("Waiting for %r to fire with new connection", self._client_deferred)
            try:
                yield self._client_deferred
            except defer.CancelledError:
                # Renew the deferred to handle future connections.
                self._client_deferred = defer.Deferred()
                raise
        defer.returnValue(self._client)

    @defer.inlineCallbacks
    def publish(
        self, message: "Message", exchange: str
    ) -> Generator[defer.Deferred[Any], FedoraMessagingProtocolV2]:
        """
        Publish a :class:`fedora_messaging.message.Message` to an `exchange`_
        on the message broker. This call will survive connection failures and try
        until it succeeds or is canceled.

        Args:
            message: The message to publish.
            exchange: The name of the AMQP exchange to publish to.

        returns:
            A deferred that fires when the message is published.

        Raises:
            NoFreeChannels: If there are no available channels on this connection.
                If this occurs, you can either reduce the number of consumers on this
                connection or create an additional connection.
            PublishReturned: If the published message is rejected by the broker.
            PublishForbidden: If the published message is rejected by the broker because
                of permission issues.
            ConnectionException: If a connection error occurs while publishing. Calling
                this method again will wait for the next connection and publish when it
                is available.

        .. _exchange: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges
        """
        while True:
            protocol = yield self.when_connected()
            try:
                yield protocol.publish(message, exchange)
                self._stats.published += 1
                break
            except ConnectionException:
                _std_log.info("Publish failed on %r, waiting for new connection", protocol)

    @defer.inlineCallbacks
    def consume(
        self,
        callback: "CallbackType",
        bindings: "BindingsType",
        queues: dict[str, "QueueConfig"],
    ) -> Generator[defer.Deferred[Any], Any, list[Consumer]]:
        """
        Start a consumer that lasts across individual connections.

        Args:
            callback: A callable object that accepts one positional argument,
                a :class:`Message` or a class object that implements the ``__call__``
                method. The class will be instantiated before use.
            bindings: Bindings to declare before consuming. This
                should be the same format as the :ref:`conf-bindings` configuration.
            queues: The queues to declare and consume from. Each key in this
                dictionary is a queue, and each value is its settings as a dictionary.
                These settings dictionaries should have the "durable", "auto_delete",
                "exclusive", and "arguments" keys. Refer to :ref:`conf-queues` for
                details on their meanings.

        Returns:
            A deferred that fires with the list of one or more
            :class:`fedora_messaging.twisted.consumer.Consumer` objects.
            These can be passed to the :meth:`FedoraMessagingFactoryV2.cancel` API to halt them.
            Each consumer object has a ``result`` instance variable that is a Deferred that fires
            or errors when the consumer halts. The Deferred may error back with a BadDeclaration
            if the user does not have permissions to consume from the queue.
        """
        expanded_bindings = collections.defaultdict(list)
        if isinstance(bindings, dict):
            bindings = [bindings]
        for binding in bindings:
            for key in binding["routing_keys"]:
                b: BindingArgument = binding.copy()  # type: ignore
                del b["routing_keys"]  # type: ignore
                b["routing_key"] = key
                if "queue" not in b:
                    b["queue"] = ""
                expanded_bindings[b["queue"]].append(b)

        expanded_queues: list[NamedQueueConfig] = []
        for name, settings in queues.items():
            q: NamedQueueConfig = {"queue": name, **settings}
            expanded_queues.append(q)

        protocol: FedoraMessagingProtocolV2 = yield self.when_connected()

        consumers = []
        for queue in expanded_queues:
            config_queue_name = queue["queue"]
            queue_name: str = yield protocol.declare_queue(queue)
            ebs: list[BindingArgument] = expanded_bindings.get(config_queue_name, [])
            _remap_queue_name(ebs, queue_name)
            yield protocol.bind_queues(ebs)
            consumer: Consumer = yield protocol.consume(callback, queue_name)
            self._consumers.append(ConsumerRecord(consumer=consumer, queue=queue, bindings=ebs))
            consumers.append(consumer)

        defer.returnValue(consumers)

    @defer.inlineCallbacks
    def cancel(self, consumers: list[Consumer]) -> Generator[defer.Deferred[Any], Any, None]:
        """
        Cancel a consumer that was previously started with consume.

        Args:
            consumer: The consumers to cancel.
        """
        for consumer in consumers:
            if consumer.queue is not None:
                self._forget_consumer(consumer.queue)
            # protocol: FedoraMessagingProtocolV2 = yield self.when_connected()
            yield consumer.cancel()

    def _forget_consumer(self, queue: str) -> None:
        """Forget about a consumer.

        Args:
            queue: Forget the consumers that consume from this queue.
        """
        self._consumers = [record for record in self._consumers if record.consumer.queue != queue]

    @property
    def stats(self) -> FactoryStatistics:
        """Statistics about this factory's consumer(s)."""
        self._stats.consumed = sum(
            (record.consumer.stats for record in self._consumers), start=ConsumerStatistics()
        )
        return self._stats

    @property
    def consuming(self) -> bool:
        """Whether the consumer(s) is currently running."""
        return any(record.consumer.running for record in self._consumers)
