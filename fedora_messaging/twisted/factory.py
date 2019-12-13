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
A Twisted Factory for creating and configuring instances of the
:class:`.FedoraMessagingProtocol`.

A factory is used to implement automatic re-connections by producing protocol
instances (connections) on demand. Twisted uses factories for its services APIs.

See the `Twisted client
<https://twistedmatrix.com/documents/current/core/howto/clients.html#clientfactory>`_
documentation for more information.
"""

from __future__ import absolute_import
import collections
import warnings
import logging

import pika
from twisted.internet import defer, protocol, error
from twisted.python import log as _legacy_twisted_log
from twisted.python.failure import Failure

from .. import config
from ..exceptions import ConnectionException
from .protocol import FedoraMessagingProtocol, FedoraMessagingProtocolV2

_std_log = logging.getLogger(__name__)


class FedoraMessagingFactory(protocol.ReconnectingClientFactory):
    """Reconnecting factory for the Fedora Messaging protocol."""

    name = u"FedoraMessaging:Factory"
    protocol = FedoraMessagingProtocol

    def __init__(
        self, parameters, confirms=True, exchanges=None, queues=None, bindings=None
    ):
        """
        Create a new factory for protocol objects.

        Any exchanges, queues, bindings, or consumers provided here will be
        declared and set up each time a new protocol instance is created. In
        other words, each time a new connection is set up to the broker, it
        will start with the declaration of these objects.

        Args:
            parameters (pika.ConnectionParameters): The connection parameters.
            confirms (bool): If true, attempt to turn on publish confirms extension.
            exchanges (list of dicts): List of exchanges to declare. Each dictionary is
                passed to :meth:`pika.channel.Channel.exchange_declare` as keyword arguments,
                so any parameter to that method is a valid key.
            queues (list of dicts): List of queues to declare each dictionary is
                passed to :meth:`pika.channel.Channel.queue_declare` as keyword arguments,
                so any parameter to that method is a valid key.
            bindings (list of dicts): A list of bindings to be created between
                queues and exchanges. Each dictionary is passed to
                :meth:`pika.channel.Channel.queue_bind`. The "queue" and "exchange" keys
                are required.
        """
        self._parameters = parameters
        self.confirms = confirms
        self.exchanges = exchanges or []
        self.queues = queues or []
        self.bindings = bindings or []
        self.consumers = {}
        self.client = None
        self._client_ready = defer.Deferred()
        warnings.warn(
            "The FedoraMessagingFactory class is deprecated and will be removed"
            " in fedora-messaging v2.0, please use FedoraMessagingFactoryV2 instead.",
            DeprecationWarning,
        )

    def startedConnecting(self, connector):
        """Called when the connection to the broker has started.

        See the documentation of
        `twisted.internet.protocol.ReconnectingClientFactory` for details.
        """
        _legacy_twisted_log.msg("Started new connection to the AMQP broker")

    def buildProtocol(self, addr):
        """Create the Protocol instance.

        See the documentation of
        `twisted.internet.protocol.ReconnectingClientFactory` for details.
        """
        self.resetDelay()
        self.client = self.protocol(self._parameters)
        self.client.factory = self
        self.client.ready.addCallback(lambda _: self._on_client_ready())
        return self.client

    @defer.inlineCallbacks
    def _on_client_ready(self):
        """Called when the client is ready to send and receive messages."""
        _legacy_twisted_log.msg("Successfully connected to the AMQP broker.")
        yield self.client.resumeProducing()

        yield self.client.declare_exchanges(self.exchanges)
        yield self.client.declare_queues(self.queues)
        yield self.client.bind_queues(self.bindings)
        for queue, callback in self.consumers.items():
            yield self.client.consume(callback, queue)

        _legacy_twisted_log.msg("Successfully declared all AMQP objects.")
        self._client_ready.callback(None)

    def clientConnectionLost(self, connector, reason):
        """Called when the connection to the broker has been lost.

        See the documentation of
        `twisted.internet.protocol.ReconnectingClientFactory` for details.
        """
        if not isinstance(reason.value, error.ConnectionDone):
            _legacy_twisted_log.msg(
                "Lost connection to the AMQP broker ({reason})".format(
                    reason=reason.value
                ),
                logLevel=logging.WARNING,
            )
        if self._client_ready.called:
            # Renew the ready deferred, it will callback when the
            # next connection is ready.
            self._client_ready = defer.Deferred()
        protocol.ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

    def clientConnectionFailed(self, connector, reason):
        """Called when the client has failed to connect to the broker.

        See the documentation of
        `twisted.internet.protocol.ReconnectingClientFactory` for details.
        """
        _legacy_twisted_log.msg(
            "Connection to the AMQP broker failed ({reason})".format(
                reason=reason.value
            ),
            logLevel=logging.WARNING,
        )
        protocol.ReconnectingClientFactory.clientConnectionFailed(
            self, connector, reason
        )

    def stopTrying(self):
        """Stop trying to reconnect to the broker.

        See the documentation of
        `twisted.internet.protocol.ReconnectingClientFactory` for details.
        """
        protocol.ReconnectingClientFactory.stopTrying(self)
        if not self._client_ready.called:
            self._client_ready.errback(
                pika.exceptions.AMQPConnectionError(
                    u"Could not connect, reconnection cancelled."
                )
            )

    @defer.inlineCallbacks
    def stopFactory(self):
        """Stop the factory.

        See the documentation of
        `twisted.internet.protocol.ReconnectingClientFactory` for details.
        """
        if self.client:
            yield self.client.stopProducing()
        protocol.ReconnectingClientFactory.stopFactory(self)

    def consume(self, callback, queue):
        """
        Register a new consumer.

        This consumer will be configured for every protocol this factory
        produces so it will be reconfigured on network failures. If a connection
        is already active, the consumer will be added to it.

        Args:
            callback (callable): The callback to invoke when a message arrives.
            queue (str): The name of the queue to consume from.
        """
        self.consumers[queue] = callback
        if self._client_ready.called:
            return self.client.consume(callback, queue)

    def cancel(self, queue):
        """
        Cancel the consumer for a queue.

        This removes the consumer from the list of consumers to be configured for
        every connection.

        Args:
            queue (str): The name of the queue the consumer is subscribed to.
        Returns:
            defer.Deferred or None: Either a Deferred that fires when the consumer
                is canceled, or None if the consumer was already canceled. Wrap
                the call in :func:`defer.maybeDeferred` to always receive a Deferred.
        """
        try:
            del self.consumers[queue]
        except KeyError:
            pass
        if self.client:
            return self.client.cancel(queue)

    @defer.inlineCallbacks
    def whenConnected(self):
        """
        Get the next connected protocol instance.

        Returns:
            defer.Deferred: A deferred that results in a connected
                :class:`FedoraMessagingProtocol`.
        """
        yield self._client_ready
        defer.returnValue(self.client)

    @defer.inlineCallbacks
    def publish(self, message, exchange=None):
        """
        Publish a :class:`fedora_messaging.message.Message` to an `exchange`_
        on the message broker. This call will survive connection failures and try
        until it succeeds or is canceled.

        Args:
            message (message.Message): The message to publish.
            exchange (str): The name of the AMQP exchange to publish to; defaults
                to :ref:`conf-publish-exchange`

        returns:
            defer.Deferred: A deferred that fires when the message is published.

        Raises:
            PublishReturned: If the published message is rejected by the broker.
            ConnectionException: If a connection error occurs while publishing. Calling
                this method again will wait for the next connection and publish when it
                is available.

        .. _exchange: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges
        """
        exchange = exchange or config.conf["publish_exchange"]
        while True:
            client = yield self.whenConnected()
            try:
                yield client.publish(message, exchange)
                break
            except ConnectionException:
                continue


class FedoraMessagingFactoryV2(protocol.ReconnectingClientFactory):
    """Reconnecting factory for the Fedora Messaging protocol."""

    def __init__(self, parameters, confirms=True):
        """
        Create a new factory for protocol objects.

        Any exchanges, queues, or bindings provided here will be declared and
        set up each time a new protocol instance is created. In other words,
        each time a new connection is set up to the broker, it will start with
        the declaration of these objects.

        Args:
            parameters (pika.ConnectionParameters): The connection parameters.
            confirms (bool): If true, attempt to turn on publish confirms extension.
        """
        self.confirms = confirms
        self.protocol = FedoraMessagingProtocolV2
        self._parameters = parameters
        # Used to implement the when_connected API
        self._client_deferred = defer.Deferred()
        self._client = None
        self._consumers = {}

    def __repr__(self):
        """Return the representation of the factory as a string"""
        return "FedoraMessagingFactoryV2(parameters={}, confirms={})".format(
            self._parameters, self.confirms
        )

    def buildProtocol(self, addr):
        """Create the Protocol instance.

        See the documentation of
        `twisted.internet.protocol.ReconnectingClientFactory` for details.
        """
        client = self.protocol(self._parameters, confirms=self.confirms)
        client.factory = self

        @defer.inlineCallbacks
        def on_ready(unused_param=None):
            """Reset the connection delay when the AMQP handshake is complete."""
            _std_log.debug("AMQP handshake completed; connection ready for use")
            self.resetDelay()
            self._client = client
            self._client_deferred.callback(client)
            # Renew the deferred to handle reconnections.
            self._client_deferred = defer.Deferred()

            # Restart any consumer from previous connections that wasn't canceled
            # including queues and bindings, as the queue might not have been durable
            for consumer, queue, bindings in self._consumers.values():
                _std_log.info("Re-registering the %r consumer", consumer)
                yield client.declare_queues([queue])
                yield client.bind_queues(bindings)
                yield client.consume(consumer.callback, consumer.queue, consumer)

        def on_ready_connection_errback(failure):
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

        def general_errback(failure):
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
    def stopFactory(self):
        """Stop the factory.

        See the documentation of
        `twisted.internet.protocol.ReconnectingClientFactory` for details.
        """
        if self._client:
            yield self._client.halt()
        protocol.ReconnectingClientFactory.stopFactory(self)

    @defer.inlineCallbacks
    def when_connected(self):
        """
        Retrieve the currently-connected Protocol, or the next one to connect.

        Returns:
            defer.Deferred: A Deferred that fires with a connected
                :class:`FedoraMessagingProtocolV2` instance. This is similar to
                the whenConnected method from the Twisted endpoints APIs, which
                is sadly isn't available before 16.1.0, which isn't available
                in EL7.
        """
        if self._client and not self._client.is_closed:
            _std_log.debug("Already connected with %r", self._client)
        else:
            self._client = None
            _std_log.debug(
                "Waiting for %r to fire with new connection", self._client_deferred
            )
            try:
                yield self._client_deferred
            except defer.CancelledError:
                # Renew the deferred to handle future connections.
                self._client_deferred = defer.Deferred()
                raise
        defer.returnValue(self._client)

    @defer.inlineCallbacks
    def publish(self, message, exchange):
        """
        Publish a :class:`fedora_messaging.message.Message` to an `exchange`_
        on the message broker. This call will survive connection failures and try
        until it succeeds or is canceled.

        Args:
            message (message.Message): The message to publish.
            exchange (str): The name of the AMQP exchange to publish to.

        returns:
            defer.Deferred: A deferred that fires when the message is published.

        Raises:
            NoFreeChannels: If there are no available channels on this connection.
                If this occurs, you can either reduce the number of consumers on this
                connection or create an additional connection.
            PublishReturned: If the published message is rejected by the broker.
            ConnectionException: If a connection error occurs while publishing. Calling
                this method again will wait for the next connection and publish when it
                is available.

        .. _exchange: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges
        """
        while True:
            protocol = yield self.when_connected()
            try:
                yield protocol.publish(message, exchange)
                break
            except ConnectionException:
                _std_log.info(
                    "Publish failed on %r, waiting for new connection", protocol
                )

    @defer.inlineCallbacks
    def consume(self, callback, bindings, queues):
        """
        Start a consumer that lasts across individual connections.

        Args:
            callback (callable): A callable object that accepts one positional argument,
                a :class:`Message` or a class object that implements the ``__call__``
                method. The class will be instantiated before use.
            bindings (dict or list of dict): Bindings to declare before consuming. This
                should be the same format as the :ref:`conf-bindings` configuration.
            queues (dict): The queues to declare and consume from. Each key in this
                dictionary is a queue, and each value is its settings as a dictionary.
                These settings dictionaries should have the "durable", "auto_delete",
                "exclusive", and "arguments" keys. Refer to :ref:`conf-queues` for
                details on their meanings.

        Returns:
            defer.Deferred:
                A deferred that fires with the list of one or more
                :class:`fedora_messaging.twisted.consumer.Consumer` objects.
                These can be passed to the
                :meth:`FedoraMessagingFactoryV2.cancel` API to halt them. Each
                consumer object has a ``result`` instance variable that is a
                Deferred that fires or errors when the consumer halts. The
                Deferred may error back with a BadDeclaration if the user does
                not have permissions to consume from the queue.
        """
        expanded_bindings = collections.defaultdict(list)
        for binding in bindings:
            for key in binding["routing_keys"]:
                b = binding.copy()
                del b["routing_keys"]
                b["routing_key"] = key
                expanded_bindings[b["queue"]].append(b)

        expanded_queues = []
        for name, settings in queues.items():
            q = {"queue": name}
            q.update(settings)
            expanded_queues.append(q)

        protocol = yield self.when_connected()

        consumers = []
        for queue in expanded_queues:
            yield protocol.declare_queues([queue])
            b = expanded_bindings.get(queue["queue"], [])
            yield protocol.bind_queues(b)
            consumer = yield protocol.consume(callback, queue["queue"])
            self._consumers[queue["queue"]] = (consumer, queue, b)
            consumers.append(consumer)

        defer.returnValue(consumers)

    @defer.inlineCallbacks
    def cancel(self, consumers):
        """
        Cancel a consumer that was previously started with consume.

        Args:
            consumer (list of fedora_messaging.api.Consumer): The consumers to cancel.
        """
        for consumer in consumers:
            del self._consumers[consumer.queue]
            protocol = yield self.when_connected()
            yield protocol.cancel(consumer)
