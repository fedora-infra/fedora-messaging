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

import pika
from twisted.internet import defer, protocol, error

from twisted.logger import Logger

from ..exceptions import ConnectionException
from .protocol import FedoraMessagingProtocol

_log = Logger(__name__)


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

    def startedConnecting(self, connector):
        """Called when the connection to the broker has started.

        See the documentation of
        `twisted.internet.protocol.ReconnectingClientFactory` for details.
        """
        _log.info("Started new connection to the AMQP broker")

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
        _log.info("Successfully connected to the AMQP broker.")
        yield self.client.resumeProducing()

        yield self.client.declare_exchanges(self.exchanges)
        yield self.client.declare_queues(self.queues)
        yield self.client.bind_queues(self.bindings)
        for queue, callback in self.consumers.items():
            yield self.client.consume(callback, queue)

        _log.info("Successfully declared all AMQP objects.")
        self._client_ready.callback(None)

    def clientConnectionLost(self, connector, reason):
        """Called when the connection to the broker has been lost.

        See the documentation of
        `twisted.internet.protocol.ReconnectingClientFactory` for details.
        """
        if not isinstance(reason.value, error.ConnectionDone):
            _log.warn(
                "Lost connection to the AMQP broker ({reason})", reason=reason.value
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
        _log.warn(
            "Connection to the AMQP broker failed ({reason})", reason=reason.value
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
        while True:
            client = yield self.whenConnected()
            try:
                yield client.publish(message, exchange)
                break
            except ConnectionException:
                continue
