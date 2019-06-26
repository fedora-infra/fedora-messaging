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
import logging

import pika
from twisted.internet import defer, protocol, error
from twisted.python.failure import Failure

from ..exceptions import ConnectionException
from .protocol import FedoraMessagingProtocol

_std_log = logging.getLogger(__name__)


class FedoraMessagingFactory(protocol.ReconnectingClientFactory):
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
        self.protocol = FedoraMessagingProtocol
        self._parameters = parameters
        # Used to implement the when_connected API
        self._client_deferred = defer.Deferred()
        self._client = None
        self._consumers = {}

    def __repr__(self):
        """Return the representation of the factory as a string"""
        return "FedoraMessagingFactory(parameters={}, confirms={})".format(
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

            # Restart any consumer from previous connections that wasn't canceled
            # including queues and bindings, as the queue might not have been durable
            for consumer, queue, bindings in self._consumers.values():
                _std_log.info("Re-registering the %r consumer", consumer)
                yield client.declare_queues([queue])
                yield client.bind_queues(bindings)
                yield client.consume(consumer.callback, consumer.queue, consumer)

        def on_ready_errback(failure):
            """If opening the connection fails or is lost, this errback is called."""
            if failure.check(pika.exceptions.AMQPConnectionError, error.ConnectionDone):
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
            else:
                _std_log.exception(
                    "The connection failed with an unexpected exception; please report this bug."
                )
                self._client_deferred.errback(failure)

        client.ready.addCallback(on_ready)
        client.ready.addErrback(on_ready_errback)
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
                :class:`FedoraMessagingProtocol` instance. This is similar to
                the whenConnected method from the Twisted endpoints APIs, which
                is sadly isn't available before 16.1.0, which isn't available
                in EL7.
        """
        if self._client and not self._client.is_closed:
            _std_log.debug("Already connected with %r", self._client)
        else:
            # This is pretty hideous, but this deferred is fired by the ready
            # callback and self._client is set, so after the yield it's a valid
            # connection.
            self._client_deferred = defer.Deferred()
            self._client = None
            _std_log.debug(
                "Waiting for %r to fire with new connection", self._client_deferred
            )
            yield self._client_deferred
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
                :meth:`FedoraMessagingFactory.cancel` API to halt them. Each
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


#: In fedora-messaging-1.x there were two versions of the factory. The original
#: version has been removed so the second became the only version. This remains
#: for compatibility with old code using the V2 name.
FedoraMessagingFactoryV2 = FedoraMessagingFactory
