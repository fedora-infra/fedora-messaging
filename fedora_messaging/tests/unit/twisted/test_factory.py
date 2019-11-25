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

from __future__ import absolute_import, unicode_literals

import logging
import unittest

import mock
import pika
import pytest
from twisted.internet import defer
from twisted.internet.error import ConnectionDone, ConnectionLost
from twisted.python.failure import Failure

from fedora_messaging import config
from fedora_messaging.twisted.factory import (
    FedoraMessagingFactory,
    FedoraMessagingFactoryV2,
)
from fedora_messaging.exceptions import ConnectionException


try:
    import pytest_twisted
except ImportError:
    pytest.skip("pytest-twisted is missing, skipping tests", allow_module_level=True)


class FactoryTests(unittest.TestCase):
    def setUp(self):
        self.protocol = mock.Mock()
        self.protocol.ready = defer.Deferred()
        protocol_class = mock.Mock(side_effect=lambda a: self.protocol)
        self.factory = FedoraMessagingFactory(
            mock.Mock(name="parameters"), {"binding key": "binding value"}
        )
        self.factory.protocol = protocol_class

    def test_started_connection(self):
        """Assert connection attempts are logged."""
        with mock.patch(
            "fedora_messaging.twisted.factory._legacy_twisted_log"
        ) as mock_log:
            self.factory.startedConnecting(None)
        mock_log.msg.assert_called_once_with(
            "Started new connection to the AMQP broker"
        )

    def test_on_client_ready_no_objects(self):
        """Assert factories with no objects to create on startup work."""
        factory = FedoraMessagingFactory(pika.URLParameters("amqp://"))
        factory.buildProtocol(None)
        mock_channel = mock.Mock()
        factory.client._allocate_channel = mock.Mock(return_value=mock_channel)
        d = factory._on_client_ready()

        d.addCallback(lambda _: self.assertTrue(factory._client_ready.called))

        return pytest_twisted.blockon(d)

    def test_on_client_ready_queues(self):
        """Assert factories with queues to create on startup work."""
        factory = FedoraMessagingFactory(None, queues=[{"queue": "my_queue"}])
        factory.buildProtocol(None)
        mock_channel = mock.Mock()
        factory.client._allocate_channel = mock.Mock(return_value=mock_channel)

        d = factory._on_client_ready()

        def _check(_):
            mock_channel.queue_declare.assert_called_once_with(
                queue="my_queue", passive=False
            )
            self.assertTrue(factory._client_ready.called)

        d.addCallback(_check)

        return pytest_twisted.blockon(d)

    def test_on_client_ready_exchanges(self):
        """Assert factories with exchanges to create on startup work."""
        factory = FedoraMessagingFactory(None, exchanges=[{"exchange": "my_exchange"}])
        factory.buildProtocol(None)
        mock_channel = mock.Mock()
        factory.client._allocate_channel = mock.Mock(return_value=mock_channel)

        d = factory._on_client_ready()

        def _check(_):
            mock_channel.exchange_declare.assert_called_once_with(
                exchange="my_exchange", passive=False
            )
            self.assertTrue(factory._client_ready.called)

        d.addCallback(_check)

        return pytest_twisted.blockon(d)

    def test_passive_declares(self):
        """Assert queues and exchanges are created passively when configured so."""
        factory = FedoraMessagingFactory(
            None,
            queues=[{"queue": "my_queue"}],
            exchanges=[{"exchange": "my_exchange"}],
        )
        factory.buildProtocol(None)
        mock_channel = mock.Mock()
        factory.client._allocate_channel = mock.Mock(return_value=mock_channel)

        def _check(_):
            mock_channel.queue_declare.assert_called_once_with(
                queue="my_queue", passive=True
            )
            mock_channel.exchange_declare.assert_called_once_with(
                exchange="my_exchange", passive=True
            )

        with mock.patch.dict(config.conf, {"passive_declares": True}):
            d = factory._on_client_ready()
            d.addCallback(_check)

        return pytest_twisted.blockon(d)

    def test_on_client_ready_consumers(self):
        """Assert factories with bindings to create on startup work."""
        factory = FedoraMessagingFactory(None)
        factory.buildProtocol(None)
        factory.consume(lambda x: x, "my_queue")
        mock_channel = mock.Mock()
        mock_channel.basic_consume.return_value = mock.Mock(), None
        factory.client._allocate_channel = mock.Mock(return_value=mock_channel)

        d = factory._on_client_ready()

        def _check(_):
            mock_channel.basic_consume.assert_called()
            self.assertIn("my_queue", factory.client._consumers)
            self.assertTrue(factory._client_ready.called)

        d.addCallback(_check)

        return pytest_twisted.blockon(d)

    def test_on_client_ready_bindings(self):
        """Assert factories with bindings to create on startup work."""
        factory = FedoraMessagingFactory(
            None, bindings=[{"queue": "my_queue", "exchange": "my_exchange"}]
        )
        factory.buildProtocol(None)
        mock_channel = mock.Mock()
        factory.client._allocate_channel = mock.Mock(return_value=mock_channel)

        d = factory._on_client_ready()

        def _check(_):
            mock_channel.queue_bind.assert_called_once_with(
                queue="my_queue", exchange="my_exchange"
            )
            self.assertTrue(factory._client_ready.called)

        d.addCallback(_check)

        return pytest_twisted.blockon(d)

    def test_buildProtocol(self):
        # Check the buildProtocol method.
        protocol = self.factory.buildProtocol(None)
        self.assertTrue(protocol is self.protocol)
        self.assertTrue(self.factory.client is self.protocol)
        self.assertTrue(protocol.factory is self.factory)
        self.protocol.ready.callback(None)
        self.assertTrue(self.factory._client_ready.called)

    def test_clientConnectionLost(self):
        # The _client_ready deferred must be renewed when the connection is
        # lost.
        self.factory._client_ready.callback(None)
        self.factory.clientConnectionLost(mock.Mock(), Failure(RuntimeError()))
        self.assertFalse(self.factory._client_ready.called)

    @mock.patch(
        "fedora_messaging.twisted.factory.protocol.ReconnectingClientFactory."
        "clientConnectionFailed",
        mock.Mock(),
    )
    def test_connection_failed(self):
        """Assert when the connection fails it is logged."""
        with mock.patch(
            "fedora_messaging.twisted.factory._legacy_twisted_log"
        ) as mock_log:
            self.factory.clientConnectionFailed(None, mock.Mock(value="something"))
        mock_log.msg.assert_called_once_with(
            "Connection to the AMQP broker failed (something)", logLevel=logging.WARNING
        )

    def test_stopTrying(self):
        # The _client_ready deferred must errback when we stop trying to
        # reconnect.
        self.factory._client_ready.addCallbacks(
            self.fail, lambda f: f.trap(pika.exceptions.AMQPConnectionError)
        )
        self.factory.stopTrying()
        return pytest_twisted.blockon(self.factory._client_ready)

    def test_stopFactory(self):
        # The protocol should be stopped when the factory is stopped.
        self.protocol.stopProducing.side_effect = lambda: defer.succeed(None)
        self.factory.buildProtocol(None)
        d = self.factory.stopFactory()

        def _check(_):
            self.protocol.stopProducing.assert_called_once()

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_consume(self):
        """Assert when there is an active protocol, consume calls are forwarded to it."""
        callback = mock.Mock()
        self.protocol.consume.side_effect = lambda cb, queue: defer.succeed(None)
        self.factory.client = self.protocol
        # Pretend the factory is ready to trigger protocol setup.
        self.factory._client_ready.callback(None)
        d = self.factory.consume(callback, "my_queue")

        def _check(_):
            self.assertEqual({"my_queue": callback}, self.factory.consumers)

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_consume_not_ready(self):
        """Assert when a client isn't ready, consume doesn't return a deferred."""
        # Check the consume method
        callback = mock.Mock()
        self.factory.client = self.protocol
        result = self.factory.consume(callback, "my_queue")
        self.assertIsNone(result)

    def test_cancel_not_ready(self):
        """Assert when a client isn't ready, cancel happens immediately."""
        self.factory.consume(mock.Mock(), "my_queue")

        result = self.factory.cancel("my_queue")

        self.assertIsNone(result)
        self.assertEqual({}, self.factory.consumers)

    def test_cancel_invalid(self):
        """Assert when a client isn't ready, cancel happens immediately."""
        cb = mock.Mock()
        self.factory.consume(cb, "my_queue")

        result = self.factory.cancel("my_other_queue")

        self.assertIsNone(result)
        self.assertEqual({"my_queue": cb}, self.factory.consumers)

    def test_cancel_with_client(self):
        """Assert when a client isn't ready, cancel happens immediately."""
        cb = mock.Mock()
        self.factory.consume(cb, "my_queue")
        self.factory.client = mock.Mock()

        self.factory.cancel("my_queue")

        self.factory.client.cancel.assert_called_once_with("my_queue")
        self.assertEqual({}, self.factory.consumers)

    def test_when_connected(self):
        """Assert whenConnected returns the current client once _client_ready fires"""
        self.factory.client = mock.Mock()
        self.factory._client_ready.callback(None)
        d = self.factory.whenConnected()
        d.addCallback(lambda client: self.assertEqual(self.factory.client, client))
        return pytest_twisted.blockon(d)

    def test_publish(self):
        """Assert publish forwards to the next available protocol instance."""
        self.factory.whenConnected = mock.Mock(
            return_value=defer.succeed(self.protocol)
        )
        self.protocol.publish.side_effect = lambda *a: defer.succeed(None)
        d = self.factory.publish("test-message", "test-exchange")

        def _check(_):
            self.protocol.publish.assert_called_once_with(
                "test-message", "test-exchange"
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_publish_connection_closed(self):
        """Assert publish retries when a connection error occurs."""
        self.factory.whenConnected = mock.Mock(
            side_effect=[defer.succeed(self.protocol), defer.succeed(self.protocol)]
        )
        self.protocol.publish.side_effect = [
            ConnectionException(reason="I wanted to"),
            defer.succeed(None),
        ]
        d = self.factory.publish("test-message", "test-exchange")

        def _check(_):
            self.assertEqual(
                [call[0] for call in self.protocol.publish.call_args_list],
                [("test-message", "test-exchange"), ("test-message", "test-exchange")],
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)


class FactoryV2Tests(unittest.TestCase):
    def setUp(self):
        self.protocol = mock.Mock()
        self.protocol.ready = defer.Deferred()
        protocol_class = mock.Mock(side_effect=lambda *a, **kw: self.protocol)
        self.factory = FedoraMessagingFactoryV2(
            mock.Mock(name="parameters"), {"binding key": "binding value"}
        )
        self.factory.protocol = protocol_class

    def test_buildProtocol(self):
        """Assert buildProtocol associates the factory"""
        protocol = self.factory.buildProtocol(None)
        self.assertEqual(protocol.factory, self.factory)

    def test_when_connected(self):
        """Assert when_connected returns the current client once _client_deferred fires"""
        self.factory.buildProtocol(None)
        d = self.factory.when_connected()
        d.addCallback(lambda client: self.assertEqual(self.factory._client, client))
        d.addCallback(
            lambda _: self.assertEqual(self.factory._client_deferred.called, True)
        )
        self.protocol.ready.callback(None)
        return pytest_twisted.blockon(d)

    def test_buildProtocol_twice(self):
        """Assert buildProtocol works when reconnecting"""

        def _get_protocol(*a, **kw):
            protocol = mock.Mock(name="protocol mock")
            protocol.ready = defer.succeed(None)
            return protocol

        self.factory.protocol = _get_protocol
        connector = mock.Mock()
        connected_d = self.factory.when_connected()
        self.factory.buildProtocol(None)
        self.factory.clientConnectionLost(connector, None)
        with mock.patch("fedora_messaging.twisted.factory._std_log") as mock_log:
            protocol = self.factory.buildProtocol(None)
        self.assertFalse(mock_log.exception.called)
        self.assertFalse(mock_log.error.called)
        d = defer.DeferredList([connected_d, protocol.ready], fireOnOneErrback=True)
        d.addErrback(lambda f: f.value.subFailure)
        return pytest_twisted.blockon(d)

    def _test_when_connected_disconnected(self, error_class, error_msg):
        """Assert when_connected errbacks on disconnections."""

        def _get_protocol(*a, **kw):
            protocol = mock.Mock(name="protocol mock")
            # Disconnect immediately
            protocol.ready = defer.fail(error_class())
            return protocol

        def _check(f):
            f.trap(ConnectionException)
            # Make sure a new Deferred has been generated for when_connected()
            new_d = self.factory.when_connected()
            assert new_d.called is False
            assert new_d != connected_d
            assert f.value.reason == error_msg

        self.factory.protocol = _get_protocol
        connected_d = self.factory.when_connected()
        connected_d.addCallbacks(
            lambda r: ValueError("This should fail but I got: {!r}".format(r)), _check
        )
        self.factory.buildProtocol(None)
        return pytest_twisted.blockon(connected_d)

    def test_when_connected_connectiondone(self):
        return self._test_when_connected_disconnected(
            ConnectionDone,
            "The TCP connection appears to have started, but the TLS or AMQP handshake "
            "with the broker failed; check your connection and authentication "
            "parameters and ensure your user has permission to access the vhost",
        )

    def test_when_connected_connectionlost(self):
        return self._test_when_connected_disconnected(
            ConnectionLost,
            "The network connection to the broker was lost in a non-clean fashion (%r);"
            " the connection should be restarted by Twisted.",
        )

    def test_when_connected_unexpected_failure(self):
        """Assert when_connected errbacks when the connection fails."""

        class DummyError(Exception):
            pass

        def _get_protocol(*a, **kw):
            protocol = mock.Mock(name="protocol mock")
            # Fail immediately
            protocol.ready = defer.fail(DummyError())
            return protocol

        def _check(f):
            f.trap(DummyError)
            # Make sure a new Deferred has been generated for when_connected()
            new_d = self.factory.when_connected()
            assert new_d.called is False
            assert new_d != connected_d

        self.factory.protocol = _get_protocol
        connected_d = self.factory.when_connected()
        connected_d.addCallbacks(
            lambda r: ValueError("This should fail but I got: {!r}".format(r)), _check
        )
        with mock.patch("fedora_messaging.twisted.factory._std_log") as mock_log:
            self.factory.buildProtocol(None)
        mock_log.error.assert_called()
        last_log_call_args = mock_log.error.call_args_list[-1][0]
        assert last_log_call_args[0] == (
            "The connection failed with an unexpected exception; please report this bug: %s"
        )
        assert last_log_call_args[1].startswith("Traceback (most recent call last):")
        return pytest_twisted.blockon(connected_d)


@pytest.mark.parametrize(
    "parameters,confirms,msg",
    [
        (
            pika.ConnectionParameters(),
            True,
            (
                "FedoraMessagingFactoryV2(parameters=<ConnectionParameters host=localhost"
                " port=5672 virtual_host=/ ssl=False>, confirms=True)"
            ),
        ),
        (
            pika.ConnectionParameters(
                host="example.com",
                credentials=pika.PlainCredentials("user", "secret"),
                port=5671,
                virtual_host="/pub",
            ),
            True,
            (
                "FedoraMessagingFactoryV2(parameters=<ConnectionParameters host=example.com"
                " port=5671 virtual_host=/pub ssl=False>, confirms=True)"
            ),
        ),
    ],
)
def test_repr(parameters, confirms, msg):
    """Assert __repr__ prints useful information"""
    f = FedoraMessagingFactoryV2(parameters, confirms)
    assert repr(f) == msg
