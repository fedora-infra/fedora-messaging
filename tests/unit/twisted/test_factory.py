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


from unittest import mock

import pika
import pytest
from twisted.internet import defer
from twisted.internet.error import ConnectionDone, ConnectionLost

from fedora_messaging.exceptions import ConnectionException
from fedora_messaging.twisted.consumer import Consumer
from fedora_messaging.twisted.factory import ConsumerRecord, FedoraMessagingFactoryV2


try:
    import pytest_twisted  # noqa: F401
except ImportError:
    pytest.skip("pytest-twisted is missing, skipping tests", allow_module_level=True)


class TestFactoryV2:
    def setup_method(self, method):
        self.protocol = mock.Mock()
        self.protocol.ready = defer.Deferred()
        self.protocol.is_closed = False
        protocol_class = mock.Mock(side_effect=lambda *a, **kw: self.protocol)
        self.factory = FedoraMessagingFactoryV2(
            mock.Mock(name="parameters"), {"binding key": "binding value"}
        )
        self.factory.protocol = protocol_class

    def test_buildProtocol(self):
        """Assert buildProtocol associates the factory"""
        protocol = self.factory.buildProtocol(None)
        assert protocol.factory == self.factory

    def test_when_connected(self):
        """Assert when_connected returns the current client once _client_deferred fires"""
        self.factory.buildProtocol(None)
        d = self.factory.when_connected()

        def assert_equal(a, b):
            assert a == b

        def assert_is(a, b):
            assert a is b

        d.addCallback(lambda client: assert_equal(self.factory._client, client))
        d.addCallback(lambda _: assert_is(self.factory._client_deferred.called, True))
        self.protocol.ready.callback(None)
        return d

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
        assert not mock_log.exception.called
        assert not mock_log.error.called
        d = defer.DeferredList([connected_d, protocol.ready], fireOnOneErrback=True)
        d.addErrback(lambda f: f.value.subFailure)
        return d

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
            lambda r: ValueError(f"This should fail but I got: {r!r}"), _check
        )
        self.factory.buildProtocol(None)
        return connected_d

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
            lambda r: ValueError(f"This should fail but I got: {r!r}"), _check
        )
        with mock.patch("fedora_messaging.twisted.factory._std_log") as mock_log:
            self.factory.buildProtocol(None)
        mock_log.error.assert_called()
        last_log_call_args = mock_log.error.call_args_list[-1][0]
        assert last_log_call_args[0] == (
            "The connection failed with an unexpected exception; please report this bug: %s"
        )
        assert last_log_call_args[1].startswith("Traceback (most recent call last):")
        return connected_d

    def test_consume_anonymous(self):
        """Assert consume handles anonymous queues."""
        # Use server-generated queue names
        queue_config = {
            "queue": "",
            "durable": False,
            "auto_delete": True,
            "exclusive": True,
        }
        declared_queue = mock.Mock()
        self.protocol.declare_queue.side_effect = lambda q: declared_queue
        # Mock the consume call
        callback = mock.Mock()
        self.protocol.consume.side_effect = lambda cb, queue: defer.succeed(
            Consumer(queue=queue, callback=cb)
        )
        bindings = [{"exchange": "amq.topic", "routing_keys": ["#"]}]
        expected_bindings = [
            {"queue": declared_queue, "exchange": "amq.topic", "routing_key": "#"}
        ]

        self.factory.buildProtocol(None)
        self.protocol.ready.callback(None)
        d = self.factory.when_connected()

        def _consume(_):
            return self.factory.consume(callback, bindings, {"": queue_config})

        def _check(_):
            assert len(self.factory._consumers) == 1
            consumer = self.factory._consumers[0].consumer
            assert consumer.queue == declared_queue
            assert consumer.callback == callback
            assert queue_config == self.factory._consumers[0].queue
            assert expected_bindings == self.factory._consumers[0].bindings

            self.protocol.declare_queue.assert_called_once_with(queue_config)
            self.protocol.bind_queues.assert_called_once_with(expected_bindings)
            self.protocol.consume.assert_called_once_with(callback, declared_queue)

        d.addCallback(_consume)
        d.addCallback(_check)
        return d

    def test_consume_anonymous_reconnect(self):
        """Assert consume handles reconnecting anonymous queues."""
        # Use server-generated queue names
        queue_config = {
            "queue": "",
            "durable": False,
            "auto_delete": True,
            "exclusive": True,
        }
        queue_orig = mock.Mock(name="queue_orig")
        queue_new = mock.Mock(name="queue_new")
        self.protocol.declare_queue.side_effect = lambda q: queue_new
        # Prepare the mocked existing consumer
        callback = mock.Mock()
        bindings = [{"exchange": "amq.topic", "routing_key": "#"}]
        expected_bindings = [
            {"queue": queue_new, "exchange": "amq.topic", "routing_key": "#"}
        ]
        consumer = Consumer(queue=queue_orig, callback=callback)
        self.factory._consumers = [
            ConsumerRecord(consumer=consumer, queue=queue_config, bindings=bindings)
        ]

        self.factory.buildProtocol(None)
        self.protocol.ready.callback(None)
        d = self.factory.when_connected()

        def _check(_):
            assert len(self.factory._consumers) == 1
            self.protocol.declare_queue.assert_called_once_with(queue_config)
            self.protocol.bind_queues.assert_called_once_with(expected_bindings)
            self.protocol.consume.assert_called_once_with(callback, queue_new, consumer)

        d.addCallback(_check)
        return d


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
