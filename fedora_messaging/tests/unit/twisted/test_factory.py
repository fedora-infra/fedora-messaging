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

import unittest

import mock
import pika
import pytest
from twisted.internet import defer
from twisted.python.failure import Failure

from fedora_messaging.twisted.factory import FedoraMessagingFactory
from fedora_messaging.exceptions import PublishReturned, ConnectionException


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

    def test_buildProtocol(self):
        # Check the buildProtocol method.
        protocol = self.factory.buildProtocol(None)
        self.assertTrue(protocol is self.protocol)
        self.assertTrue(self.factory.client is self.protocol)
        self.assertTrue(protocol.factory is self.factory)
        self.protocol.ready.callback(None)
        self.assertTrue(self.factory._client_ready.called)

    def test_buildProtocol_already_consuming(self):
        # When a message callback has been previously set, new calls to
        # buildProtocol should setup the reading process.
        self.protocol.setupRead.side_effect = lambda _: defer.succeed(None)
        self.protocol.resumeProducing.side_effect = lambda: defer.succeed(None)
        self.factory._message_callback = mock.Mock(name="callback")
        self.factory.buildProtocol(None)
        self.protocol.ready.callback(None)
        self.protocol.setupRead.assert_called_once_with(self.factory._message_callback)
        self.protocol.resumeProducing.assert_called_once()

    def test_clientConnectionLost(self):
        # The _client_ready deferred must be renewed when the connection is
        # lost.
        self.factory._client_ready.callback(None)
        self.factory.clientConnectionLost(mock.Mock(), Failure(RuntimeError()))
        self.assertFalse(self.factory._client_ready.called)

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
        # Check the consume method
        callback = mock.Mock()
        self.protocol.setupRead.side_effect = lambda _: defer.succeed(None)
        self.protocol.resumeProducing.side_effect = lambda: defer.succeed(None)
        self.factory.client = self.protocol
        # Pretend the factory is ready to trigger protocol setup.
        self.factory._client_ready.callback(None)
        d = self.factory.consume(callback)

        def _check(_):
            self.assertTrue(self.factory._message_callback is callback)
            self.protocol.setupRead.assert_called_once_with(callback)
            self.protocol.resumeProducing.assert_called_once()

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_consume_not_ready(self):
        # Check the consume method
        callback = mock.Mock()
        self.factory.client = self.protocol
        d = self.factory.consume(callback)

        def _check(_):
            self.assertTrue(self.factory._message_callback is callback)
            self.protocol.setupRead.assert_not_called()
            self.protocol.resumeProducing.assert_not_called()

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_publish(self):
        self.factory.client = self.protocol
        self.factory._client_ready.callback(None)
        self.protocol.publish.side_effect = lambda *a: defer.succeed(None)
        d = self.factory.publish("test-message", "test-exchange")

        def _check(_):
            self.protocol.publish.assert_called_once_with(
                "test-message", "test-exchange"
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_publish_connection_closed(self):
        self.factory.client = self.protocol
        self.factory._client_ready.callback(None)
        self.protocol.publish.side_effect = [
            pika.exceptions.ConnectionClosed(42, "testing"),
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

    def test_publish_channel_closed(self):
        self.factory.client = self.protocol
        self.factory._client_ready.callback(None)
        self.protocol.publish.side_effect = [
            pika.exceptions.ChannelClosed(42, "testing"),
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

    def test_publish_nack_unroutable(self):
        self.factory.client = self.protocol
        self.factory._client_ready.callback(None)
        self.protocol.publish.side_effect = [
            pika.exceptions.NackError(messages=[]),
            pika.exceptions.UnroutableError(messages=[]),
        ]
        d1 = self.factory.publish("test-message", "test-exchange")
        d1.addCallbacks(self.fail, lambda f: f.trap(PublishReturned))
        d2 = self.factory.publish("test-message", "test-exchange")
        d2.addCallbacks(self.fail, lambda f: f.trap(PublishReturned))
        return pytest_twisted.blockon(
            defer.DeferredList([d1, d2], fireOnOneErrback=True)
        )

    def test_publish_amqp_error(self):
        self.factory.client = self.protocol
        self.factory._client_ready.callback(None)
        self.protocol.publish.side_effect = pika.exceptions.AMQPError()
        self.protocol.close.side_effect = lambda: defer.succeed(None)
        self.factory.stopTrying = mock.Mock()
        d = self.factory.publish("test-message", "test-exchange")

        def _check(f):
            self.factory.stopTrying.assert_called_once()
            self.protocol.close.assert_called_once()
            f.trap(ConnectionException)

        d.addCallbacks(self.fail, _check)
        return pytest_twisted.blockon(d)
