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

import json
import unittest

import mock
import pika
import pytest
from twisted.internet import defer, error

from fedora_messaging import config
from fedora_messaging.message import Message
from fedora_messaging.exceptions import (
    Nack,
    Drop,
    HaltConsumer,
    NoFreeChannels,
    ConnectionException,
)
from fedora_messaging.twisted.protocol import (
    FedoraMessagingProtocol,
    FedoraMessagingProtocolV2,
    Consumer,
    _add_timeout,
)


try:
    import pytest_twisted
except ImportError:
    pytest.skip("pytest-twisted is missing, skipping tests", allow_module_level=True)


class MockChannel(mock.Mock):
    """A mock object with Channel-specific methods that return Deferreds."""

    def __init__(self, *args, **kwargs):
        super(MockChannel, self).__init__(*args, **kwargs)
        deferred_methods = (
            "basic_qos",
            "confirm_delivery",
            "exchange_declare",
            "queue_bind",
            "basic_ack",
            "basic_nack",
            "basic_publish",
        )
        for method in deferred_methods:
            setattr(
                self,
                method,
                mock.Mock(side_effect=lambda *a, **kw: defer.succeed(None)),
            )
        self.queue_declare = mock.Mock(
            side_effect=lambda **kw: defer.succeed(
                pika.frame.Method(0, pika.spec.Queue.DeclareOk(queue=kw["queue"]))
            )
        )
        self.basic_consume = mock.Mock(
            side_effect=lambda **kw: defer.succeed(
                (mock.Mock(name="queue_object"), "consumer-tag")
            )
        )


class MockProtocol(FedoraMessagingProtocol):
    """A Protocol object that mocks the underlying channel and impl."""

    def __init__(self, *args, **kwargs):
        FedoraMessagingProtocol.__init__(self, *args, **kwargs)
        self._impl = mock.Mock(name="_impl")
        self._impl.is_closed = True
        self._channel = MockChannel(name="_channel")
        self.channel = mock.Mock(
            name="channel", side_effect=lambda: defer.succeed(self._channel)
        )
        self._running = False


class ProtocolTests(unittest.TestCase):
    def setUp(self):
        self.protocol = MockProtocol(None)
        self.factory = mock.Mock()
        self.protocol.factory = self.factory

    def test_allocate_channel_no_more_channels(self):
        """Assert a pika NoFreeChannels exception turns into a fedora_messaging exception."""
        self.protocol.channel = mock.Mock(
            name="channel", side_effect=pika.exceptions.NoFreeChannels()
        )

        d = self.protocol._allocate_channel()
        d.addCallback(pytest.fail, "Expected a NoFreeChannels exception")
        d.addErrback(lambda f: f.trap(NoFreeChannels))
        pytest_twisted.blockon(d)

    @mock.patch(
        "fedora_messaging.twisted.protocol.uuid.uuid4", mock.Mock(return_value="tag1")
    )
    def test_consume_not_running(self):
        """Assert calling consume results in resumeProducing being invoked."""
        self.protocol._running = False
        func = mock.Mock()

        def _check(consumer):
            assert self.protocol._running is True
            consumer.channel.basic_qos.assert_called_with(
                prefetch_count=config.conf["qos"]["prefetch_count"],
                prefetch_size=config.conf["qos"]["prefetch_size"],
            )
            consumer.channel.basic_consume.assert_called_once_with(
                queue="my_queue", consumer_tag="tag1"
            )
            assert self.protocol._consumers["my_queue"] == consumer

        d = self.protocol.consume(func, "my_queue")
        d.addCallback(_check)

        return pytest_twisted.blockon(d)

    def test_consume_twice(self):
        """Assert calling consume on the same queue updates the callback."""

        def cb1():
            pass

        def cb2():
            pass

        def _check(_):
            self.assertEqual(1, len(self.protocol._consumers))
            self.assertEqual(cb2, self.protocol._consumers["my_queue"].callback)

        d = self.protocol.consume(cb1, "my_queue")
        d.addCallback(lambda _: self.protocol.consume(cb2, "my_queue"))
        d.addCallback(_check)

        return pytest_twisted.blockon(d)

    @mock.patch(
        "fedora_messaging.twisted.protocol.uuid.uuid4", mock.Mock(return_value="tag1")
    )
    def test_consume_running(self):
        """Assert when running, consume sets up the AMQP consumer"""
        self.protocol._running = True
        func = mock.Mock()

        def _check(consumer):
            consumer.channel.basic_qos.assert_called_with(
                prefetch_count=config.conf["qos"]["prefetch_count"],
                prefetch_size=config.conf["qos"]["prefetch_size"],
            )
            consumer.channel.basic_consume.assert_called_once_with(
                queue="my_queue", consumer_tag="tag1"
            )
            assert self.protocol._consumers["my_queue"] == consumer

        d = self.protocol.consume(func, "my_queue")
        d.addCallback(_check)

        return pytest_twisted.blockon(d)

    def test_cancel(self):
        """Assert a consumer is removed from the consumer list and canceled."""

        def cb():
            pass

        def _check(_):
            self.assertEqual(1, len(self.protocol._consumers))

        d = self.protocol.consume(cb, "my_queue")
        d.addCallback(lambda _: self.protocol.consume(cb, "my_queue2"))
        d.addCallback(lambda consumer: self.protocol.cancel(consumer.queue))
        d.addCallback(_check)

        return pytest_twisted.blockon(d)

    def test_cancel_no_consumer(self):
        """Assert canceling a non-existent consumer just returns None."""
        d = self.protocol.cancel("my_invalid_queue")
        d.addCallback(lambda ret: self.assertIsNone(ret))

        return pytest_twisted.blockon(d)

    def test_cancel_channel_error(self):
        """Assert channel errors are caught; a closed channel cancels consumers."""
        consumer = Consumer("consumer1", "my_queue", lambda _: _, mock.Mock())
        consumer.channel.basic_cancel.side_effect = pika.exceptions.AMQPChannelError()
        self.protocol._consumers = {"my_queue": consumer}

        def _check(_):
            self.assertEqual({}, self.protocol._consumers)
            consumer.channel.basic_cancel.assert_called_once_with(
                consumer_tag=consumer.tag
            )

        d = self.protocol.consume(consumer.callback, consumer.queue)
        d.addCallback(lambda consumer: self.protocol.cancel(consumer.queue))
        d.addCallback(_check)

        return pytest_twisted.blockon(d)

    def test_connection_ready(self):
        # Check the ready Deferred.
        def _check(_):
            self.protocol.channel.assert_called_once_with()

        d = self.protocol.ready
        d.addCallback(_check)

        self.protocol._on_connection_ready(None)

        return pytest_twisted.blockon(d)

    def test_publish(self):
        # Check the publish method.
        body = {"bodykey": "bodyvalue"}
        headers = {"headerkey": "headervalue"}
        message = Message(body, headers, "testing.topic")
        d = self.protocol.publish(message, "test-exchange")

        def _check(_):
            self.protocol._channel.basic_publish.assert_called_once()
            args = self.protocol._channel.basic_publish.call_args_list[0][1]
            self.assertEqual(args["exchange"], "test-exchange")
            self.assertEqual(args["routing_key"], b"testing.topic")
            self.assertEqual(args["body"], json.dumps(body).encode("utf-8"))
            props = args["properties"]
            self.assertEqual(props.headers, headers)
            self.assertEqual(props.content_encoding, "utf-8")
            self.assertEqual(props.content_type, "application/json")
            self.assertEqual(props.delivery_mode, 2)

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_resumeProducing(self):
        # Check the resumeProducing method.
        self.protocol._running = False
        callback = mock.Mock()
        self.protocol._consumers = {
            "testqueue1": Consumer(
                "consumer1", "testqueue1", callback, self.protocol._channel
            ),
            "testqueue2": Consumer(
                "consumer2", "testqueue2", callback, self.protocol._channel
            ),
            "testqueue3": Consumer(
                "consumer3", "testqueue3", callback, self.protocol._channel
            ),
        }
        self.protocol._read = mock.Mock(side_effect=lambda _, __: defer.succeed(None))
        d = self.protocol.resumeProducing()

        def _check(_):
            self.assertTrue(self.protocol._running)
            self.assertEqual(self.protocol._channel.basic_consume.call_count, 3)
            called_queues = [
                kw["queue"]
                for arg, kw in self.protocol._channel.basic_consume.call_args_list
            ]
            self.assertEqual(
                sorted(called_queues), sorted(self.protocol._consumers.keys())
            )
            self.assertEqual(self.protocol._read.call_count, 3)

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_pauseProducing(self):
        # Check the pauseProducing method.
        self.protocol._running = True
        self.protocol._consumers = {
            "testqueue1": Consumer("ct1", "testqueue1", None, self.protocol._channel),
            "testqueue2": Consumer("ct2", "testqueue2", None, self.protocol._channel),
        }
        d = self.protocol.pauseProducing()

        def _check(_):
            self.assertFalse(self.protocol._running)
            self.assertEqual(self.protocol._channel.basic_cancel.call_count, 2)
            called_cts = [
                kw["consumer_tag"]
                for arg, kw in self.protocol._channel.basic_cancel.call_args_list
            ]
            self.assertEqual(sorted(called_cts), sorted(["ct1", "ct2"]))

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_pauseProducing_not_running(self):
        # The pauseProducing method should do nothing when the protocol is not
        # consuming messages..
        self.protocol._channel.consumer_tags = ["ct1", "ct2"]
        self.protocol._running = False
        d = self.protocol.pauseProducing()

        def _check(_):
            self.protocol._channel.basic_cancel.assert_not_called()

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_pauseProducing_no_channel(self):
        # The pauseProducing method should do nothing when the protocol has no
        # channel yet.
        self.protocol._channel = None
        d = self.protocol.pauseProducing()
        return pytest_twisted.blockon(d)

    def test_stopProducing(self):
        # Check the stopProducing method.
        self.protocol._channel.consumer_tags = []
        self.protocol.close = mock.Mock()
        self.protocol._running = True
        self.protocol._impl.is_closed = False
        d = self.protocol.stopProducing()

        def _check(_):
            self.assertFalse(self.protocol._running)
            self.protocol.close.assert_called()
            self.assertIsNone(self.protocol._channel)

        d.addCallback(_check)
        return pytest_twisted.blockon(d)


class ProtocolReadTests(unittest.TestCase):
    """Test the _read() method on the Protocol."""

    def setUp(self):
        self.protocol = MockProtocol(None)
        self.protocol._running = True
        self.protocol._impl.is_closed = False
        self.protocol._on_message = mock.Mock()
        self.queue = mock.Mock()
        self.consumer = Consumer("consumer1", "my_queue_name", lambda _: _, mock.Mock())

    def test_read_not_running(self):
        # When not running, _read() should do nothing.
        self.queue.get.side_effect = lambda: defer.succeed(None)
        self.protocol._running = False
        d = self.protocol._read(self.queue, self.consumer)

        def _check(_):
            self.queue.get.assert_not_called()

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_read(self):
        # Check the nominal case for _read().
        self.queue.get.side_effect = [
            defer.succeed((None, "df1", "prop1", "body1")),
            defer.succeed((None, "df2", "prop2", "body2")),
            defer.succeed((None, "df3", "prop3", "body3")),
            pika.exceptions.ChannelClosed(42, "testing"),
        ]

        def _check(_):
            self.assertEqual(
                self.protocol._on_message.call_args_list,
                [
                    (("df1", "prop1", "body1", self.consumer), {}),
                    (("df2", "prop2", "body2", self.consumer), {}),
                    (("df3", "prop3", "body3", self.consumer), {}),
                ],
            )

        d = self.protocol._read(self.queue, self.consumer)
        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_read_no_body(self):
        # When a message has no body, it should not be passed to the callback.
        self.queue.get.side_effect = [
            defer.succeed((None, None, None, None)),
            defer.succeed((None, None, None, "")),
        ]
        d = self.protocol._read(self.queue, self.consumer)

        def _check(_):
            self.protocol._on_message.assert_not_called()

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_read_exit_loop(self):
        # Check the exceptions that cause the read loop to exit.
        exceptions = [
            error.ConnectionDone(),
            pika.exceptions.ChannelClosed(42, "testing"),
            pika.exceptions.ConsumerCancelled(),
            RuntimeError(),
            pika.exceptions.ChannelClosedByClient(42, "testing"),
        ]

        deferreds = []
        for exc in exceptions:
            queue = mock.Mock()
            queue.get.side_effect = exc
            deferreds.append(self.protocol._read(queue, self.consumer))

        def _check(_):
            self.protocol._on_message.assert_not_called()

        d = defer.DeferredList(deferreds)
        d.addCallback(_check)
        return pytest_twisted.blockon(d)


class ProtocolOnMessageTests(unittest.TestCase):
    """Test the _on_message() method on the Protocol."""

    def setUp(self):
        self.protocol = MockProtocol(None)
        self._message_callback = mock.Mock()
        self.protocol._impl.is_closed = False
        self.protocol._consumers["my_queue_name"] = Consumer(
            "consumer1", "my_queue_name", self._message_callback, mock.Mock()
        )

    def _call_on_message(self, topic, headers, body):
        """Prepare arguments for the _on_message() method and call it."""
        full_headers = {"fedora_messaging_schema": "fedora_messaging.message:Message"}
        full_headers.update(headers)
        return self.protocol._on_message(
            pika.spec.Basic.Deliver(routing_key=topic, delivery_tag="delivery_tag"),
            pika.spec.BasicProperties(headers=full_headers),
            json.dumps(body).encode("utf-8"),
            self.protocol._consumers["my_queue_name"],
        )

    def test_on_message(self):
        # Check the nominal case.
        channel = self.protocol._consumers["my_queue_name"].channel
        d = self._call_on_message("testing.topic", {}, {"key": "value"})

        def _check(_):
            self._message_callback.assert_called()
            channel.basic_ack.assert_called_with(delivery_tag="delivery_tag")

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_on_message_invalid(self):
        # When a message is invalid, it should be Nacked.
        channel = self.protocol._consumers["my_queue_name"].channel
        d = self._call_on_message("testing.topic", {}, "testing")

        def _check(_):
            self._message_callback.assert_not_called()
            channel.basic_nack.assert_called_with(
                delivery_tag="delivery_tag", requeue=False
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_on_message_nack(self):
        # When the callback raises a Nack, the server should be notified.
        self._message_callback.side_effect = Nack()
        channel = self.protocol._consumers["my_queue_name"].channel
        d = self._call_on_message("testing.topic", {}, {"key": "value"})

        def _check(_):
            self._message_callback.assert_called()
            channel.basic_nack.assert_called_with(
                delivery_tag="delivery_tag", requeue=True
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_on_message_drop(self):
        # When the callback raises a Drop, the server should be notified.
        self._message_callback.side_effect = Drop()
        channel = self.protocol._consumers["my_queue_name"].channel
        d = self._call_on_message("testing.topic", {}, {"key": "value"})

        def _check(_):
            self._message_callback.assert_called()
            channel.basic_nack.assert_called_with(
                delivery_tag="delivery_tag", requeue=False
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_on_message_halt(self):
        """Assert the consumer is canceled when HaltConsumer is raised"""
        self._message_callback.side_effect = HaltConsumer()
        channel = self.protocol._consumers["my_queue_name"].channel
        tag = self.protocol._consumers["my_queue_name"].tag
        self.protocol._running = True
        d = self._call_on_message("testing.topic", {}, {"key": "value"})

        def _check(_):
            self._message_callback.assert_called()
            channel.basic_ack.assert_called_with(delivery_tag="delivery_tag")
            channel.basic_cancel.assert_called_with(consumer_tag=tag)
            channel.close.assert_called_once_with()
            self.assertTrue(self.protocol._running)

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_on_message_exception(self):
        # On an unknown exception, the consumer should stop and all
        # unacknowledged messages should be requeued.
        self._message_callback.side_effect = ValueError()
        channel = self.protocol._consumers["my_queue_name"].channel
        self.protocol._running = True
        d = self._call_on_message("testing.topic", {}, {"key": "value"})

        def _check(_):
            self._message_callback.assert_called()
            channel.basic_nack.assert_called_with(
                delivery_tag=0, multiple=True, requeue=True
            )
            self.assertTrue(self.protocol._running)
            channel.close.assert_called_once_with()

        d.addCallback(_check)
        return pytest_twisted.blockon(d)


class ProtocolV2Tests(unittest.TestCase):
    """Unit tests for the ProtocolV2 class."""

    def test_consume_connection_exception(self):
        """If consuming fails due to a non-permission error, a ConnectionException happens."""
        proto = FedoraMessagingProtocolV2(None)
        mock_channel = mock.Mock()
        mock_channel.basic_consume.side_effect = pika.exceptions.ChannelClosed(
            400, "Bad Request!"
        )
        deferred_channel = defer.succeed(mock_channel)
        proto._allocate_channel = mock.Mock(return_value=deferred_channel)

        def check(failure):
            assert isinstance(failure.value, ConnectionException)

        d = proto.consume(lambda x: x, "test_queue")
        d.addBoth(check)
        return pytest_twisted.blockon(d)


class AddTimeoutTests(unittest.TestCase):
    """Unit tests for the _add_timeout helper function."""

    def test_twisted12_timeout(self):
        """Assert timeouts work for Twisted 12.2 (EL7)"""
        d = defer.Deferred()
        d.addTimeout = mock.Mock(side_effect=AttributeError())
        _add_timeout(d, 0.1)

        d.addCallback(self.fail, "Expected errback to be called")
        d.addErrback(
            lambda failure: self.assertIsInstance(failure.value, defer.CancelledError)
        )

        return pytest_twisted.blockon(d)

    def test_twisted12_cancel_cancel_callback(self):
        """Assert canceling the cancel call for Twisted 12.2 (EL7) works."""
        d = defer.Deferred()
        d.addTimeout = mock.Mock(side_effect=AttributeError())
        d.cancel = mock.Mock()
        with mock.patch("fedora_messaging.twisted.protocol.reactor") as mock_reactor:
            _add_timeout(d, 1)
            delayed_cancel = mock_reactor.callLater.return_value
            d.callback(None)
            delayed_cancel.cancel.assert_called_once_with()
