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


import json
from unittest import mock, TestCase

import pika
import pytest
from twisted.internet import defer, error

from fedora_messaging.exceptions import (
    ConnectionException,
    ConsumerCanceled,
    Drop,
    HaltConsumer,
    Nack,
    PermissionException,
)
from fedora_messaging.twisted.consumer import _add_timeout, Consumer

from .utils import MockProtocol


try:
    import pytest_twisted
except ImportError:
    pytest.skip("pytest-twisted is missing, skipping tests", allow_module_level=True)


class MockDeliveryFrame:
    def __init__(self, tag, routing_key=None):
        self.delivery_tag = tag
        self.routing_key = routing_key or "routing_key"


class MockProperties:
    def __init__(self, msgid=None):
        self.headers = {}
        self.content_encoding = None
        self.message_id = msgid or "msgid"


class ConsumerTests(TestCase):
    """Unit tests for the Consumer class."""

    def setUp(self):
        self.protocol = MockProtocol(None)
        self.protocol._impl.is_closed = False
        self.callback = mock.Mock()
        self.consumer = Consumer("my_queue_name", self.callback)
        self.protocol._register_consumer(self.consumer)
        self.protocol.factory = mock.Mock()

    # Canceling

    def test_cancel(self):
        """The cancel method must call the corresponding channel methods"""

        def check(_):
            self.assertEqual(len(self.protocol._consumers), 0)
            # self.protocol._forget_consumer.assert_called_with("queue")
            channel = self.protocol._channel
            channel.basic_cancel.assert_called_with(consumer_tag=self.consumer._tag)
            channel.close.assert_called_with()

        d = self.consumer.cancel()
        d.addCallback(check)
        return pytest_twisted.blockon(d)

    def test_cancel_channel_error(self):
        """Assert channel errors are caught; a closed channel cancels consumers."""
        self.consumer._channel.basic_cancel.side_effect = (
            pika.exceptions.AMQPChannelError()
        )

        def _check(_):
            self.assertEqual(len(self.protocol._consumers), 0)
            # consumer._protocol._forget_consumer.assert_called_with("my_queue")
            self.consumer._channel.basic_cancel.assert_called_once_with(
                consumer_tag=self.consumer._tag
            )

        d = self.consumer.cancel()
        d.addCallback(_check)

        return pytest_twisted.blockon(d)

    # Consuming

    def _queue_contents(self, values):
        yield from values
        self.consumer._running = False
        yield defer.CancelledError()

    def _call_read_one(self, topic, headers, body):
        """Prepare arguments for the _read_one() method and call it."""
        # consumer = self.protocol._consumers["my_queue_name"]
        full_headers = {"fedora_messaging_schema": "fedora_messaging.message:Message"}
        full_headers.update(headers)
        queue = mock.Mock()
        queue.get.return_value = defer.succeed(
            (
                self.consumer._channel,
                pika.spec.Basic.Deliver(routing_key=topic, delivery_tag="delivery_tag"),
                pika.spec.BasicProperties(headers=full_headers),
                json.dumps(body).encode("utf-8"),
            )
        )
        return self.consumer._read_one(queue)

    @mock.patch("fedora_messaging.twisted.consumer.get_message")
    def test_read(self, get_message):
        # Check the nominal case for _read().
        message = mock.Mock(name="message")
        get_message.return_value = message
        props = MockProperties()
        queue = mock.Mock()
        queue.get.side_effect = self._queue_contents(
            [
                defer.succeed(
                    (
                        self.consumer._channel,
                        MockDeliveryFrame("dt1", "rk1"),
                        props,
                        "body1",
                    )
                ),
                defer.succeed(
                    (
                        self.consumer._channel,
                        MockDeliveryFrame("dt2", "rk2"),
                        props,
                        "body2",
                    )
                ),
                defer.succeed(
                    (
                        self.consumer._channel,
                        MockDeliveryFrame("dt3", "rk3"),
                        props,
                        "body3",
                    )
                ),
            ]
        )

        def _check(_):
            self.assertEqual(
                get_message.call_args_list,
                [
                    (("rk1", props, "body1"), {}),
                    (("rk2", props, "body2"), {}),
                    (("rk3", props, "body3"), {}),
                ],
            )
            self.assertEqual(message.queue, "my_queue_name")
            self.assertEqual(
                self.callback.call_args_list,
                [
                    ((message,), {}),
                    ((message,), {}),
                    ((message,), {}),
                ],
            )
            self.assertEqual(
                self.consumer._channel.basic_ack.call_args_list,
                [
                    (tuple(), dict(delivery_tag="dt1")),
                    (tuple(), dict(delivery_tag="dt2")),
                    (tuple(), dict(delivery_tag="dt3")),
                ],
            )

        d = self.consumer._read(queue)
        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_read_not_running(self):
        # When not running, _read() should do nothing.
        self.consumer._running = False
        queue = mock.Mock()
        queue.get.side_effect = lambda: defer.succeed(None)
        d = self.consumer._read(queue)

        def _check(_):
            queue.get.assert_not_called()

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    # Running the callback and handling callback errors

    def test_message_invalid(self):
        # When a message is invalid, it should be Nacked.
        d = self._call_read_one("testing.topic", {}, "invalid-json-body")

        def _check(_):
            self.callback.assert_not_called()
            self.consumer._channel.basic_nack.assert_called_with(
                delivery_tag="delivery_tag", requeue=False
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_callback_nack(self):
        # When the callback raises a Nack, the server should be notified.
        self.callback.side_effect = Nack()
        d = self._call_read_one("testing.topic", {}, {"key": "value"})

        def _check(_):
            self.callback.assert_called()
            self.consumer._channel.basic_nack.assert_called_with(
                delivery_tag="delivery_tag", requeue=True
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_callback_drop(self):
        # When the callback raises a Drop, the server should be notified.
        self.callback.side_effect = Drop()
        d = self._call_read_one("testing.topic", {}, {"key": "value"})

        def _check(_):
            self.callback.assert_called()
            self.consumer._channel.basic_nack.assert_called_with(
                delivery_tag="delivery_tag", requeue=False
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_callback_halt(self):
        """Assert the consumer is canceled when HaltConsumer is raised"""
        self.callback.side_effect = HaltConsumer()
        d = self._call_read_one("testing.topic", {}, {"key": "value"})

        def _check(_):
            self.callback.assert_called()
            channel = self.consumer._channel
            channel.basic_ack.assert_called_with(delivery_tag="delivery_tag")
            channel.basic_cancel.assert_called_with(consumer_tag=self.consumer._tag)
            channel.close.assert_called_once_with()

        d.addCallback(_check)
        d.addErrback(lambda failure: self.assertIsInstance(failure.value, HaltConsumer))
        return pytest_twisted.blockon(d)

    def test_callback_exception(self):
        # On an unknown exception, the consumer should stop and all
        # unacknowledged messages should be requeued.
        self.callback.side_effect = ValueError()
        d = self._call_read_one("testing.topic", {}, {"key": "value"})

        def _check(_):
            self.callback.assert_called()
            channel = self.consumer._channel
            channel.basic_nack.assert_called_with(
                delivery_tag=0, multiple=True, requeue=True
            )
            channel.close.assert_called_once_with()

        d.addCallback(_check)
        d.addErrback(lambda failure: self.assertIsInstance(failure.value, ValueError))
        return pytest_twisted.blockon(d)

    # Handling read errors

    def test_consume_channel_closed(self):
        # Check consuming when the channel is closed
        self.consumer._channel.basic_consume.side_effect = (
            pika.exceptions.ChannelClosed(42, "testing")
        )
        self.consumer._read = mock.Mock()

        def _check(failure):
            self.assertIsNone(self.consumer._read_loop)
            self.consumer._read.assert_not_called()
            failure.check(ConnectionException)

        d = self.consumer.consume()
        d.addCallbacks(self.fail, _check)
        return pytest_twisted.blockon(d)

    def test_consume_channel_forbidden(self):
        # Check consuming when the channel is forbidden
        self.consumer._channel.basic_consume.side_effect = (
            pika.exceptions.ChannelClosed(403, "testing")
        )
        self.consumer._read = mock.Mock()

        def _check(failure):
            self.assertIsNone(self.consumer._read_loop)
            self.consumer._read.assert_not_called()
            failure.check(PermissionException)

        d = self.consumer.consume()
        d.addCallbacks(self.fail, _check)
        return pytest_twisted.blockon(d)

    @mock.patch("fedora_messaging.twisted.consumer._std_log")
    def test_exit_loop_connection_done(self, log):
        # Check the exceptions that cause the read loop to exit.
        queue = mock.Mock()
        queue.get.side_effect = error.ConnectionDone()
        self.consumer._channel.queue_object = queue
        self.consumer.consume()

        def _check(_):
            self.callback.assert_not_called()
            log.warning.assert_called()
            self.assertIn(
                "The connection to the broker was lost", log.warning.call_args[0][0]
            )
            # Temporary error, will restart
            self.assertFalse(self.consumer.result.called)

        d = self.consumer._read_loop
        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    @mock.patch("fedora_messaging.twisted.consumer._std_log")
    def test_exit_loop_channel_closed(self, log):
        # Check the exceptions that cause the read loop to exit.
        queue = mock.Mock()
        queue.get.side_effect = pika.exceptions.ChannelClosed(42, "testing")
        self.consumer._channel.queue_object = queue
        self.consumer.consume()

        def _check(_):
            self.callback.assert_not_called()
            log.exception.assert_called()
            logmsg = log.exception.call_args[0][0]
            self.assertIn("Consumer halted", logmsg)
            self.assertIn("the connection should restart", logmsg)
            # Temporary error, will restart
            self.assertFalse(self.consumer.result.called)

        d = self.consumer._read_loop
        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_exit_loop_channel_forbidden(self):
        # Check the exceptions that cause the read loop to exit.
        queue = mock.Mock()
        queue.get.side_effect = pika.exceptions.ChannelClosed(403, "nope!")
        self.consumer._channel.queue_object = queue
        self.consumer.consume()

        def _check(_):
            self.callback.assert_not_called()
            # Permanent error, no restart
            self.assertFalse(self.consumer._running)
            self.assertTrue(self.consumer.result.called)
            self.consumer.result.addErrback(lambda f: f.check(PermissionException))
            # The consumer should have been cancelled and the channel should have been closed
            self.consumer._channel.basic_cancel.assert_called_with(
                consumer_tag=self.consumer._tag
            )
            self.consumer._channel.close.assert_called_with()
            # Check the result's errback
            return self.consumer.result

        d = self.consumer._read_loop
        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    @mock.patch("fedora_messaging.twisted.consumer._std_log")
    def test_exit_loop_consumer_cancelled(self, log):
        # Check the exceptions that cause the read loop to exit.
        queue = mock.Mock()
        queue.get.side_effect = pika.exceptions.ConsumerCancelled()
        self.consumer._channel.queue_object = queue
        self.consumer.consume()

        def _check(_):
            self.callback.assert_not_called()
            # Permanent error, no restart
            self.assertFalse(self.consumer._running)
            self.assertEqual(len(self.protocol._consumers), 0)
            self.assertTrue(self.consumer.result.called)
            self.consumer.result.addErrback(lambda f: f.check(ConsumerCanceled))
            return self.consumer.result

        d = self.consumer._read_loop
        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    @mock.patch("fedora_messaging.twisted.consumer._std_log")
    def test_exit_loop_amqp_error(self, log):
        # Check the exceptions that cause the read loop to exit.
        queue = mock.Mock()
        queue.get.side_effect = pika.exceptions.AMQPHeartbeatTimeout()
        self.consumer._channel.queue_object = queue
        self.consumer.consume()

        def _check(_):
            self.callback.assert_not_called()
            log.exception.assert_called()
            self.assertIn(
                "An unexpected AMQP error occurred; the connection should restart",
                log.exception.call_args[0][0],
            )
            # Temporary error, will restart
            self.assertFalse(self.consumer.result.called)

        d = self.consumer._read_loop
        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_exit_loop_unknown_error(self):
        # Check the exceptions that cause the read loop to exit.
        queue = mock.Mock()
        queue.get.side_effect = RuntimeError()
        self.consumer._channel.queue_object = queue
        self.consumer.consume()

        def _check(_):
            self.callback.assert_not_called()
            # Permanent error, no restart
            self.assertFalse(self.consumer._running)
            self.assertTrue(self.consumer.result.called)
            self.consumer.result.addErrback(lambda f: f.check(RuntimeError))
            # The consumer should have been cancelled and the channel should have been closed
            self.consumer._channel.basic_cancel.assert_called_with(
                consumer_tag=self.consumer._tag
            )
            self.consumer._channel.close.assert_called_with()
            # Check the result's errback
            return self.consumer.result

        d = self.consumer._read_loop
        d.addCallback(_check)
        return pytest_twisted.blockon(d)


class AddTimeoutTests(TestCase):
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
        with mock.patch("fedora_messaging.twisted.consumer.reactor") as mock_reactor:
            _add_timeout(d, 1)
            delayed_cancel = mock_reactor.callLater.return_value
            d.callback(None)
            delayed_cancel.cancel.assert_called_once_with()
