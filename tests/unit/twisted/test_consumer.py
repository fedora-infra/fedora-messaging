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
from unittest.mock import Mock, patch

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

try:
    from unittest.mock import AsyncMock
except ImportError:
    from mock import AsyncMock


class MockDeliveryFrame:
    def __init__(self, tag, routing_key=None):
        self.delivery_tag = tag
        self.routing_key = routing_key or "routing_key"


class MockProperties:
    def __init__(self, msgid=None):
        self.headers = {}
        self.content_encoding = None
        self.message_id = msgid or "msgid"


def _queue_contents(consumer, values):
    yield from values
    consumer._running = False
    yield defer.CancelledError()


def _call_read_one(consumer, topic, headers, body):
    """Prepare arguments for the _read_one() method and call it."""
    # consumer = self.protocol._consumers["my_queue_name"]
    full_headers = {
        "fedora_messaging_schema": "base.message",
        "fedora_messaging_content_encoding": "utf-8",
    }
    full_headers.update(headers)
    queue = Mock()
    queue.get.return_value = defer.succeed(
        (
            consumer._channel,
            pika.spec.Basic.Deliver(routing_key=topic, delivery_tag="delivery_tag"),
            pika.spec.BasicProperties(headers=full_headers),
            json.dumps(body).encode("utf-8"),
        )
    )
    return consumer._read_one(queue)


def _make_consumer_with_callback(callback):
    protocol = MockProtocol(None)
    protocol._impl.is_closed = False
    consumer = Consumer("my_queue_name", callback)
    protocol._register_consumer(consumer)
    protocol.factory = Mock()
    return consumer


class TestConsumer:
    """Unit tests for the Consumer class."""

    def setup_method(self):
        self.callback = Mock()
        self.consumer = _make_consumer_with_callback(self.callback)
        self.protocol = self.consumer._protocol

    # Canceling

    @pytest_twisted.inlineCallbacks
    def test_cancel(self):
        """The cancel method must call the corresponding channel methods"""
        yield self.consumer.cancel()
        assert len(self.protocol._consumers) == 0
        # self.protocol._forget_consumer.assert_called_with("queue")
        channel = self.protocol._channel
        channel.basic_cancel.assert_called_with(consumer_tag=self.consumer._tag)
        channel.close.assert_called_with()

    @pytest_twisted.inlineCallbacks
    def test_cancel_channel_error(self):
        """Assert channel errors are caught; a closed channel cancels consumers."""
        self.consumer._channel.basic_cancel.side_effect = (
            pika.exceptions.AMQPChannelError()
        )
        yield self.consumer.cancel()
        assert len(self.protocol._consumers) == 0
        # consumer._protocol._forget_consumer.assert_called_with("my_queue")
        self.consumer._channel.basic_cancel.assert_called_once_with(
            consumer_tag=self.consumer._tag
        )

    # Consuming

    @pytest_twisted.inlineCallbacks
    def test_read(self, mocker):
        message = Mock(name="message")
        get_message = mocker.patch(
            "fedora_messaging.twisted.consumer.get_message", return_value=message
        )
        # Check the nominal case for _read().
        props = MockProperties()
        queue = Mock()
        queue.get.side_effect = _queue_contents(
            self.consumer,
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
            ],
        )

        yield self.consumer._read(queue)

        assert get_message.call_args_list == [
            (("rk1", props, "body1"), {}),
            (("rk2", props, "body2"), {}),
            (("rk3", props, "body3"), {}),
        ]
        assert message.queue == "my_queue_name"
        assert self.callback.call_args_list == [
            ((message,), {}),
            ((message,), {}),
            ((message,), {}),
        ]
        assert self.consumer._channel.basic_ack.call_args_list == [
            (tuple(), dict(delivery_tag="dt1")),
            (tuple(), dict(delivery_tag="dt2")),
            (tuple(), dict(delivery_tag="dt3")),
        ]

    @pytest_twisted.inlineCallbacks
    def test_read_not_running(self):
        # When not running, _read() should do nothing.
        self.consumer._running = False
        queue = Mock()
        queue.get.side_effect = lambda: defer.succeed(None)
        yield self.consumer._read(queue)
        queue.get.assert_not_called()

    @pytest_twisted.inlineCallbacks
    def test_message_invalid(self):
        # When a message is invalid, it should be Nacked.
        yield _call_read_one(self.consumer, "testing.topic", {}, "invalid-json-body")
        self.callback.assert_not_called()
        self.consumer._channel.basic_nack.assert_called_with(
            delivery_tag="delivery_tag", requeue=False
        )

    @pytest.mark.parametrize("error_class", [HaltConsumer, ValueError])
    @pytest_twisted.inlineCallbacks
    def test_read_exception(self, mocker, error_class):
        # Check that some exceptions from the callback cancel the consumer.
        self.callback.side_effect = error_class()
        message = Mock(name="message")
        mocker.patch(
            "fedora_messaging.twisted.consumer.get_message", return_value=message
        )
        props = MockProperties()
        yield self.consumer._channel.queue_object.put(
            (
                self.consumer._channel,
                MockDeliveryFrame("dt1", "rk1"),
                props,
                "body1",
            )
        )
        self.consumer.cancel = Mock(return_value=defer.succeed(None))

        yield self.consumer.consume()
        yield self.consumer._read_loop
        try:
            yield self.consumer.result
        except error_class:
            pass
        else:
            assert False, f"This should have raised {error_class}"

        self.consumer.cancel.assert_called_once_with()
        if error_class == HaltConsumer:
            self.consumer._channel.basic_ack.assert_called_once_with(delivery_tag="dt1")
        else:
            self.consumer._channel.basic_nack.assert_called_once_with(
                delivery_tag=0, multiple=True, requeue=True
            )

    # Handling read errors

    @pytest_twisted.inlineCallbacks
    def test_consume_channel_closed(self):
        # Check consuming when the channel is closed
        self.consumer._channel.basic_consume.side_effect = (
            pika.exceptions.ChannelClosed(42, "testing")
        )
        self.consumer._read = Mock()

        try:
            yield self.consumer.consume()
        except ConnectionException:
            assert self.consumer._read_loop is None
            self.consumer._read.assert_not_called()
        else:
            assert False, "This should have raised ConnectionException"

    @pytest_twisted.inlineCallbacks
    def test_consume_channel_forbidden(self):
        # Check consuming when the channel is forbidden
        self.consumer._channel.basic_consume.side_effect = (
            pika.exceptions.ChannelClosed(403, "testing")
        )
        self.consumer._read = Mock()

        try:
            yield self.consumer.consume()
        except PermissionException:
            assert self.consumer._read_loop is None
            self.consumer._read.assert_not_called()
        else:
            assert False, "This should have raised PermissionException"

    @pytest_twisted.inlineCallbacks
    def test_exit_loop_connection_done(self, mocker):
        # Check the exceptions that cause the read loop to exit.
        log = mocker.patch("fedora_messaging.twisted.consumer._std_log")
        queue = Mock()
        queue.get.side_effect = error.ConnectionDone()
        self.consumer._channel.queue_object = queue
        yield self.consumer.consume()

        yield self.consumer._read_loop
        self.callback.assert_not_called()
        log.warning.assert_called()
        assert ("The connection to the broker was lost") in log.warning.call_args[0][0]
        # Temporary error, will restart
        assert self.consumer.result.called is False

    @pytest_twisted.inlineCallbacks
    def test_exit_loop_channel_closed(self, mocker):
        # Check the exceptions that cause the read loop to exit.
        log = mocker.patch("fedora_messaging.twisted.consumer._std_log")
        queue = Mock()
        queue.get.side_effect = pika.exceptions.ChannelClosed(42, "testing")
        self.consumer._channel.queue_object = queue
        yield self.consumer.consume()

        yield self.consumer._read_loop
        self.callback.assert_not_called()
        log.exception.assert_called()
        logmsg = log.exception.call_args[0][0]
        assert "Consumer halted" in logmsg
        assert "the connection should restart" in logmsg
        # Temporary error, will restart
        assert self.consumer.result.called is False

    @pytest_twisted.inlineCallbacks
    def test_exit_loop_channel_forbidden(self):
        # Check the exceptions that cause the read loop to exit.
        queue = Mock()
        queue.get.side_effect = pika.exceptions.ChannelClosed(403, "nope!")
        self.consumer._channel.queue_object = queue
        self.consumer.consume()

        yield self.consumer._read_loop
        self.callback.assert_not_called()
        # Permanent error, no restart
        assert self.consumer._running is False
        assert self.consumer.result.called is True
        self.consumer.result.addErrback(lambda f: f.check(PermissionException))
        # The consumer should have been cancelled and the channel should have been closed
        self.consumer._channel.basic_cancel.assert_called_with(
            consumer_tag=self.consumer._tag
        )
        self.consumer._channel.close.assert_called_with()
        # Check the result's errback
        yield self.consumer.result

    @pytest_twisted.inlineCallbacks
    def test_exit_loop_consumer_cancelled(self):
        # Check the exceptions that cause the read loop to exit.
        queue = Mock()
        queue.get.side_effect = pika.exceptions.ConsumerCancelled()
        self.consumer._channel.queue_object = queue
        self.consumer.consume()

        yield self.consumer._read_loop
        self.callback.assert_not_called()
        # Permanent error, no restart
        assert self.consumer._running is False
        assert len(self.protocol._consumers) == 0
        assert self.consumer.result.called is True
        self.consumer.result.addErrback(lambda f: f.check(ConsumerCanceled))
        yield self.consumer.result

    @pytest_twisted.inlineCallbacks
    def test_exit_loop_amqp_error(self, mocker):
        # Check the exceptions that cause the read loop to exit.
        log = mocker.patch("fedora_messaging.twisted.consumer._std_log")
        queue = Mock()
        queue.get.side_effect = pika.exceptions.AMQPHeartbeatTimeout()
        self.consumer._channel.queue_object = queue
        self.consumer.consume()

        yield self.consumer._read_loop
        self.callback.assert_not_called()
        log.exception.assert_called()
        assert (
            "An unexpected AMQP error occurred; the connection should restart"
        ) in log.exception.call_args[0][0]
        # Temporary error, will restart
        assert self.consumer.result.called is False

    @pytest_twisted.inlineCallbacks
    def test_exit_loop_unknown_error(self):
        # Check the exceptions that cause the read loop to exit.
        queue = Mock()
        queue.get.side_effect = RuntimeError()
        self.consumer._channel.queue_object = queue
        self.consumer.consume()

        yield self.consumer._read_loop
        self.callback.assert_not_called()
        # Permanent error, no restart
        assert self.consumer._running is False
        assert self.consumer.result.called is True
        self.consumer.result.addErrback(lambda f: f.check(RuntimeError))
        # The consumer should have been cancelled and the channel should have been closed
        self.consumer._channel.basic_cancel.assert_called_with(
            consumer_tag=self.consumer._tag
        )
        self.consumer._channel.close.assert_called_with()
        # Check the result's errback
        yield self.consumer.result


@pytest.mark.parametrize("mock_class", [Mock, AsyncMock])
class TestConsumerCallback:
    """Unit tests for the Consumer class with a sync or async callback."""

    @pytest_twisted.inlineCallbacks
    def test_read(self, mock_class):
        # Check the nominal case for _read().
        callback = mock_class()
        consumer = _make_consumer_with_callback(callback)
        yield _call_read_one(consumer, "testing.topic", {}, {"key": "value"})
        callback.assert_called_once()
        consumer._channel.basic_ack.assert_called_once_with(delivery_tag="delivery_tag")

    @pytest_twisted.inlineCallbacks
    def test_nack(self, mock_class):
        # When the callback raises a Nack, the server should be notified.
        callback = mock_class(side_effect=Nack())
        consumer = _make_consumer_with_callback(callback)
        yield _call_read_one(consumer, "testing.topic", {}, {"key": "value"})
        callback.assert_called()
        consumer._channel.basic_nack.assert_called_with(
            delivery_tag="delivery_tag", requeue=True
        )

    @pytest_twisted.inlineCallbacks
    def test_drop(self, mock_class):
        # When the callback raises a Drop, the server should be notified.
        callback = mock_class(side_effect=Drop())
        consumer = _make_consumer_with_callback(callback)
        yield _call_read_one(consumer, "testing.topic", {}, {"key": "value"})
        callback.assert_called()
        consumer._channel.basic_nack.assert_called_with(
            delivery_tag="delivery_tag", requeue=False
        )

    @pytest.mark.parametrize("requeue", [False, True])
    @pytest_twisted.inlineCallbacks
    def test_halt(self, mock_class, requeue):
        """Assert the consumer is canceled when HaltConsumer is raised"""
        callback = mock_class(side_effect=HaltConsumer(requeue=requeue))
        consumer = _make_consumer_with_callback(callback)
        try:
            yield _call_read_one(consumer, "testing.topic", {}, {"key": "value"})
        except HaltConsumer:
            pass
        else:
            assert False, "This should have raised HaltConsumer"

        callback.assert_called()
        channel = consumer._channel
        if requeue:
            channel.basic_ack.assert_not_called()
            channel.basic_nack.assert_called_with(
                delivery_tag="delivery_tag", requeue=True
            )
        else:
            channel.basic_ack.assert_called_with(delivery_tag="delivery_tag")
            channel.basic_nack.assert_not_called()

    @pytest_twisted.inlineCallbacks
    def test_exception(self, mock_class):
        # On an unknown exception, the consumer should stop and all
        # unacknowledged messages should be requeued.
        callback = mock_class(side_effect=ValueError())
        consumer = _make_consumer_with_callback(callback)

        try:
            yield _call_read_one(consumer, "testing.topic", {}, {"key": "value"})
        except ValueError:
            pass
        else:
            assert False, "This should have raised ValueError"

        callback.assert_called()
        channel = consumer._channel
        channel.basic_nack.assert_called_with(
            delivery_tag=0, multiple=True, requeue=True
        )


class TestAddTimeout:
    """Unit tests for the _add_timeout helper function."""

    def test_twisted12_timeout(self):
        """Assert timeouts work for Twisted 12.2 (EL7)"""
        d = defer.Deferred()
        d.addTimeout = Mock(side_effect=AttributeError())
        _add_timeout(d, 0.1)

        d.addCallback(pytest.fail, "Expected errback to be called")

        def _check_failure(failure):
            assert isinstance(failure.value, defer.CancelledError)

        d.addErrback(_check_failure)

        return d

    def test_twisted12_cancel_cancel_callback(self):
        """Assert canceling the cancel call for Twisted 12.2 (EL7) works."""
        d = defer.Deferred()
        d.addTimeout = Mock(side_effect=AttributeError())
        d.cancel = Mock()
        with patch("fedora_messaging.twisted.consumer.reactor") as mock_reactor:
            _add_timeout(d, 1)
            delayed_cancel = mock_reactor.callLater.return_value
            d.callback(None)
            delayed_cancel.cancel.assert_called_once_with()
