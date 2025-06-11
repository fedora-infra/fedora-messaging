# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

import json
from typing import cast
from unittest.mock import AsyncMock, Mock, patch

import pika
import pika.exceptions
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
from fedora_messaging.twisted.consumer import Consumer

from .utils import MockChannel, MockProtocol


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
            pika.spec.Basic.Deliver(routing_key=topic, delivery_tag=42),
            pika.spec.BasicProperties(headers=full_headers),
            json.dumps(body).encode("utf-8"),
        )
    )
    return consumer._read_one(queue)


def _make_consumer_with_callback(callback):
    protocol = MockProtocol(None)
    protocol._impl.is_closed = False
    consumer = Consumer("my_queue_name", callback)
    consumer._protocol = protocol
    consumer._channel = cast(MockChannel, protocol._channel)
    protocol._consumers["my_queue_name"] = consumer
    return consumer


class TestConsumer:
    """Unit tests for the Consumer class."""

    def setup_method(self):
        self.callback = Mock()
        self.consumer = _make_consumer_with_callback(self.callback)
        self.consumer._protocol = cast(MockProtocol, self.consumer._protocol)
        self.consumer._channel = cast(MockChannel, self.consumer._channel)
        self.protocol = self.consumer._protocol
        self.channel = self.consumer._channel

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
        self.channel.basic_cancel.side_effect = pika.exceptions.AMQPChannelError()
        yield self.consumer.cancel()
        assert len(self.protocol._consumers) == 0
        # consumer._protocol._forget_consumer.assert_called_with("my_queue")
        self.channel.basic_cancel.assert_called_once_with(consumer_tag=self.consumer._tag)

    @pytest_twisted.inlineCallbacks
    def test_cancel_no_protocol(self):
        """The cancel method must work when no protocol or no channel has been set."""
        consumer = Consumer("my_queue_name", self.callback)
        # This must not raise an exception
        yield consumer.cancel()

    # Init errors

    @pytest_twisted.inlineCallbacks
    def test_no_channel(self):
        """Assert the channel is set before we consume."""
        consumer = Consumer("my_queue_name", Mock())
        with pytest.raises(RuntimeError):
            yield consumer.consume()

    @pytest_twisted.inlineCallbacks
    def test_no_queue(self):
        """Assert the queue is set before we consume."""
        consumer = Consumer(callback=Mock())
        consumer._channel = MockChannel()
        with pytest.raises(RuntimeError):
            yield consumer.consume()

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
                        self.channel,
                        MockDeliveryFrame("dt1", "rk1"),
                        props,
                        "body1",
                    )
                ),
                defer.succeed(
                    (
                        self.channel,
                        MockDeliveryFrame("dt2", "rk2"),
                        props,
                        "body2",
                    )
                ),
                defer.succeed(
                    (
                        self.channel,
                        MockDeliveryFrame("dt3", "rk3"),
                        props,
                        "body3",
                    )
                ),
            ],
        )

        self.consumer._running = True
        assert self.consumer.running is True

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
        assert self.channel.basic_ack.call_args_list == [
            (tuple(), dict(delivery_tag="dt1")),
            (tuple(), dict(delivery_tag="dt2")),
            (tuple(), dict(delivery_tag="dt3")),
        ]
        assert self.consumer.stats.received == 3
        assert self.consumer.stats.processed == 3

    @pytest_twisted.inlineCallbacks
    def test_read_not_running(self):
        # When not running, _read() should do nothing.
        self.consumer._running = False
        assert self.consumer.running is False
        queue = Mock()
        queue.get.side_effect = lambda: defer.succeed(None)
        yield self.consumer._read(queue)
        queue.get.assert_not_called()

    @pytest_twisted.inlineCallbacks
    def test_message_invalid(self):
        # When a message is invalid, it should be Nacked.
        yield _call_read_one(self.consumer, "testing.topic", {}, "invalid-json-body")
        self.callback.assert_not_called()
        self.channel.basic_nack.assert_called_with(delivery_tag=42, requeue=False)
        assert self.consumer.stats.received == 0

    @pytest.mark.parametrize("error_class", [HaltConsumer, ValueError])
    @pytest_twisted.inlineCallbacks
    def test_read_exception(self, mocker, error_class):
        # Check that some exceptions from the callback cancel the consumer.
        self.callback.side_effect = error_class()
        message = Mock(name="message")
        mocker.patch("fedora_messaging.twisted.consumer.get_message", return_value=message)
        props = MockProperties()
        yield self.channel.queue_object.put(
            (
                self.channel,
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
            pytest.fail(f"This should have raised {error_class}")

        self.consumer.cancel.assert_called_once_with()
        assert self.consumer.stats.received == 1
        assert self.consumer.stats.rejected == 0
        assert self.consumer.stats.dropped == 0
        if error_class == HaltConsumer:
            self.channel.basic_ack.assert_called_once_with(delivery_tag="dt1")
            assert self.consumer.stats.processed == 1
            assert self.consumer.stats.failed == 0
        else:
            self.channel.basic_nack.assert_called_once_with(
                delivery_tag=0, multiple=True, requeue=True
            )
            assert self.consumer.stats.failed == 1
            assert self.consumer.stats.processed == 0

    @pytest_twisted.inlineCallbacks
    def test_consume_no_protocol(self):
        """The consume method must raise when no protocol has been set."""
        consumer = Consumer("my_queue_name", self.callback)
        # Set the channel but not the protocol
        consumer._channel = MockChannel()
        with patch.object(consumer, "_read_one", side_effect=pika.exceptions.ConsumerCancelled()):
            yield consumer.consume()
        assert consumer._running is False
        with pytest.raises(ConsumerCanceled):
            yield consumer.result

    # Handling read errors

    @pytest_twisted.inlineCallbacks
    def test_consume_channel_closed(self):
        # Check consuming when the channel is closed
        self.channel.basic_consume.side_effect = pika.exceptions.ChannelClosed(42, "testing")
        self.consumer._read = Mock()

        try:
            yield self.consumer.consume()
        except ConnectionException:
            assert self.consumer._read_loop.result is None
            self.consumer._read.assert_not_called()
        else:
            pytest.fail("This should have raised ConnectionException")

    @pytest_twisted.inlineCallbacks
    def test_consume_channel_forbidden(self):
        # Check consuming when the channel is forbidden
        self.channel.basic_consume.side_effect = pika.exceptions.ChannelClosed(403, "testing")
        self.consumer._read = Mock()

        try:
            yield self.consumer.consume()
        except PermissionException:
            assert self.consumer._read_loop.result is None
            self.consumer._read.assert_not_called()
        else:
            pytest.fail("This should have raised PermissionException")

    @pytest_twisted.inlineCallbacks
    def test_exit_loop_connection_done(self, mocker):
        # Check the exceptions that cause the read loop to exit.
        log = mocker.patch("fedora_messaging.twisted.consumer._std_log")
        queue = Mock()
        queue.get.side_effect = error.ConnectionDone()
        self.channel.queue_object = queue
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
        self.channel.queue_object = queue
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
        self.channel.queue_object = queue
        self.consumer.consume()

        yield self.consumer._read_loop
        self.callback.assert_not_called()
        # Permanent error, no restart
        assert self.consumer._running is False
        assert self.consumer.result.called is True
        self.consumer.result.addErrback(lambda f: f.check(PermissionException))
        # The consumer should have been cancelled and the channel should have been closed
        self.channel.basic_cancel.assert_called_with(consumer_tag=self.consumer._tag)
        self.channel.close.assert_called_with()
        # Check the result's errback
        yield self.consumer.result

    @pytest_twisted.inlineCallbacks
    def test_exit_loop_consumer_cancelled(self):
        # Check the exceptions that cause the read loop to exit.
        queue = Mock()
        queue.get.side_effect = pika.exceptions.ConsumerCancelled()
        self.channel.queue_object = queue
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
        self.channel.queue_object = queue
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
        self.channel.queue_object = queue
        self.consumer.consume()

        yield self.consumer._read_loop
        self.callback.assert_not_called()
        # Permanent error, no restart
        assert self.consumer._running is False
        assert self.consumer.result.called is True
        self.consumer.result.addErrback(lambda f: f.check(RuntimeError))
        # The consumer should have been cancelled and the channel should have been closed
        self.channel.basic_cancel.assert_called_with(consumer_tag=self.consumer._tag)
        self.channel.close.assert_called_with()
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
        consumer._channel = cast(MockChannel, consumer._channel)
        consumer._channel.basic_ack.assert_called_once_with(delivery_tag=42)
        assert consumer.stats.received == 1
        assert consumer.stats.processed == 1

    @pytest_twisted.inlineCallbacks
    def test_nack(self, mock_class):
        # When the callback raises a Nack, the server should be notified.
        callback = mock_class(side_effect=Nack())
        consumer = _make_consumer_with_callback(callback)
        yield _call_read_one(consumer, "testing.topic", {}, {"key": "value"})
        callback.assert_called()
        consumer._channel = cast(MockChannel, consumer._channel)
        consumer._channel.basic_nack.assert_called_with(delivery_tag=42, requeue=True)
        assert consumer.stats.received == 1
        assert consumer.stats.rejected == 1

    @pytest_twisted.inlineCallbacks
    def test_drop(self, mock_class):
        # When the callback raises a Drop, the server should be notified.
        callback = mock_class(side_effect=Drop())
        consumer = _make_consumer_with_callback(callback)
        yield _call_read_one(consumer, "testing.topic", {}, {"key": "value"})
        callback.assert_called()
        consumer._channel = cast(MockChannel, consumer._channel)
        consumer._channel.basic_nack.assert_called_with(delivery_tag=42, requeue=False)
        assert consumer.stats.received == 1
        assert consumer.stats.dropped == 1

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
            pytest.fail("This should have raised HaltConsumer")

        callback.assert_called()
        consumer._channel = cast(MockChannel, consumer._channel)
        channel = consumer._channel
        assert consumer.stats.received == 1
        if requeue:
            channel.basic_ack.assert_not_called()
            channel.basic_nack.assert_called_with(delivery_tag=42, requeue=True)
            assert consumer.stats.rejected == 1
        else:
            channel.basic_ack.assert_called_with(delivery_tag=42)
            channel.basic_nack.assert_not_called()
            assert consumer.stats.processed == 1

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
            pytest.fail("This should have raised ValueError")

        callback.assert_called()
        consumer._channel = cast(MockChannel, consumer._channel)
        channel = consumer._channel
        channel.basic_nack.assert_called_with(delivery_tag=0, multiple=True, requeue=True)
        assert consumer.stats.received == 1
        assert consumer.stats.failed == 1
