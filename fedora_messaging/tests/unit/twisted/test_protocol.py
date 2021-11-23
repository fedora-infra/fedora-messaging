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
from fedora_messaging import config
from fedora_messaging.exceptions import (
    ConnectionException,
    Drop,
    HaltConsumer,
    Nack,
    NoFreeChannels,
    PublishForbidden,
    ValidationError,
)
from fedora_messaging.message import Message
from fedora_messaging.twisted.protocol import (
    ConsumerV2,
    FedoraMessagingProtocolV2,
    _add_timeout,
)

from twisted.internet import defer
from .utils import MockChannel

try:
    import pytest_twisted
except ImportError:
    pytest.skip("pytest-twisted is missing, skipping tests", allow_module_level=True)


class MockProtocol(FedoraMessagingProtocolV2):
    """A Protocol object that mocks the underlying channel and impl."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._impl = mock.Mock(name="_impl")
        self._impl.is_closed = True
        self._channel = MockChannel(name="_channel")
        self.channel = mock.Mock(
            name="channel", side_effect=lambda: defer.succeed(self._channel)
        )


class MockDeliveryFrame:
    def __init__(self, tag, routing_key=None):
        self.delivery_tag = tag
        self.routing_key = routing_key or "routing_key"


class MockProperties:
    def __init__(self, msgid=None):
        self.headers = {}
        self.content_encoding = None
        self.message_id = msgid or "msgid"


class ProtocolTests(TestCase):
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
        "fedora_messaging.twisted.consumer.uuid.uuid4", mock.Mock(return_value="tag1")
    )
    def test_consume_not_running(self):
        """Assert calling consume results in resumeProducing being invoked."""
        func = mock.Mock()

        def _check(consumer):
            consumer._channel.basic_qos.assert_called_with(
                prefetch_count=config.conf["qos"]["prefetch_count"],
                prefetch_size=config.conf["qos"]["prefetch_size"],
            )
            consumer._channel.basic_consume.assert_called_once_with(
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
        "fedora_messaging.twisted.consumer.uuid.uuid4", mock.Mock(return_value="tag1")
    )
    def test_consume_running(self):
        """Assert when running, consume sets up the AMQP consumer"""
        func = mock.Mock()

        def _check(consumer):
            consumer._channel.basic_qos.assert_called_with(
                prefetch_count=config.conf["qos"]["prefetch_count"],
                prefetch_size=config.conf["qos"]["prefetch_size"],
            )
            consumer._channel.basic_consume.assert_called_once_with(
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
        d.addCallback(lambda consumer: consumer.cancel())
        d.addCallback(_check)

        return pytest_twisted.blockon(d)

    def test_forget_no_consumer(self):
        """Assert forgetting a non-existent consumer just returns None."""
        result = self.protocol._forget_consumer("my_invalid_queue")
        self.assertIsNone(result)

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

    def test_publish_forbidden(self):
        # Check the publish method when publishing is forbidden.
        body = {"bodykey": "bodyvalue"}
        headers = {"headerkey": "headervalue"}
        message = Message(body, headers, "testing.topic")
        self.protocol._channel.basic_publish.side_effect = (
            pika.exceptions.ChannelClosed(403, "Test forbidden message")
        )
        d = self.protocol.publish(message, "test-exchange")

        d.addCallback(pytest.fail, "Expected a PublishForbidden exception")
        d.addErrback(lambda f: f.trap(PublishForbidden))

        def _check(_):
            # Make sure the channel will be re-allocated
            assert self.protocol._publish_channel is None

        d.addBoth(_check)

        return pytest_twisted.blockon(d)

    def test_publish_access_denied(self):
        body = {"bodykey": "bodyvalue"}
        headers = {"headerkey": "headervalue"}
        message = Message(body, headers, "testing.topic")
        self.protocol._channel.basic_publish.side_effect = (
            pika.exceptions.ProbableAccessDeniedError(403, "Test access denied message")
        )
        d = self.protocol.publish(message, "test-exchange")

        d.addCallback(pytest.fail, "Expected a PublishForbidden exception")
        d.addErrback(lambda f: f.trap(PublishForbidden))

        return pytest_twisted.blockon(d)


class ProtocolReadTests(TestCase):
    """Test the _read() method on the Protocol."""

    def setUp(self):
        self.protocol = MockProtocol(None)
        self.protocol._impl.is_closed = False
        self.queue = mock.Mock()
        self.callback = mock.Mock()
        self.consumer = ConsumerV2("my_queue_name", self.callback)
        self.consumer._channel = mock.Mock()

    def _queue_contents(self, values):
        for value in values:
            yield value
        self.consumer._running = False
        yield defer.CancelledError()

    def test_read_not_running(self):
        # When not running, _read() should do nothing.
        self.consumer._running = False
        self.queue.get.side_effect = lambda: defer.succeed(None)
        d = self.protocol._read(self.queue, self.consumer)

        def _check(_):
            self.queue.get.assert_not_called()

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    @mock.patch("fedora_messaging.twisted.protocol.get_message")
    def test_read(self, get_message):
        # Check the nominal case for _read().
        message = mock.Mock(name="message")
        get_message.return_value = message
        props = MockProperties()
        self.queue.get.side_effect = self._queue_contents(
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

        d = self.protocol._read(self.queue, self.consumer)
        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    @mock.patch(
        "fedora_messaging.twisted.protocol.get_message",
        mock.Mock(side_effect=ValidationError),
    )
    def test_read_no_body(self):
        # When a message has no body, it should not be passed to the callback.
        df = MockDeliveryFrame("df")
        prop = MockProperties()
        self.queue.get.side_effect = self._queue_contents(
            [
                defer.succeed((self.consumer._channel, df, prop, "")),
                defer.succeed((self.consumer._channel, df, prop, "weird")),
            ]
        )
        d = self.protocol._read(self.queue, self.consumer)

        def _check(_):
            self.callback.assert_not_called()
            self.consumer._channel.basic_nack.assert_called_with(
                delivery_tag="df", requeue=False
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    # TODO: re-enable when the read loop is part of the consumer
    # def test_read_exit_loop(self):
    #     # Check the exceptions that cause the read loop to exit.
    #     exceptions = [
    #         error.ConnectionDone(),
    #         pika.exceptions.ChannelClosed(42, "testing"),
    #         pika.exceptions.ConsumerCancelled(),
    #         RuntimeError(),
    #         pika.exceptions.ChannelClosedByClient(42, "testing"),
    #     ]

    #     deferreds = []
    #     for exc in exceptions:
    #         self.queue.get.side_effect = exc
    #         deferreds.append(self.protocol._read(self.queue, self.consumer))

    #     def _check(_):
    #         self.protocol._read_one.assert_not_called()

    #     d = defer.DeferredList(deferreds)
    #     d.addCallback(_check)
    #     return pytest_twisted.blockon(d)


class ProtocolReadOneTests(TestCase):
    """Test the _read_one() method on the Protocol."""

    def setUp(self):
        self.protocol = MockProtocol(None)
        self._message_callback = mock.Mock()
        self.protocol._impl.is_closed = False
        consumer = self.protocol._consumers["my_queue_name"] = ConsumerV2(
            "my_queue_name", self._message_callback
        )
        consumer._channel = mock.Mock()

    def _call_read_one(self, topic, headers, body):
        """Prepare arguments for the _read_one() method and call it."""
        consumer = self.protocol._consumers["my_queue_name"]
        full_headers = {"fedora_messaging_schema": "fedora_messaging.message:Message"}
        full_headers.update(headers)
        queue = mock.Mock()
        queue.get.return_value = defer.succeed(
            (
                consumer._channel,
                pika.spec.Basic.Deliver(routing_key=topic, delivery_tag="delivery_tag"),
                pika.spec.BasicProperties(headers=full_headers),
                json.dumps(body).encode("utf-8"),
            )
        )
        return self.protocol._read_one(queue, consumer)

    def test_read_one(self):
        # Check the nominal case.
        channel = self.protocol._consumers["my_queue_name"]._channel
        d = self._call_read_one("testing.topic", {}, {"key": "value"})

        def _check(_):
            self._message_callback.assert_called()
            channel.basic_ack.assert_called_with(delivery_tag="delivery_tag")

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_read_one_invalid(self):
        # When a message is invalid, it should be Nacked.
        channel = self.protocol._consumers["my_queue_name"]._channel
        d = self._call_read_one("testing.topic", {}, "testing")

        def _check(_):
            self._message_callback.assert_not_called()
            channel.basic_nack.assert_called_with(
                delivery_tag="delivery_tag", requeue=False
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_read_one_nack(self):
        # When the callback raises a Nack, the server should be notified.
        self._message_callback.side_effect = Nack()
        channel = self.protocol._consumers["my_queue_name"]._channel
        d = self._call_read_one("testing.topic", {}, {"key": "value"})

        def _check(_):
            self._message_callback.assert_called()
            channel.basic_nack.assert_called_with(
                delivery_tag="delivery_tag", requeue=True
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_read_one_drop(self):
        # When the callback raises a Drop, the server should be notified.
        self._message_callback.side_effect = Drop()
        channel = self.protocol._consumers["my_queue_name"]._channel
        d = self._call_read_one("testing.topic", {}, {"key": "value"})

        def _check(_):
            self._message_callback.assert_called()
            channel.basic_nack.assert_called_with(
                delivery_tag="delivery_tag", requeue=False
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_read_one_halt(self):
        """Assert the consumer is canceled when HaltConsumer is raised"""
        self._message_callback.side_effect = HaltConsumer()
        channel = self.protocol._consumers["my_queue_name"]._channel
        tag = self.protocol._consumers["my_queue_name"]._tag
        d = self._call_read_one("testing.topic", {}, {"key": "value"})

        def _check(_):
            self._message_callback.assert_called()
            channel.basic_ack.assert_called_with(delivery_tag="delivery_tag")
            channel.basic_cancel.assert_called_with(consumer_tag=tag)
            channel.close.assert_called_once_with()

        d.addCallback(_check)
        d.addErrback(lambda failure: self.assertIsInstance(failure.value, HaltConsumer))
        return pytest_twisted.blockon(d)

    def test_read_one_exception(self):
        # On an unknown exception, the consumer should stop and all
        # unacknowledged messages should be requeued.
        self._message_callback.side_effect = ValueError()
        channel = self.protocol._consumers["my_queue_name"]._channel
        d = self._call_read_one("testing.topic", {}, {"key": "value"})

        def _check(_):
            self._message_callback.assert_called()
            channel.basic_nack.assert_called_with(
                delivery_tag=0, multiple=True, requeue=True
            )
            channel.close.assert_called_once_with()

        d.addCallback(_check)
        d.addErrback(lambda failure: self.assertIsInstance(failure.value, ValueError))
        return pytest_twisted.blockon(d)


class ProtocolV2Tests(TestCase):
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

    def test_consume_existing_consumer(self):
        """Consuming should re-use an existing consumer if possible."""
        proto = FedoraMessagingProtocolV2(None)
        proto._allocate_channel = mock.Mock()

        new_callback = mock.Mock(name="new_callback")

        # Prepare the existing consumer
        existing_callback = mock.Mock(name="existing_callback")
        consumer = ConsumerV2(queue="test_queue", callback=existing_callback)
        proto._consumers["test_queue"] = consumer

        def check(result):
            self.assertEqual(consumer, result)
            self.assertEqual(consumer.callback, new_callback)
            proto._allocate_channel.assert_not_called()

        d = proto.consume(new_callback, "test_queue", consumer)
        d.addBoth(check)
        return pytest_twisted.blockon(d)

    def test_consume_provided_consumer(self):
        """The consume() method must handle being passed a consumer."""
        proto = FedoraMessagingProtocolV2(None)
        mock_channel = mock.Mock(name="mock_channel")
        deferred_channel = defer.succeed(mock_channel)
        proto._allocate_channel = mock.Mock(return_value=deferred_channel)
        # Don't go into the read loop
        proto._read = mock.Mock(retrun_value=defer.succeed(None))
        # basic_consume() must return a tuple
        mock_channel.basic_consume.return_value = (mock.Mock(), mock.Mock())
        # Prepare the consumer
        callback = mock.Mock()
        consumer = ConsumerV2(queue="queue_orig", callback=callback)

        def check(result):
            self.assertEqual(consumer, result)
            self.assertEqual(consumer.queue, "queue_new")
            self.assertEqual(consumer._protocol, proto)
            self.assertEqual(consumer._channel, mock_channel)

        d = proto.consume(callback, "queue_new", consumer)
        d.addBoth(check)
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
        with mock.patch("fedora_messaging.twisted.protocol.reactor") as mock_reactor:
            _add_timeout(d, 1)
            delayed_cancel = mock_reactor.callLater.return_value
            d.callback(None)
            delayed_cancel.cancel.assert_called_once_with()
