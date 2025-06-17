# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

import json
from typing import Any
from unittest import mock

import pika
import pika.exceptions
import pytest
from twisted.internet import defer

from fedora_messaging import config
from fedora_messaging.exceptions import (
    BadDeclaration,
    ConnectionException,
    NoFreeChannels,
    PublishForbidden,
    PublishReturned,
)
from fedora_messaging.message import HeaderType, INFO, Message
from fedora_messaging.twisted.consumer import Consumer
from fedora_messaging.twisted.protocol import BindingArgument, FedoraMessagingProtocolV2

from .utils import MockProtocol


try:
    import pytest_twisted
except ImportError:
    pytest.skip("pytest-twisted is missing, skipping tests", allow_module_level=True)


def fail(result_: Any, reason: str = "") -> None:
    pytest.fail(reason)


class TestProtocol:
    def setup_method(self, method):
        self.protocol = MockProtocol(None)

    def test_allocate_channel_no_more_channels(self):
        """Assert a pika NoFreeChannels exception turns into a fedora_messaging exception."""
        self.protocol.channel = mock.Mock(
            name="channel", side_effect=pika.exceptions.NoFreeChannels()
        )

        d = self.protocol._allocate_channel()
        d.addCallback(fail, "Expected a NoFreeChannels exception")
        d.addErrback(lambda f: f.trap(NoFreeChannels))
        return d

    def test_allocate_channel_connection_exception(self):
        """
        Assert a pika ConnectionWrongStateError exception turns into a fedora_messaging exception.
        """
        self.protocol.channel = mock.Mock(
            name="channel", side_effect=pika.exceptions.ConnectionWrongStateError()
        )

        d = self.protocol._allocate_channel()
        d.addCallback(fail, "Expected a ConnectionException exception")
        d.addErrback(lambda f: f.trap(ConnectionException))
        return d

    @mock.patch("fedora_messaging.twisted.consumer.uuid.uuid4", mock.Mock(return_value="tag1"))
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

        return d

    def test_consume_twice(self):
        """Assert calling consume on the same queue updates the callback."""

        def cb1(message: Message):
            pass

        def cb2(message: Message):
            pass

        def _check(_):
            assert 1 == len(self.protocol._consumers)
            assert cb2 == self.protocol._consumers["my_queue"].callback

        d = self.protocol.consume(cb1, "my_queue")
        d.addCallback(lambda _: self.protocol.consume(cb2, "my_queue"))
        d.addCallback(_check)

        return d

    @mock.patch("fedora_messaging.twisted.consumer.uuid.uuid4", mock.Mock(return_value="tag1"))
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

        return d

    def test_cancel(self):
        """Assert a consumer is removed from the consumer list and canceled."""

        def cb(message: Message):
            pass

        def _check(_):
            assert 1 == len(self.protocol._consumers)

        d = self.protocol.consume(cb, "my_queue")
        d.addCallback(lambda _: self.protocol.consume(cb, "my_queue2"))
        d.addCallback(lambda consumer: consumer.cancel())
        d.addCallback(_check)

        return d

    def test_forget_no_consumer(self):
        """Assert forgetting a non-existent consumer does nothing."""
        self.protocol._forget_consumer("my_invalid_queue")

    def test_forget_consumer_none(self):
        """Assert forget_consumer does not crash if passed None."""
        self.protocol._forget_consumer(None)

    def test_forget_consumer_no_factory(self):
        """Assert forget_consumer does not crash if there is not factory."""
        self.protocol.factory = None
        result = self.protocol._forget_consumer("queue-name")
        assert result is None

    def test_connection_ready(self):
        # Check the ready Deferred.
        def _check(_):
            self.protocol.channel.assert_called_once_with()

        d = self.protocol.ready
        d.addCallback(_check)

        # missing from types-pika
        self.protocol._on_connection_ready(None)  # type: ignore

        return d

    def test_publish(self):
        # Check the publish method.
        body = {"bodykey": "bodyvalue"}
        headers: HeaderType = {"headerkey": "headervalue"}
        message = Message(body, headers, "testing.topic")
        d = self.protocol.publish(message, "test-exchange")

        def _check(_):
            self.protocol._channel.basic_publish.assert_called_once()
            args = self.protocol._channel.basic_publish.call_args_list[0][1]
            assert args["exchange"] == "test-exchange"
            assert args["routing_key"] == "testing.topic"
            assert args["body"] == json.dumps(body).encode("utf-8")
            props = args["properties"]
            assert props.headers["fedora_messaging_schema"] == "base.message"
            assert props.headers["fedora_messaging_severity"] == INFO
            assert props.headers["headerkey"] == "headervalue"
            assert "sent-at" in props.headers
            assert props.content_encoding == "utf-8"
            assert props.content_type == "application/json"
            assert props.delivery_mode == 2

        d.addCallback(_check)
        return d

    def test_publish_forbidden(self):
        # Check the publish method when publishing is forbidden.
        body = {"bodykey": "bodyvalue"}
        headers: HeaderType = {"headerkey": "headervalue"}
        message = Message(body, headers, "testing.topic")
        self.protocol._channel.basic_publish.side_effect = pika.exceptions.ChannelClosed(
            403, "Test forbidden message"
        )
        d = self.protocol.publish(message, "test-exchange")

        d.addCallback(fail, "Expected a PublishForbidden exception")
        d.addErrback(lambda f: f.trap(PublishForbidden))

        def _check(_):
            # Make sure the channel will be re-allocated
            assert self.protocol._publish_channel is None

        d.addBoth(_check)

        return d

    def test_publish_access_denied(self):
        body = {"bodykey": "bodyvalue"}
        headers: HeaderType = {"headerkey": "headervalue"}
        message = Message(body, headers, "testing.topic")
        self.protocol._channel.basic_publish.side_effect = (
            pika.exceptions.ProbableAccessDeniedError(403, "Test access denied message")
        )
        d = self.protocol.publish(message, "test-exchange")

        d.addCallback(fail, "Expected a PublishForbidden exception")
        d.addErrback(lambda f: f.trap(PublishForbidden))

        return d

    def test_publish_returned(self):
        body = {"bodykey": "bodyvalue"}
        headers: HeaderType = {"headerkey": "headervalue"}
        message = Message(body, headers, "testing.topic")
        self.protocol._channel.basic_publish.side_effect = pika.exceptions.NackError([])
        d = self.protocol.publish(message, "test-exchange")

        d.addCallback(fail, "Expected a PublishReturned exception")
        d.addErrback(lambda f: f.trap(PublishReturned))

        return d

    def test_publish_connection_closed(self):
        body = {"bodykey": "bodyvalue"}
        headers: HeaderType = {"headerkey": "headervalue"}
        message = Message(body, headers, "testing.topic")
        self.protocol._channel.basic_publish.side_effect = pika.exceptions.ConnectionClosed(
            42, "testing"
        )
        d = self.protocol.publish(message, "test-exchange")

        d.addCallback(fail, "Expected a ConnectionException exception")
        d.addErrback(lambda f: f.trap(ConnectionException))

        return d

    def test_consume_connection_exception(self):
        """If consuming fails due to a non-permission error, a ConnectionException happens."""
        proto = FedoraMessagingProtocolV2(pika.ConnectionParameters())
        mock_channel = mock.Mock()
        mock_channel.basic_consume.side_effect = pika.exceptions.ChannelClosed(400, "Bad Request!")
        deferred_channel = defer.succeed(mock_channel)
        proto._allocate_channel = mock.Mock(return_value=deferred_channel)

        def check(failure):
            assert isinstance(failure.value, ConnectionException)

        d = proto.consume(lambda x: None, "test_queue")
        d.addBoth(check)
        return d

    def test_consume_existing_consumer(self):
        """Consuming should re-use an existing consumer if possible."""
        proto = FedoraMessagingProtocolV2(pika.ConnectionParameters())
        proto._allocate_channel = mock.Mock()

        new_callback = mock.Mock(name="new_callback")

        # Prepare the existing consumer
        existing_callback = mock.Mock(name="existing_callback")
        consumer = Consumer(queue="test_queue", callback=existing_callback)
        proto._consumers["test_queue"] = consumer

        def check(result):
            assert consumer == result
            assert consumer.callback == new_callback
            proto._allocate_channel.assert_not_called()  # pyright: ignore

        d = proto.consume(new_callback, "test_queue", consumer)
        d.addBoth(check)
        return d

    def test_consume_provided_consumer(self):
        """The consume() method must handle being passed a consumer."""
        proto = FedoraMessagingProtocolV2(pika.ConnectionParameters())
        mock_channel = mock.Mock(name="mock_channel")
        deferred_channel = defer.succeed(mock_channel)
        proto._allocate_channel = mock.Mock(return_value=deferred_channel)
        # basic_consume() must return a tuple
        mock_channel.basic_consume.return_value = (mock.Mock(), mock.Mock())
        # Prepare the consumer
        callback = mock.Mock()
        consumer = Consumer(queue="queue_orig", callback=callback)
        # Don't go into the read loop
        consumer._read = mock.Mock(return_value=defer.succeed(None))

        def check(result):
            assert consumer == result
            assert consumer.queue == "queue_new"
            assert consumer._protocol == proto
            assert consumer._channel == mock_channel

        d = proto.consume(callback, "queue_new", consumer)
        d.addBoth(check)
        return d

    @pytest_twisted.inlineCallbacks
    def test_declare_exchanges(self):
        exchanges: list[config.ExchangeDefinition] = [
            {
                "exchange": "amq.topic",
                "durable": False,
                "auto_delete": False,
                "exchange_type": "topic",
                "arguments": {},
            }
        ]
        yield self.protocol.declare_exchanges(exchanges)
        self.protocol.channel.assert_called_once_with()
        self.protocol._channel.exchange_declare.assert_called_once_with(
            **{"passive": False, **exchanges[0]}
        )
        self.protocol._channel.close.assert_called_once_with()

    @pytest_twisted.inlineCallbacks
    def test_declare_exchanges_failure(self):
        exchanges: list[config.ExchangeDefinition] = [
            {
                "exchange": "amq.topic",
                "durable": False,
                "auto_delete": False,
                "exchange_type": "topic",
                "arguments": {},
            }
        ]
        self.protocol._channel.exchange_declare.side_effect = pika.exceptions.ChannelClosed(
            400, "Bad Request!"
        )
        self.protocol._channel.close.side_effect = pika.exceptions.ChannelClosed(
            400, "Already closed"
        )
        with pytest.raises(BadDeclaration):
            yield self.protocol.declare_exchanges(exchanges)
        self.protocol._channel.exchange_declare.assert_called_once()
        self.protocol._channel.close.assert_called_once_with()

    @pytest_twisted.inlineCallbacks
    def test_declare_queue(self):
        queue: config.NamedQueueConfig = {
            "queue": "queue-name",
            "durable": False,
            "exclusive": False,
            "auto_delete": False,
            "arguments": {},
        }
        declared_queue = yield self.protocol.declare_queue(queue)
        self.protocol.channel.assert_called_once_with()
        self.protocol._channel.queue_declare.assert_called_once_with(**{"passive": False, **queue})
        self.protocol._channel.close.assert_called_once_with()
        assert declared_queue == "queue-name"

    @pytest_twisted.inlineCallbacks
    def test_declare_queue_failure(self):
        queue: config.NamedQueueConfig = {
            "queue": "queue-name",
            "durable": False,
            "exclusive": False,
            "auto_delete": False,
            "arguments": {},
        }
        self.protocol._channel.queue_declare.side_effect = pika.exceptions.ChannelClosed(
            400, "Bad Request!"
        )
        self.protocol._channel.close.side_effect = pika.exceptions.ChannelClosed(
            400, "Already closed"
        )
        with pytest.raises(BadDeclaration):
            yield self.protocol.declare_queue(queue)
        self.protocol._channel.queue_declare.assert_called_once()
        self.protocol._channel.close.assert_called_once_with()

    @pytest_twisted.inlineCallbacks
    def test_bind_queues(self):
        binding: BindingArgument = {
            "queue": "queue-name",
            "exchange": "amq.topic",
            "routing_key": "routing.key",
        }
        yield self.protocol.bind_queues([binding])
        self.protocol.channel.assert_called_once_with()
        self.protocol._channel.queue_bind.assert_called_once_with(**binding)
        self.protocol._channel.close.assert_called_once_with()

    @pytest_twisted.inlineCallbacks
    def test_bind_queues_failure(self):
        binding: BindingArgument = {
            "queue": "queue-name",
            "exchange": "amq.topic",
            "routing_key": "routing.key",
        }
        self.protocol._channel.queue_bind.side_effect = pika.exceptions.ChannelClosed(
            400, "Bad Request!"
        )
        self.protocol._channel.close.side_effect = pika.exceptions.ChannelClosed(
            400, "Already closed"
        )
        with pytest.raises(BadDeclaration):
            yield self.protocol.bind_queues([binding])
        self.protocol._channel.queue_bind.assert_called_once()
        self.protocol._channel.close.assert_called_once_with()
