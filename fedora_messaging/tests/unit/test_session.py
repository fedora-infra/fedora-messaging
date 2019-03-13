# coding: utf-8

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

import os
import ssl
import unittest
import signal

import mock
import pkg_resources
from pika import exceptions as pika_errs, URLParameters, credentials
from jsonschema.exceptions import ValidationError as JSONValidationError

from fedora_messaging import _session, config, message
from fedora_messaging.exceptions import (
    PublishReturned,
    ConnectionException,
    Nack,
    Drop,
    HaltConsumer,
    ConfigurationException,
)
from fedora_messaging.tests import FIXTURES_DIR


class PublisherSessionTests(unittest.TestCase):
    def setUp(self):
        self.publisher = _session.PublisherSession()
        self.publisher._connection = mock.Mock()
        self.publisher._channel = mock.Mock()
        if _session._pika_version < pkg_resources.parse_version("1.0.0b1"):
            self.publisher_channel_publish = self.publisher._channel.publish
        else:
            self.publisher_channel_publish = self.publisher._channel.basic_publish
        self.message = mock.Mock()
        self.message._headers = {}
        self.message.topic = "test.topic"
        self.message._encoded_routing_key = b"test.topic"
        self.message.body = "test body"
        self.message._encoded_body = b'"test body"'
        self.tls_conf = {
            "keyfile": None,
            "certfile": None,
            "ca_cert": os.path.join(FIXTURES_DIR, "ca_bundle.pem"),
        }

    def test_publisher_init(self):
        with mock.patch.dict(config.conf, {"tls": self.tls_conf}):
            publisher = _session.PublisherSession()
        self.assertEqual(publisher._parameters.host, "localhost")
        self.assertEqual(publisher._parameters.port, 5672)
        self.assertEqual(publisher._parameters.virtual_host, "/")
        self.assertIsNone(publisher._parameters.ssl_options)

    def test_publish_init_custom_url(self):
        """Assert a custom URL can be provided to the publisher session."""
        with mock.patch.dict(config.conf, {"tls": self.tls_conf}):
            publisher = _session.PublisherSession(
                "amqps://username:password@rabbit.example.com/vhost"
            )
        self.assertEqual(publisher._parameters.host, "rabbit.example.com")
        self.assertEqual(publisher._parameters.port, 5671)
        self.assertEqual(publisher._parameters.virtual_host, "vhost")
        self.assertIsNotNone(publisher._parameters.ssl_options)

    def test_publish_init_custom_url_with_client_params(self):
        """Assert a custom URL with client props can be provided to the publisher session."""
        with mock.patch.dict(config.conf, {"tls": self.tls_conf}):
            publisher = _session.PublisherSession(
                "amqps://username:password@rabbit.example.com/vhost?client_properties={'k':'v'}"
            )
        self.assertEqual(publisher._parameters.host, "rabbit.example.com")
        self.assertEqual(publisher._parameters.port, 5671)
        self.assertEqual(publisher._parameters.virtual_host, "vhost")
        self.assertEqual(publisher._parameters.client_properties, {"k": "v"})
        self.assertIsNotNone(publisher._parameters.ssl_options)

    def test_plain_auth(self):
        """Assert when there's no key or certfile, plain authentication is used"""
        with mock.patch.dict(config.conf, {"tls": self.tls_conf}):
            publisher = _session.PublisherSession(
                "amqps://username:password@rabbit.example.com/vhost"
            )
        self.assertIsInstance(
            publisher._parameters.credentials, credentials.PlainCredentials
        )

    def test_external_auth(self):
        """Assert when there's both a key and certfile, external auth is used"""
        tls_conf = {
            "keyfile": os.path.join(FIXTURES_DIR, "key.pem"),
            "certfile": os.path.join(FIXTURES_DIR, "cert.pem"),
            "ca_cert": os.path.join(FIXTURES_DIR, "ca_bundle.pem"),
        }
        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            publisher = _session.PublisherSession(
                "amqps://username:password@rabbit.example.com/vhost"
            )
        self.assertIsInstance(
            publisher._parameters.credentials, credentials.ExternalCredentials
        )

    def test_publish(self):
        # Check that the publication works properly.
        self.publisher.publish(self.message)
        self.message.validate.assert_called_once()
        self.publisher_channel_publish.assert_called_once()
        publish_call = self.publisher_channel_publish.call_args_list[0][1]
        self.assertEqual(publish_call["exchange"], None)
        self.assertEqual(publish_call["routing_key"], b"test.topic")
        self.assertEqual(publish_call["body"], b'"test body"')

    def test_publish_without_delivery_confirmation(self):
        """Check that the publication without delivery confirmation works properly."""
        self.publisher._confirms = False
        self.publisher._connection = None
        self.publisher._channel = None
        connection_class_mock = mock.Mock()
        connection_mock = mock.Mock()
        channel_mock = mock.Mock()
        connection_class_mock.return_value = connection_mock
        connection_mock.channel.return_value = channel_mock
        with mock.patch(
            "fedora_messaging._session.pika.BlockingConnection", connection_class_mock
        ):
            self.publisher.publish(self.message)
        self.message.validate.assert_called_once()
        if _session._pika_version < pkg_resources.parse_version("1.0.0b1"):
            publish_mock_method = self.publisher._channel.publish
        else:
            publish_mock_method = self.publisher._channel.basic_publish
        publish_mock_method.assert_called_once()
        self.publisher._channel.confirm_delivery.assert_not_called()
        publish_call = publish_mock_method.call_args_list[0][1]
        self.assertEqual(publish_call["exchange"], None)
        self.assertEqual(publish_call["routing_key"], b"test.topic")
        self.assertEqual(publish_call["body"], b'"test body"')

    def test_publish_rejected(self):
        # Check that the correct exception is raised when the publication is
        # rejected.
        # Nacks
        self.publisher_channel_publish.side_effect = pika_errs.NackError([self.message])
        self.assertRaises(PublishReturned, self.publisher.publish, self.message)
        # Unroutables
        self.publisher_channel_publish.side_effect = pika_errs.UnroutableError(
            [self.message]
        )
        self.assertRaises(PublishReturned, self.publisher.publish, self.message)

    def test_publish_generic_error(self):
        # Check that the correct exception is raised when the publication has
        # failed for an unknown reason, and that the connection is closed.
        self.publisher._connection.is_open = False
        self.publisher_channel_publish.side_effect = pika_errs.AMQPError()
        self.assertRaises(ConnectionException, self.publisher.publish, self.message)
        self.publisher._connection.is_open = True
        self.assertRaises(ConnectionException, self.publisher.publish, self.message)
        self.publisher._connection.close.assert_called_once()

    def test_connect_and_publish_not_connnected(self):
        self.publisher._connection = None
        self.publisher._channel = None
        connection_class_mock = mock.Mock()
        connection_mock = mock.Mock()
        channel_mock = mock.Mock()
        connection_class_mock.return_value = connection_mock
        connection_mock.channel.return_value = channel_mock
        self.message._properties = "properties"
        with mock.patch(
            "fedora_messaging._session.pika.BlockingConnection", connection_class_mock
        ):
            self.publisher._connect_and_publish(None, self.message)
        connection_class_mock.assert_called_with(self.publisher._parameters)
        channel_mock.confirm_delivery.assert_called_once()
        if _session._pika_version < pkg_resources.parse_version("1.0.0b1"):
            publish_mock_method = channel_mock.publish
        else:
            publish_mock_method = channel_mock.basic_publish
        publish_mock_method.assert_called_with(
            exchange=None,
            routing_key=b"test.topic",
            body=b'"test body"',
            properties="properties",
        )

    def test_publish_disconnected(self):
        # The publisher must try to re-establish a connection on publish.
        self.publisher_channel_publish.side_effect = pika_errs.ConnectionClosed(
            200, "I wanted to"
        )
        connection_class_mock = mock.Mock()
        connection_mock = mock.Mock()
        channel_mock = mock.Mock()
        connection_class_mock.return_value = connection_mock
        connection_mock.channel.return_value = channel_mock
        with mock.patch(
            "fedora_messaging._session.pika.BlockingConnection", connection_class_mock
        ):
            self.publisher.publish(self.message)
        # Check that the connection was reestablished
        connection_class_mock.assert_called_with(self.publisher._parameters)
        channel_mock.confirm_delivery.assert_called_once()
        self.assertEqual(self.publisher._connection, connection_mock)
        self.assertEqual(self.publisher._channel, channel_mock)
        if _session._pika_version < pkg_resources.parse_version("1.0.0b1"):
            publish_mock_method = channel_mock.publish
        else:
            publish_mock_method = channel_mock.basic_publish
        publish_mock_method.assert_called_once()

    def test_publish_reconnect_failed_generic_error(self):
        # The publisher must try to re-establish a connection on publish, and
        # close the connection if it can't be established.
        self.publisher_channel_publish.side_effect = pika_errs.ConnectionClosed(
            200, "I wanted to"
        )
        connection_class_mock = mock.Mock()
        connection_mock = mock.Mock()
        connection_class_mock.return_value = connection_mock
        connection_mock.channel.side_effect = pika_errs.AMQPError()
        with mock.patch(
            "fedora_messaging._session.pika.BlockingConnection", connection_class_mock
        ):
            self.assertRaises(ConnectionException, self.publisher.publish, self.message)
        # Check that the connection was reestablished
        connection_class_mock.assert_called_with(self.publisher._parameters)
        self.assertEqual(self.publisher._connection, connection_mock)
        connection_mock.close.assert_called_once()

    def test_publish_reconnect_failed_rejected(self):
        # The publisher must try to re-establish a connection on publish, and
        # close the connection if it can't be established.
        self.publisher_channel_publish.side_effect = pika_errs.ConnectionClosed(
            200, "I wanted to"
        )
        connection_class_mock = mock.Mock()
        connection_mock = mock.Mock()
        connection_class_mock.return_value = connection_mock
        connection_mock.channel.side_effect = pika_errs.NackError([self.message])
        with mock.patch(
            "fedora_messaging._session.pika.BlockingConnection", connection_class_mock
        ):
            self.assertRaises(PublishReturned, self.publisher.publish, self.message)
        # Check that the connection was reestablished
        connection_class_mock.assert_called_with(self.publisher._parameters)
        self.assertEqual(self.publisher._connection, connection_mock)

    def test_publish_topic_prefix(self):
        # Check that the topic prefix is correctly prepended to outgoing messages.
        with mock.patch.dict(config.conf, {"topic_prefix": "prefix"}):
            msg = message.Message(topic="test.topic")
            self.publisher.publish(msg)
        self.publisher_channel_publish.assert_called_once()
        publish_call = self.publisher_channel_publish.call_args_list[0][1]
        self.assertEqual(publish_call["routing_key"], b"prefix.test.topic")


class ConsumerSessionTests(unittest.TestCase):
    def setUp(self):
        self.consumer = _session.ConsumerSession()

    def tearDown(self):
        self.consumer._shutdown()

    def test_plain_auth(self):
        """Assert when there's no key or certfile, plain authentication is used"""
        tls_conf = {
            "amqp_url": "amqps://",
            "tls": {
                "keyfile": None,
                "certfile": None,
                "ca_cert": os.path.join(FIXTURES_DIR, "ca_bundle.pem"),
            },
        }

        with mock.patch.dict(config.conf, tls_conf):
            consumer = _session.ConsumerSession()

        self.assertTrue(consumer._parameters.ssl_options is not None)
        self.assertIsInstance(
            consumer._parameters.credentials, credentials.PlainCredentials
        )

    def test_external_auth(self):
        """Assert when there's both a key and certfile, external auth is used"""
        tls_conf = {
            "amqp_url": "amqps://",
            "tls": {
                "keyfile": os.path.join(FIXTURES_DIR, "key.pem"),
                "certfile": os.path.join(FIXTURES_DIR, "cert.pem"),
                "ca_cert": os.path.join(FIXTURES_DIR, "ca_bundle.pem"),
            },
        }

        with mock.patch.dict(config.conf, tls_conf):
            consumer = _session.ConsumerSession()

        self.assertTrue(consumer._parameters.ssl_options is not None)
        self.assertIsInstance(
            consumer._parameters.credentials, credentials.ExternalCredentials
        )

    def test_url_with_client_properties(self):
        """Assert when URL contains client props, values are not taken from config."""
        test_url = "amqp://username:password@rabbit.example.com/vhost?client_properties={'k':'v'}"

        with mock.patch.dict(config.conf, {"amqp_url": test_url}):
            consumer = _session.ConsumerSession()

        self.assertEqual(consumer._parameters.client_properties, {"k": "v"})
        self.assertIsInstance(
            consumer._parameters.credentials, credentials.PlainCredentials
        )

    def test_consume(self):
        """Test the consume function with callable callback."""

        def stop_consumer():
            # Necessary to exit the while loop
            self.consumer._running = False

        connection = mock.Mock()
        connection.ioloop.start.side_effect = stop_consumer
        with mock.patch(
            "fedora_messaging._session.pika.SelectConnection",
            lambda *a, **kw: connection,
        ):
            # Callback is a callable
            def callback(m):
                return

            self.consumer.consume(callback)
            self.assertEqual(self.consumer._consumer_callback, callback)
            connection.ioloop.start.assert_called_once()
            # Callback is a callable class
            self.consumer.consume(mock.Mock)
            self.assertTrue(isinstance(self.consumer._consumer_callback, mock.Mock))
            # Configuration defaults
            self.consumer.consume(callback)
            self.assertEqual(self.consumer._bindings, config.conf["bindings"])
            self.assertEqual(self.consumer._queues, config.conf["queues"])
            self.assertEqual(self.consumer._exchanges, config.conf["exchanges"])
            # Configuration overrides
            test_value = [{"test": "test"}]
            self.consumer.consume(
                callback, bindings=test_value, queues=test_value, exchanges=test_value
            )
            self.assertEqual(self.consumer._bindings, test_value)
            self.assertEqual(self.consumer._queues, test_value)
            self.assertEqual(self.consumer._exchanges, test_value)

    def test_consume_uncallable_callback(self):
        """Test the consume function with not callable callback."""

        class NotCallableClass:
            pass

        # Callback is a not callable class
        self.assertRaises(ValueError, self.consumer.consume, NotCallableClass)
        self.assertFalse(hasattr(self.consumer, "_consumer_callback"))
        # Callback is a not callable
        self.assertRaises(ValueError, self.consumer.consume, "not_callable")
        self.assertFalse(hasattr(self.consumer, "_consumer_callback"))

    def test_consume_with_method(self):
        """Test that a method can be provided as the callback."""

        class Callback(object):
            def callback(self):
                return

        callback = Callback().callback

        def stop_consumer():
            # Necessary to exit the while loop
            self.consumer._running = False

        connection = mock.Mock()
        connection.ioloop.start.side_effect = stop_consumer

        with mock.patch(
            "fedora_messaging._session.pika.SelectConnection",
            lambda *a, **kw: connection,
        ):
            self.consumer.consume(callback)
            self.assertEqual(self.consumer._consumer_callback, callback)
            connection.ioloop.start.assert_called_once()
            # Callback is a callable class
            self.consumer.consume(mock.Mock)
            self.assertTrue(isinstance(self.consumer._consumer_callback, mock.Mock))
            # Configuration defaults
            self.consumer.consume(callback)
            self.assertEqual(self.consumer._bindings, config.conf["bindings"])
            self.assertEqual(self.consumer._queues, config.conf["queues"])
            self.assertEqual(self.consumer._exchanges, config.conf["exchanges"])
            # Configuration overrides
            test_value = [{"test": "test"}]
            self.consumer.consume(
                callback, bindings=test_value, queues=test_value, exchanges=test_value
            )
            self.assertEqual(self.consumer._bindings, test_value)
            self.assertEqual(self.consumer._queues, test_value)
            self.assertEqual(self.consumer._exchanges, test_value)

    def test_declare(self):
        # Test that the exchanges, queues and bindings are properly
        # declared.
        self.consumer._channel = mock.Mock()
        self.consumer._exchanges = {
            "testexchange": {
                "type": "type",
                "durable": "durable",
                "auto_delete": "auto_delete",
                "arguments": "arguments",
            }
        }
        self.consumer._queues = {
            "testqueue": {
                "durable": "durable",
                "auto_delete": "auto_delete",
                "exclusive": "exclusive",
                "arguments": "arguments",
            }
        }
        self.consumer._bindings = [
            {
                "queue": "testqueue",
                "exchange": "testexchange",
                "routing_keys": ["testrk"],
            }
        ]
        # Declare exchanges and queues
        self.consumer._on_qosok(None)
        self.consumer._channel.exchange_declare.assert_called_with(
            exchange="testexchange",
            exchange_type="type",
            durable="durable",
            auto_delete="auto_delete",
            arguments="arguments",
            passive=False,
            callback=self.consumer._on_exchange_declareok,
        )
        self.consumer._channel.queue_declare.assert_called_with(
            queue="testqueue",
            durable="durable",
            auto_delete="auto_delete",
            exclusive="exclusive",
            arguments="arguments",
            passive=False,
            callback=self.consumer._on_queue_declareok,
        )
        # Declare bindings
        frame = mock.Mock()
        frame.method.queue = "testqueue"
        self.consumer._on_queue_declareok(frame)
        self.consumer._channel.queue_bind.assert_called_with(
            queue="testqueue",
            exchange="testexchange",
            routing_key="testrk",
            callback=None,
        )
        if _session._pika_version < pkg_resources.parse_version("1.0.0b1"):
            self.consumer._channel.basic_consume.assert_called_with(
                consumer_callback=self.consumer._on_message, queue="testqueue"
            )
        else:
            self.consumer._channel.basic_consume.assert_called_with(
                on_message_callback=self.consumer._on_message, queue="testqueue"
            )

    def test_declare_passive(self):
        # Test that the exchanges, queues and bindings are passively declared
        # if configured so.
        self.consumer._channel = mock.Mock()
        self.consumer._exchanges = {
            "testexchange": {
                "type": "type",
                "durable": "durable",
                "auto_delete": "auto_delete",
                "arguments": "arguments",
            }
        }
        self.consumer._queues = {
            "testqueue": {
                "durable": "durable",
                "auto_delete": "auto_delete",
                "exclusive": "exclusive",
                "arguments": "arguments",
            }
        }
        with mock.patch.dict(config.conf, {"passive_declares": True}):
            # Declare exchanges and queues
            self.consumer._on_qosok(None)
            call_args = self.consumer._channel.exchange_declare.call_args_list[-1][1]
            assert call_args.get("passive") is True
            call_args = self.consumer._channel.queue_declare.call_args_list[-1][1]
            assert call_args.get("passive") is True

    @mock.patch("fedora_messaging._session.pika.SelectConnection")
    def test_reconnect_not_running(self, mock_new_connection):
        """Assert listening on old connection is terminated on reconnection."""
        mock_old_connection = mock.Mock()
        self.consumer._running = False
        self.consumer._connection = mock_old_connection
        self.consumer.reconnect()
        mock_old_connection.ioloop.stop.assert_called_once_with()
        mock_new_connection.assert_not_called()
        mock_new_connection.ioloop.start.assert_not_called()

    @mock.patch("fedora_messaging._session.pika.SelectConnection")
    def test_reconnect_running(self, mock_new_connection):
        """
        Assert listening on old connection is terminated on reconnection,
        and new connection is created.
        """
        mock_new_connection.return_value = mock_new_connection
        self.consumer._running = True
        mock_old_connection = mock.Mock()
        self.consumer._connection = mock_old_connection
        self.consumer.reconnect()
        mock_old_connection.ioloop.stop.assert_called_once_with()
        mock_new_connection.assert_called_once_with(
            self.consumer._parameters,
            on_open_callback=self.consumer._on_connection_open,
            on_open_error_callback=self.consumer._on_connection_error,
            on_close_callback=self.consumer._on_connection_close,
        )
        mock_new_connection.ioloop.start.assert_called_once_with()
        self.assertIs(self.consumer._connection, mock_new_connection)

    @mock.patch("fedora_messaging._session.pika.SelectConnection")
    def test_connect(self, mock_connection):
        """
        Assert new connection of type pika.SelectConnection is created
        and proper callbacks are registered.
        """
        mock_connection.return_value = mock_connection
        self.consumer.connect()
        mock_connection.assert_called_once_with(
            self.consumer._parameters,
            on_open_callback=self.consumer._on_connection_open,
            on_open_error_callback=self.consumer._on_connection_error,
            on_close_callback=self.consumer._on_connection_close,
        )
        self.assertIs(self.consumer._connection, mock_connection)

    def test_schedule_callback_with_call_later(self):
        """Assert callback is scheduled to run with method 'call_later'."""
        delay = "test_delay"
        callback = "test_callback"
        mock_connection = mock.Mock()
        self.consumer._connection = mock_connection
        self.consumer.call_later(delay, callback)
        mock_connection.ioloop.call_later.assert_called_once_with(delay, callback)

    def test_schedule_callback_with_add_timeout(self):
        """Assert callback is scheduled to run with method 'add_timeout'."""
        delay = "test_delay"
        callback = "test_callback"
        mock_connection = mock.Mock()
        del mock_connection.ioloop.call_later
        self.consumer._connection = mock_connection
        self.consumer.call_later(delay, callback)
        mock_connection.ioloop.add_timeout.assert_called_once_with(delay, callback)

    def test_on_cancel(self):
        """Assert proper information is logged on callback _on_cancel call."""
        with mock.patch("fedora_messaging._session._log") as mock_log:
            self.consumer._on_cancel("cancel_frame")
        mock_log.info.assert_called_once_with("Server canceled consumer")

    def test_on_exchange_declareok(self):
        """Assert proper information is logged on callback _on_exchange_declareok call."""
        with mock.patch("fedora_messaging._session._log") as mock_log:
            self.consumer._on_exchange_declareok("declare_frame")
        mock_log.info.assert_called_once_with("Exchange declared successfully")

    def test_connection_error_with_string_as_error(self):
        """Assert callback is called on connection error."""
        connection = "test_connection"
        error_message = "test_err_msg"
        mock_connection = mock.Mock()
        self.consumer._connection = mock_connection
        with mock.patch("fedora_messaging._session._log") as mock_log:
            self.consumer._on_connection_error(connection, error_message)
        mock_connection.ioloop.call_later.assert_called_once_with(
            1, self.consumer.reconnect
        )
        self.assertEqual(self.consumer._channel, None)
        mock_log.error.assert_called_once_with(error_message)

    def test_connection_error_with_pika_exception_as_error(self):
        """Assert callback is called on connection error."""
        connection = "test_connection"
        error_message = "test_err_msg"
        mock_connection = mock.Mock()
        self.consumer._connection = mock_connection
        with mock.patch("fedora_messaging._session._log") as mock_log:
            self.consumer._on_connection_error(
                connection, pika_errs.AMQPConnectionError(error_message)
            )
        mock_connection.ioloop.call_later.assert_called_once_with(
            1, self.consumer.reconnect
        )
        self.assertEqual(self.consumer._channel, None)
        mock_log.error.assert_called_once_with(repr(error_message))

    def test_on_cancelok(self):
        """Assert proper method is called to rejecet messages on channel."""
        self.consumer._channel = mock.Mock()
        with mock.patch("fedora_messaging._session._log") as mock_log:
            self.consumer._on_cancelok("cancel_frame")
        mock_log.info.assert_called_once_with(
            "Consumer canceled; returning all unprocessed messages to the queue"
        )
        self.consumer._channel.basic_nack.assert_called_once_with(
            delivery_tag=0, multiple=True, requeue=True
        )

    def test_on_channel_open(self):
        """Assert proper callbacks are registered when channel is open."""
        mock_channel = mock.Mock()
        with mock.patch.dict(
            config.conf, {"qos": {"test_arg_1": "t_a_1", "test_arg_2": "t_a_2"}}
        ):
            self.consumer._on_channel_open(mock_channel)
        mock_channel.add_on_close_callback.assert_called_once_with(
            self.consumer._on_channel_close
        )
        mock_channel.add_on_cancel_callback.assert_called_once_with(
            self.consumer._on_cancel
        )
        mock_channel.basic_qos.assert_called_once_with(
            callback=self.consumer._on_qosok, test_arg_1="t_a_1", test_arg_2="t_a_2"
        )

    def test_on_connection_open(self):
        """Assert proper callback is registered when connection is open."""
        host_name = "test_host"
        mock_channel = mock.Mock()
        conn_mock_attrs = {
            "channel.return_value": mock_channel,
            "params.host": host_name,
        }
        mock_connection = mock.Mock(**conn_mock_attrs)
        with mock.patch("fedora_messaging._session._log") as mock_log:
            self.consumer._on_connection_open(mock_connection)
        mock_log.info.assert_called_once_with(
            "Successfully opened connection to %s", host_name
        )
        mock_connection.channel.assert_called_once_with(
            on_open_callback=self.consumer._on_channel_open
        )
        self.assertEqual(self.consumer._channel, mock_channel)

    def test_on_channel_close_pass_int_and_str(self):
        """Assert proper information is logged on callback _on_channel_close call."""
        self.consumer._channel = mock.Mock()
        reply_code_or_reason = 1
        reply_text = "reply_text"
        test_channel = "test_channel"
        with mock.patch("fedora_messaging._session._log") as mock_log:
            self.consumer._on_channel_close(
                test_channel, reply_code_or_reason, reply_text
            )
        mock_log.info.assert_called_once_with(
            "Channel %r closed (%d): %s", test_channel, reply_code_or_reason, reply_text
        )
        self.assertEqual(self.consumer._channel, None)

    def test_on_channel_close_pass_str_and_str(self):
        """Assert proper information is logged on callback _on_channel_close call."""
        self.consumer._channel = mock.Mock()
        reply_code_or_reason = "reply_code_or_reason"
        reply_text = "reply_text"
        test_channel = "test_channel"
        with mock.patch("fedora_messaging._session._log") as mock_log:
            self.consumer._on_channel_close(
                test_channel, reply_code_or_reason, reply_text
            )
        mock_log.info.assert_called_once_with(
            "Channel %r closed (%d): %s", test_channel, 0, reply_code_or_reason
        )
        self.assertEqual(self.consumer._channel, None)

    def test_on_connection_close_pass_int_200_and_str(self):
        """
        Assert proper information is logged on callback _on_connection_close call
        and connection is closed when reply_code is 200.
        """
        mock_connection = mock.Mock()
        self.consumer._channel = mock.Mock()
        reply_code_or_reason = 200
        reply_text = "reply_text"
        with mock.patch("fedora_messaging._session._log") as mock_log:
            self.consumer._on_connection_close(
                mock_connection, reply_code_or_reason, reply_text
            )
        mock_log.info.assert_called_once_with(
            "Server connection closed (%s), shutting down", reply_text
        )
        self.assertEqual(self.consumer._channel, None)
        mock_connection.ioloop.stop.assert_called_once_with()

    def test_on_connection_close_pass_str_and_str(self):
        """
        Assert proper information is logged on callback _on_connection_close call
        and method call_later is called when reply_code is not 200.
        """
        host_name = "test_host"
        conn_mock_attrs = {"params.host": host_name}
        mock_connection = mock.Mock(**conn_mock_attrs)
        self.consumer._connection = mock_connection
        self.consumer._channel = mock.Mock()
        reply_code_or_reason = "reply_code"
        reply_text = "reply_text"
        with mock.patch("fedora_messaging._session._log") as mock_log:
            self.consumer._on_connection_close(
                mock_connection, reply_code_or_reason, reply_text
            )
        mock_log.warning.assert_called_once_with(
            "Connection to %s closed unexpectedly (%d): %s",
            host_name,
            0,
            reply_code_or_reason,
        )
        self.assertEqual(self.consumer._channel, None)
        mock_connection.ioloop.call_later.assert_called_once_with(
            1, self.consumer.reconnect
        )

    def test_signal_SIGTERM(self):
        """Assert ConsumerSession._shutdown is called on signal SIGTERM."""
        consumer_tags = "test_consumer_tags"
        channel_mock_attrs = {"consumer_tags": consumer_tags}
        mock_channel = mock.Mock(**channel_mock_attrs)
        conn_mock_attrs = {"is_open": True}
        self.consumer._running = True
        mock_connection = mock.Mock(**conn_mock_attrs)
        self.consumer._connection = mock_connection
        self.consumer._channel = mock_channel
        signal_handler = signal.getsignal(signal.SIGTERM)

        with mock.patch("fedora_messaging._session._log") as mock_log:
            signal_handler(signal.SIGTERM, "test_frame")

        mock_log.info.assert_called_once_with(
            "Halting %r consumer sessions", consumer_tags
        )
        self.assertEqual(self.consumer._running, False)
        mock_connection.close.assert_called_once_with()
        self.assertEqual(signal.getsignal(signal.SIGTERM), signal.SIG_DFL)

    def test_signal_SIGINT(self):
        """Assert ConsumerSession._shutdown is called on signal SIGINT."""
        consumer_tags = "test_consumer_tags"
        channel_mock_attrs = {"consumer_tags": consumer_tags}
        mock_channel = mock.Mock(**channel_mock_attrs)
        conn_mock_attrs = {"is_open": True}
        mock_connection = mock.Mock(**conn_mock_attrs)
        self.consumer._running = True
        self.consumer._connection = mock_connection
        self.consumer._channel = mock_channel
        signal_handler = signal.getsignal(signal.SIGINT)

        with mock.patch("fedora_messaging._session._log") as mock_log:
            signal_handler(signal.SIGINT, "test_frame")

        mock_log.info.assert_called_once_with(
            "Halting %r consumer sessions", consumer_tags
        )
        self.assertEqual(self.consumer._running, False)
        mock_connection.close.assert_called_once_with()
        self.assertEqual(signal.getsignal(signal.SIGINT), signal.SIG_DFL)

    def test_other_signals(self):
        """Assert handlers for signals other than SIGINT and SIGTERM are default handlers."""
        for sig_no in range(1, signal.NSIG):
            if sig_no in (signal.SIGINT, signal.SIGTERM, 32, 33):
                continue
            signal_handler = signal.getsignal(sig_no)
            if sig_no in (signal.SIGPIPE, signal.SIGXFSZ):
                self.assertEqual(signal.SIG_IGN, signal_handler)
            else:
                self.assertEqual(signal.SIG_DFL, signal_handler)


class ConsumerSessionMessageTests(unittest.TestCase):
    def setUp(self):
        self.consumer = _session.ConsumerSession()
        self.callback = self.consumer._consumer_callback = mock.Mock()
        self.channel = mock.Mock()
        self.consumer._connection = mock.Mock()
        self.consumer._running = True
        self.consumer._consumers["consumer1"] = "my_queue"
        self.frame = mock.Mock()
        self.frame.consumer_tag = "consumer1"
        self.frame.delivery_tag = "testtag"
        self.frame.routing_key = "test.topic"
        self.properties = mock.Mock()
        self.properties.headers = {}
        self.properties.content_encoding = "utf-8"
        self.validate_patch = mock.patch("fedora_messaging.message.Message.validate")
        self.validate_mock = self.validate_patch.start()

    def tearDown(self):
        self.validate_patch.stop()
        self.consumer._shutdown()

    def test_message(self):
        body = b'"test body"'
        self.consumer._on_message(self.channel, self.frame, self.properties, body)
        self.consumer._consumer_callback.assert_called_once()
        msg = self.consumer._consumer_callback.call_args_list[0][0][0]
        msg.validate.assert_called_once()
        self.channel.basic_ack.assert_called_with(delivery_tag="testtag")
        self.assertEqual(msg.body, "test body")

    def test_message_queue_set(self):
        """Assert the queue attribute is set on messages."""
        self.consumer._on_message(self.channel, self.frame, self.properties, b"{}")
        msg = self.consumer._consumer_callback.call_args_list[0][0][0]
        self.assertEqual(msg.queue, "my_queue")

    def test_message_encoding(self):
        body = '"test body unicode é à ç"'.encode("utf-8")
        self.properties.content_encoding = None
        self.consumer._on_message(self.channel, self.frame, self.properties, body)
        self.consumer._consumer_callback.assert_called_once()
        msg = self.consumer._consumer_callback.call_args_list[0][0][0]
        self.assertEqual(msg.body, "test body unicode é à ç")

    def test_message_wrong_encoding(self):
        body = '"test body unicode é à ç"'.encode("utf-8")
        self.properties.content_encoding = "ascii"
        self.consumer._on_message(self.channel, self.frame, self.properties, body)
        self.channel.basic_nack.assert_called_with(
            delivery_tag="testtag", requeue=False
        )
        self.consumer._consumer_callback.assert_not_called()

    def test_message_not_json(self):
        body = b"plain string"
        self.consumer._on_message(self.channel, self.frame, self.properties, body)
        self.channel.basic_nack.assert_called_with(
            delivery_tag="testtag", requeue=False
        )
        self.consumer._consumer_callback.assert_not_called()

    def test_message_not_object(self):
        body = b"'json string'"
        self.consumer._on_message(self.channel, self.frame, self.properties, body)
        self.channel.basic_nack.assert_called_with(
            delivery_tag="testtag", requeue=False
        )
        self.consumer._consumer_callback.assert_not_called()

    def test_message_validation_failed(self):
        body = b'"test body"'
        self.validate_mock.side_effect = JSONValidationError(None)
        self.consumer._on_message(self.channel, self.frame, self.properties, body)
        self.channel.basic_nack.assert_called_with(
            delivery_tag="testtag", requeue=False
        )
        self.consumer._consumer_callback.assert_not_called()

    def test_message_nack(self):
        self.consumer._consumer_callback.side_effect = Nack()
        self.consumer._on_message(self.channel, self.frame, self.properties, b'"body"')
        self.channel.basic_nack.assert_called_with(delivery_tag="testtag", requeue=True)

    def test_message_drop(self):
        self.consumer._consumer_callback.side_effect = Drop()
        self.consumer._on_message(self.channel, self.frame, self.properties, b'"body"')
        self.channel.basic_nack.assert_called_with(
            delivery_tag="testtag", requeue=False
        )

    def test_message_halt_requeue(self):
        """Assert messages are requeued when the HaltConsumer exception is raised with requeue"""
        self.consumer._consumer_callback.side_effect = HaltConsumer(requeue=True)
        self.consumer._on_message(self.channel, self.frame, self.properties, b'"body"')
        self.channel.basic_nack.assert_called_with(delivery_tag="testtag", requeue=True)
        self.assertFalse(self.consumer._running)
        self.consumer._connection.close.assert_called_once()

    def test_message_halt_no_requeue(self):
        """Assert messages are not requeued by default with HaltConsumer"""
        self.consumer._consumer_callback.side_effect = HaltConsumer()
        self.consumer._on_message(self.channel, self.frame, self.properties, b'"body"')
        self.channel.basic_nack.assert_called_with(
            delivery_tag="testtag", requeue=False
        )
        self.assertFalse(self.consumer._running)
        self.consumer._connection.close.assert_called_once()

    def test_message_halt_exitcode_not_0(self):
        """Assert HaltConsumer exception is re-raised when exit code is not 0"""
        self.consumer._consumer_callback.side_effect = HaltConsumer(exit_code=1)
        self.assertRaises(
            HaltConsumer,
            self.consumer._on_message,
            self.channel,
            self.frame,
            self.properties,
            b'"body"',
        )
        self.channel.basic_nack.assert_called_with(
            delivery_tag="testtag", requeue=False
        )
        self.assertFalse(self.consumer._running)
        self.consumer._connection.close.assert_called_once()

    def test_message_exception(self):
        error = ValueError()
        self.consumer._consumer_callback.side_effect = error
        with self.assertRaises(HaltConsumer) as cm:
            self.consumer._on_message(
                self.channel, self.frame, self.properties, b'"body"'
            )
        self.assertEqual(cm.exception.exit_code, 1)
        self.assertEqual(cm.exception.reason, error)
        self.channel.basic_nack.assert_called_with(
            delivery_tag=0, multiple=True, requeue=True
        )
        self.assertFalse(self.consumer._running)
        self.consumer._connection.close.assert_called_once()

    def test_message_topic_prefix(self):
        # Check that the topic_prefix isn't added to incoming messages.
        body = b'"test body"'
        with mock.patch.dict(config.conf, {"topic_prefix": "prefix"}):
            self.consumer._on_message(self.channel, self.frame, self.properties, body)
        self.consumer._consumer_callback.assert_called_once()
        msg = self.consumer._consumer_callback.call_args_list[0][0][0]
        self.assertEqual(msg.topic, "test.topic")


class ConfigureTlsParameters(unittest.TestCase):
    """Tests for :func:`fedora_messaging._session._configure_tls_parameters`"""

    @unittest.skipIf(_session.SSLOptions is not None, "Pika supports SSLContext")
    def test_old_pika_approach(self):
        """Assert if pika is pre-1.0.0, the TLS settings are applied."""
        params = URLParameters("amqps://")
        tls_conf = {
            "keyfile": "key.pem",
            "certfile": "cert.pem",
            "ca_cert": "custom_ca_bundle.pem",
        }
        expected_options = {
            "keyfile": "key.pem",
            "certfile": "cert.pem",
            "ca_certs": "custom_ca_bundle.pem",
            "cert_reqs": ssl.CERT_REQUIRED,
            "ssl_version": ssl.PROTOCOL_TLSv1_2,
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            _session._configure_tls_parameters(params)

        self.assertTrue(params.ssl)
        self.assertEqual(params.ssl_options, expected_options)

    @unittest.skipIf(_session.SSLOptions is not None, "Pika supports SSLContext")
    def test_old_pika_approach_no_key(self):
        """Assert if no key is provided, no cert is passed to pika either."""
        params = URLParameters("amqps://")
        tls_conf = {
            "keyfile": None,
            "certfile": "cert.pem",
            "ca_cert": "custom_ca_bundle.pem",
        }
        expected_options = {
            "keyfile": None,
            "certfile": None,
            "ca_certs": "custom_ca_bundle.pem",
            "cert_reqs": ssl.CERT_REQUIRED,
            "ssl_version": ssl.PROTOCOL_TLSv1_2,
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            _session._configure_tls_parameters(params)

        self.assertTrue(params.ssl)
        self.assertEqual(params.ssl_options, expected_options)

    @unittest.skipIf(_session.SSLOptions is not None, "Pika supports SSLContext")
    def test_old_pika_approach_no_cert(self):
        """Assert if no cert is provided, no key is passed to pika either."""
        params = URLParameters("amqps://")
        tls_conf = {"keyfile": "key.pem", "certfile": None, "ca_cert": "ca_bundle.pem"}
        expected_options = {
            "keyfile": None,
            "certfile": None,
            "ca_certs": "ca_bundle.pem",
            "cert_reqs": ssl.CERT_REQUIRED,
            "ssl_version": ssl.PROTOCOL_TLSv1_2,
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            _session._configure_tls_parameters(params)

        self.assertTrue(params.ssl)
        self.assertEqual(params.ssl_options, expected_options)

    @unittest.skipIf(
        _session.SSLOptions is None, "Pika version does not support SSLContext"
    )
    def test_new_pika(self):
        """Assert configuring a cert and key results in a TLS connection with new pika versions."""
        params = URLParameters("amqps://myhost")
        tls_conf = {
            "keyfile": os.path.join(FIXTURES_DIR, "key.pem"),
            "certfile": os.path.join(FIXTURES_DIR, "cert.pem"),
            "ca_cert": os.path.join(FIXTURES_DIR, "ca_bundle.pem"),
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            _session._configure_tls_parameters(params)

        self.assertTrue(isinstance(params.ssl_options, _session.SSLOptions))

    @unittest.skipIf(
        _session.SSLOptions is None, "Pika version does not support SSLContext"
    )
    def test_new_pika_invalid_key(self):
        """Assert a ConfigurationException is raised when the key can't be opened."""
        params = URLParameters("amqps://myhost")
        tls_conf = {
            "keyfile": os.path.join(FIXTURES_DIR, "invalid_key.pem"),
            "certfile": os.path.join(FIXTURES_DIR, "cert.pem"),
            "ca_cert": os.path.join(FIXTURES_DIR, "ca_bundle.pem"),
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            self.assertRaises(
                ConfigurationException, _session._configure_tls_parameters, params
            )

        self.assertTrue(isinstance(params.ssl_options, _session.SSLOptions))

    @unittest.skipIf(
        _session.SSLOptions is None, "Pika version does not support SSLContext"
    )
    def test_new_pika_invalid_ca_cert(self):
        """Assert a ConfigurationException is raised when the CA can't be opened."""
        params = URLParameters("amqps://myhost")
        tls_conf = {
            "keyfile": os.path.join(FIXTURES_DIR, "key.pem"),
            "certfile": os.path.join(FIXTURES_DIR, "cert.pem"),
            "ca_cert": os.path.join(FIXTURES_DIR, "invalid_ca.pem"),
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            self.assertRaises(
                ConfigurationException, _session._configure_tls_parameters, params
            )

        self.assertTrue(isinstance(params.ssl_options, _session.SSLOptions))
