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

import mock
import pkg_resources
from pika import exceptions as pika_errs, URLParameters, credentials
from jsonschema.exceptions import ValidationError as JSONValidationError

from fedora_messaging import _session, config
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
        self.message = mock.Mock()
        self.message._headers = {}
        self.message.topic = "test.topic"
        self.message._encoded_routing_key = b"test.topic"
        self.message._body = "test body"
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
                "amqps://username:password@rabbit.example.com/vhost", "test_exchange"
            )
        self.assertEqual(publisher._parameters.host, "rabbit.example.com")
        self.assertEqual(publisher._parameters.port, 5671)
        self.assertEqual(publisher._parameters.virtual_host, "vhost")
        self.assertIsNotNone(publisher._parameters.ssl_options)
        self.assertEqual(publisher._exchange, "test_exchange")

    def test_plain_auth(self):
        """Assert when there's no key or certfile, plain authentication is used"""
        with mock.patch.dict(config.conf, {"tls": self.tls_conf}):
            publisher = _session.PublisherSession(
                "amqps://username:password@rabbit.example.com/vhost", "test_exchange"
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
                "amqps://username:password@rabbit.example.com/vhost", "test_exchange"
            )
        self.assertIsInstance(
            publisher._parameters.credentials, credentials.ExternalCredentials
        )

    def test_publish(self):
        # Check that the publication works properly.
        self.publisher.publish(self.message)
        self.message.validate.assert_called_once()
        self.publisher._channel.publish.assert_called_once()
        publish_call = self.publisher._channel.publish.call_args_list[0][1]
        self.assertEqual(publish_call["exchange"], None)
        self.assertEqual(publish_call["routing_key"], b"test.topic")
        self.assertEqual(publish_call["body"], b'"test body"')

    def test_publish_rejected(self):
        # Check that the correct exception is raised when the publication is
        # rejected.
        self.publisher._channel.publish.side_effect = pika_errs.NackError(
            [self.message]
        )
        self.assertRaises(PublishReturned, self.publisher.publish, self.message)
        self.publisher._channel.publish.side_effect = pika_errs.UnroutableError(
            [self.message]
        )
        self.assertRaises(PublishReturned, self.publisher.publish, self.message)

    def test_publish_generic_error(self):
        # Check that the correct exception is raised when the publication has
        # failed for an unknown reason, and that the connection is closed.
        self.publisher._connection.is_open = False
        self.publisher._channel.publish.side_effect = pika_errs.AMQPError()
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
        channel_mock.publish.assert_called_with(
            exchange=None,
            routing_key=b"test.topic",
            body=b'"test body"',
            properties="properties",
        )

    def test_publish_disconnected(self):
        # The publisher must try to re-establish a connection on publish.
        self.publisher._channel.publish.side_effect = pika_errs.ConnectionClosed(
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
        channel_mock.publish.assert_called_once()

    def test_publish_reconnect_failed_generic_error(self):
        # The publisher must try to re-establish a connection on publish, and
        # close the connection if it can't be established.
        self.publisher._channel.publish.side_effect = pika_errs.ConnectionClosed(
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
        self.publisher._channel.publish.side_effect = pika_errs.ConnectionClosed(
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

    def test_consume(self):
        # Test the consume function.
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
            # Callback is a class
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
            callback=self.consumer._on_exchange_declareok,
        )
        self.consumer._channel.queue_declare.assert_called_with(
            queue="testqueue",
            durable="durable",
            auto_delete="auto_delete",
            exclusive="exclusive",
            arguments="arguments",
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
        if pkg_resources.get_distribution("pika").version.startswith("0.12."):
            self.consumer._channel.basic_consume.assert_called_with(
                consumer_callback=self.consumer._on_message, queue="testqueue"
            )
        else:
            self.consumer._channel.basic_consume.assert_called_with(
                on_message_callback=self.consumer._on_message, queue="testqueue"
            )


class ConsumerSessionMessageTests(unittest.TestCase):
    def setUp(self):
        self.consumer = _session.ConsumerSession()
        self.callback = self.consumer._consumer_callback = mock.Mock()
        self.channel = mock.Mock()
        self.consumer._connection = mock.Mock()
        self.consumer._running = True
        self.frame = mock.Mock()
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
        self.assertEqual(msg._body, "test body")

    def test_message_encoding(self):
        body = '"test body unicode é à ç"'.encode("utf-8")
        self.properties.content_encoding = None
        self.consumer._on_message(self.channel, self.frame, self.properties, body)
        self.consumer._consumer_callback.assert_called_once()
        msg = self.consumer._consumer_callback.call_args_list[0][0][0]
        self.assertEqual(msg._body, "test body unicode é à ç")

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
