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
from pika import exceptions as pika_errs

from fedora_messaging import _session
from fedora_messaging.exceptions import PublishReturned, ConnectionException


class PublisherSessionTests(unittest.TestCase):

    def setUp(self):
        self.publisher = _session.PublisherSession()
        self.publisher._connection = mock.Mock()
        self.publisher._channel = mock.Mock()
        self.message = mock.Mock()
        self.message.headers = {}
        self.message.topic = "test.topic"
        self.message.body = "test body"
        self.message.schema_version = 1

    def test_publisher_init(self):
        publisher = _session.PublisherSession()
        self.assertEqual(publisher._parameters.host, "localhost")
        self.assertEqual(publisher._parameters.port, 5672)
        self.assertEqual(publisher._parameters.virtual_host, "/")
        self.assertEqual(publisher._parameters.ssl, False)
        # Now test with a custom URL
        publisher = _session.PublisherSession(
            "amqps://username:password@rabbit.example.com/vhost",
            "test_exchange",
        )
        self.assertEqual(publisher._parameters.host, "rabbit.example.com")
        self.assertEqual(publisher._parameters.port, 5671)
        self.assertEqual(publisher._parameters.virtual_host, "vhost")
        self.assertEqual(publisher._parameters.ssl, True)
        self.assertEqual(publisher._exchange, "test_exchange")

    def test_publish(self):
        # Check that the publication works properly.
        self.publisher.publish(self.message)
        self.message.validate.assert_called_once()
        self.publisher._channel.publish.assert_called_once()
        publish_call = self.publisher._channel.publish.call_args_list[0][0]
        self.assertEqual(publish_call[0], None)
        self.assertEqual(publish_call[1], b"test.topic")
        self.assertEqual(publish_call[2], b'"test body"')
        properties = publish_call[3]
        self.assertEqual(properties.content_type, "application/json")
        self.assertEqual(properties.content_encoding, "utf-8")
        self.assertEqual(properties.delivery_mode, 2)
        self.assertDictEqual(properties.headers, {
            'fedora_messaging_schema': "mock.mock:Mock",
            'fedora_messaging_schema_version': 1,
        })

    def test_publish_rejected(self):
        # Check that the correct exception is raised when the publication is
        # rejected.
        self.publisher._channel.publish.side_effect = \
            pika_errs.NackError([self.message])
        self.assertRaises(
            PublishReturned, self.publisher.publish, self.message)
        self.publisher._channel.publish.side_effect = \
            pika_errs.UnroutableError([self.message])
        self.assertRaises(
            PublishReturned, self.publisher.publish, self.message)

    def test_publish_generic_error(self):
        # Check that the correct exception is raised when the publication has
        # failed for an unknown reason, and that the connection is closed.
        self.publisher._connection.is_open = False
        self.publisher._channel.publish.side_effect = \
            pika_errs.AMQPError()
        self.assertRaises(
            ConnectionException, self.publisher.publish, self.message)
        self.publisher._connection.is_open = True
        self.assertRaises(
            ConnectionException, self.publisher.publish, self.message)
        self.publisher._connection.close.assert_called_once()

    def test_connect_and_publish_not_connnected(self):
        self.publisher._connection = None
        self.publisher._channel = None
        connection_class_mock = mock.Mock()
        connection_mock = mock.Mock()
        channel_mock = mock.Mock()
        connection_class_mock.return_value = connection_mock
        connection_mock.channel.return_value = channel_mock
        with mock.patch(
                "fedora_messaging._session.pika.BlockingConnection",
                connection_class_mock):
            self.publisher._connect_and_publish(
                None, self.message, "properties")
        connection_class_mock.assert_called_with(self.publisher._parameters)
        channel_mock.confirm_delivery.assert_called_once()
        channel_mock.publish.assert_called_with(
            None, b"test.topic", b'"test body"', "properties",
        )

    def test_publish_disconnected(self):
        # The publisher must try to re-establish a connection on publish.
        self.publisher._channel.publish.side_effect = \
            pika_errs.ConnectionClosed()
        connection_class_mock = mock.Mock()
        connection_mock = mock.Mock()
        channel_mock = mock.Mock()
        connection_class_mock.return_value = connection_mock
        connection_mock.channel.return_value = channel_mock
        with mock.patch(
                "fedora_messaging._session.pika.BlockingConnection",
                connection_class_mock):
            self.publisher.publish(self.message)
        # Check that the connection was reestablished
        connection_class_mock.assert_called_with(self.publisher._parameters)
        channel_mock.confirm_delivery.assert_called_once()
        self.assertEqual(self.publisher._connection, connection_mock)
        self.assertEqual(self.publisher._channel, channel_mock)
        channel_mock.publish.assert_called_once()

    def test_publish_reconnect_failed(self):
        # The publisher must try to re-establish a connection on publish, and
        # close the connection if it can't be established.
        self.publisher._channel.publish.side_effect = \
            pika_errs.ChannelClosed()
        connection_class_mock = mock.Mock()
        connection_mock = mock.Mock()
        connection_class_mock.return_value = connection_mock
        connection_mock.channel.side_effect = pika_errs.AMQPConnectionError()
        with mock.patch(
                "fedora_messaging._session.pika.BlockingConnection",
                connection_class_mock):
            self.assertRaises(
                ConnectionException, self.publisher.publish, self.message)
        # Check that the connection was reestablished
        connection_class_mock.assert_called_with(self.publisher._parameters)
        self.assertEqual(self.publisher._connection, connection_mock)
        connection_mock.close.assert_called_once()
