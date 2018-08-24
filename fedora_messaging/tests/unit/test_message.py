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

import datetime
import json
import unittest

import jsonschema
import mock

from fedora_messaging import message, exceptions


class GetMessageTests(unittest.TestCase):
    """Tests for the :func:`fedora_messaging.message.get_message` function."""

    def test_missing_severity(self):
        """Assert the default severity is INFO if it's not in the headers."""
        msg = message.Message(severity=message.ERROR)
        del msg._headers["fedora_messaging_severity"]

        recv_msg = message.get_message("", msg._properties, b"{}")
        self.assertEqual(recv_msg.severity, message.INFO)

    def test_invalid_severity(self):
        """Assert the invalid severity fails validation."""
        msg = message.Message()
        msg._headers["fedora_messaging_severity"] = 42

        self.assertRaises(
            exceptions.ValidationError, message.get_message, "", msg._properties, b"{}"
        )

    def test_missing_headers(self):
        """Assert missing headers results in a default message."""
        msg = message.Message()
        msg._headers = None
        expected_message = message.Message()
        expected_message.id = msg.id

        received_msg = message.get_message(
            msg._encoded_routing_key, msg._properties, msg._encoded_body
        )
        self.assertIsInstance(received_msg, message.Message)


class MessageTests(unittest.TestCase):
    """Tests for the :class:`fedora_messaging.message.Message` class."""

    def test_summary(self):
        """Assert message summaries default to the message topic."""
        msg = message.Message(topic="test.topic")
        self.assertEqual(msg.topic, msg.summary)

    def test_str(self):
        """Assert calling str on a message produces a human-readable result."""
        msg = message.Message(topic="test.topic", body={"my": "key"})
        expected_headers = json.dumps(
            msg._headers, sort_keys=True, indent=4, separators=(",", ": ")
        )
        expected = (
            "Id: {}\nTopic: test.topic\n"
            "Headers: {}"
            '\nBody: {{\n    "my": "key"\n}}'
        ).format(msg.id, expected_headers)
        self.assertEqual(expected, str(msg))

    def test_equality(self):
        """
        Assert two messages of the same class with the same topic, headers, and
        body are equivalent.
        """
        self.assertEqual(
            message.Message(topic="test.topic", body={"my": "key"}),
            message.Message(topic="test.topic", body={"my": "key"}),
        )

    def test_repr(self):
        """Assert the message produces a valid representation of the message."""
        msg = message.Message(topic="test.topic", body={"my": "key"})
        expected = "Message(id='{}', topic='test.topic', body={{'my': 'key'}})".format(
            msg.id
        )
        self.assertEqual(expected, repr(msg))

    def test_valid_message(self):
        """Assert that the default schema allows objects for the header and body."""
        message.Message(topic="test.topic", headers={}, body={}).validate()

    def test_invalid_message(self):
        """Assert that a non-object body raises a ValidationError on validation."""
        msg = message.Message(topic="test.topic", headers={}, body="text")
        self.assertRaises(jsonschema.ValidationError, msg.validate)

    def test_default_message(self):
        """Assert the default message is valid."""
        message.Message().validate()

    def test_properties_default(self):
        msg = message.Message()
        self.assertEqual(msg._properties.content_type, "application/json")
        self.assertEqual(msg._properties.content_encoding, "utf-8")
        self.assertEqual(msg._properties.delivery_mode, 2)
        self.assertIn("sent-at", msg._properties.headers)
        self.assertIn("fedora_messaging_schema", msg._properties.headers)
        self.assertEqual(
            msg._properties.headers["fedora_messaging_schema"],
            "fedora_messaging.message:Message",
        )

    def test_headers(self):
        msg = message.Message(headers={"foo": "bar"})
        self.assertIn("foo", msg._properties.headers)
        self.assertEqual(msg._properties.headers["foo"], "bar")
        # The fedora_messaging_schema key must also be added when headers are given.
        self.assertEqual(
            msg._properties.headers["fedora_messaging_schema"],
            "fedora_messaging.message:Message",
        )

    def test_severity_default_header_set(self):
        """Assert the default severity is placed in the header if unspecified."""
        self.assertEqual(message.Message.severity, message.INFO)
        msg = message.Message()
        self.assertEqual(msg._headers["fedora_messaging_severity"], message.INFO)

    def test_severity_custom_header_set(self):
        """Assert custom severity setting is placed in the header."""
        self.assertEqual(message.Message.severity, message.INFO)
        msg = message.Message(severity=message.ERROR)
        self.assertEqual(msg._headers["fedora_messaging_severity"], message.ERROR)

    def test_sent_at(self):
        """Assert a timestamp is inserted and contains explicit timezone information."""
        mock_datetime = mock.Mock()
        mock_datetime.utcnow.return_value = datetime.datetime(1970, 1, 1, 0, 0, 0)

        with mock.patch("datetime.datetime", mock_datetime):
            msg = message.Message()

        self.assertEqual("1970-01-01T00:00:00+00:00", msg._headers["sent-at"])

    def test_properties(self):
        properties = object()
        msg = message.Message(properties=properties)
        self.assertEqual(msg._properties, properties)

    def test_encoded_routing_key(self):
        """Assert encoded routing key is correct."""
        msg = message.Message(topic="test.topic")
        self.assertEqual(msg._encoded_routing_key, b"test.topic")

    def test_encoded_body(self):
        """Assert encoded body is correct."""
        body = {"foo": "barr\u00e9"}
        msg = message.Message(body=body)
        self.assertEqual(msg._encoded_body, json.dumps(body).encode("utf-8"))

    def test_url(self):
        # The url property must exist and defaults to None
        self.assertIsNone(message.Message().url)

    def test_app_icon(self):
        # The app_icon property must exist and defaults to None
        self.assertIsNone(message.Message().app_icon)

    def test_agent_avatar(self):
        # The agent_avatar property must exist and defaults to None
        self.assertIsNone(message.Message().agent_avatar)

    def test_usernames(self):
        # The usenames property must exist and be a list
        self.assertEqual(message.Message().usernames, [])

    def test_packages(self):
        # The packages property must exist and be a list
        self.assertEqual(message.Message().packages, [])


class ClassRegistryTests(unittest.TestCase):
    """Tests for the :func:`fedora_messaging.message.load_message_classes`."""

    def test_load_message(self):
        with mock.patch.dict(message._class_registry, {}, clear=True):
            message.load_message_classes()
            self.assertIn("fedora_messaging.message:Message", message._class_registry)
            self.assertTrue(
                message._class_registry["fedora_messaging.message:Message"]
                is message.Message
            )

    @mock.patch("fedora_messaging.message._registry_loaded", False)
    def test_get_class_autoload(self):
        """Assert the registry is automatically loaded."""
        with mock.patch.dict(message._class_registry, {}, clear=True):
            self.assertEqual(
                message.get_class("fedora_messaging.message:Message"), message.Message
            )

    @mock.patch("fedora_messaging.message._registry_loaded", False)
    def test_get_class_autoload_first_call(self):
        """Assert the registry loads classes on first call to get_class."""
        with mock.patch.dict(message._class_registry, {}, clear=True):
            self.assertEqual(
                message.get_class("fedora_messaging.message:Message"), message.Message
            )

    @mock.patch("fedora_messaging.message._registry_loaded", True)
    def test_get_class_autoload_once(self):
        """Assert the registry doesn't repeatedly load."""
        with mock.patch.dict(message._class_registry, {}, clear=True):
            self.assertRaises(
                KeyError, message.get_class, "fedora_messaging.message:Message"
            )
