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
import pika

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

        received_msg = message.get_message(
            msg._encoded_routing_key, msg._properties, msg._encoded_body
        )
        self.assertIsInstance(received_msg, message.Message)


class MessageDumpsTests(unittest.TestCase):
    """Tests for the :func:`fedora_messaging.message.dumps` function."""

    def test_proper_message(self):
        """Assert proper json is returned"""
        test_topic = "test topic"
        test_body = {"test_key": "test_value"}
        test_queue = "test queue"
        test_id = "test id"
        test_headers = {
            "fedora_messaging_schema": "base.message",
            "fedora_messaging_severity": message.WARNING,
        }
        test_properties = pika.BasicProperties(
            content_type="application/json",
            content_encoding="utf-8",
            delivery_mode=2,
            headers=test_headers,
            message_id=test_id,
        )
        test_msg = message.Message(
            body=test_body, topic=test_topic, properties=test_properties
        )

        test_msg.queue = test_queue
        expected_json = (
            '{"body": {"test_key": "test_value"}, "headers": {"fedora_messaging_schema": '
            '"base.message", "fedora_messaging_severity": 30}, "id": "test id", "queue": '
            '"test queue", "topic": "test topic"}\n'
        )
        self.assertEqual(expected_json, message.dumps(test_msg))

    def test_proper_message_multiple(self):
        """Assert proper json is returned"""
        test_topic = "test topic"
        test_body = {"test_key": "test_value"}
        test_queue = "test queue"
        test_id = "test id"
        test_headers = {
            "fedora_messaging_schema": "base.message",
            "fedora_messaging_severity": message.WARNING,
        }
        test_properties = pika.BasicProperties(
            content_type="application/json",
            content_encoding="utf-8",
            delivery_mode=2,
            headers=test_headers,
            message_id=test_id,
        )
        test_msg = message.Message(
            body=test_body, topic=test_topic, properties=test_properties
        )
        test_msg2 = message.Message(
            body=test_body, topic=test_topic, properties=test_properties
        )
        test_msg.queue = test_queue
        test_msg2.queue = test_queue
        expected_json = (
            '{"body": {"test_key": "test_value"}, "headers": {"fedora_messaging_schema": '
            '"base.message", "fedora_messaging_severity": 30}, "id": "test id", "queue": '
            '"test queue", "topic": "test topic"}\n'
            '{"body": {"test_key": "test_value"}, "headers": {"fedora_messaging_schema": '
            '"base.message", "fedora_messaging_severity": 30}, "id": "test id", "queue": '
            '"test queue", "topic": "test topic"}\n'
        )

        self.assertEqual(expected_json, message.dumps([test_msg, test_msg2]))

    def test_improper_messages(self):
        """Assert TypeError is raised when improper messages are provided"""
        messages = ["m1", "m2"]
        self.assertRaises(exceptions.ValidationError, message.dumps, messages)


class MessageLoadsTests(unittest.TestCase):
    """Tests for the :func:`fedora_messaging.message.loads` function."""

    def test_proper_json(self):
        """Assert loading single message from json work."""
        message_json = (
            '{"topic": "test topic", "headers": {"fedora_messaging_schema": "base.message", '
            '"fedora_messaging_severity": 30}, "id": "test id", "body": '
            '{"test_key": "test_value"}, "queue": "test queue"}\n'
        )
        messages = message.loads(message_json)
        test_message = messages[0]
        self.assertEqual(len(messages), 1)
        self.assertIsInstance(test_message, message.Message)
        self.assertEqual("test topic", test_message.topic)
        self.assertEqual("test id", test_message.id)
        self.assertEqual({"test_key": "test_value"}, test_message.body)
        self.assertEqual("test queue", test_message.queue)
        self.assertEqual(
            message.WARNING, test_message._headers["fedora_messaging_severity"]
        )
        self.assertEqual(
            "base.message", test_message._headers["fedora_messaging_schema"]
        )

    def test_improper_json(self):
        """Assert proper exception is raised when improper json is provided."""
        message_json = "improper json"
        self.assertRaises(exceptions.ValidationError, message.loads, message_json)

    def test_missing_headers(self):
        """Assert proper exception is raised when headers are missing."""
        message_json = (
            '[{"topic": "test topic", "id": "test id",'
            '"body": {"test_key": "test_value"}, "queue": "test queue"}]'
        )
        self.assertRaises(exceptions.ValidationError, message.loads, message_json)

    def test_missing_messaging_schema(self):
        """Assert proper exception is raised when messaging schema missing."""
        message_json = (
            '[{"topic": "test topic", "headers": {"fedora_messaging_severity": 30}, '
            '"id": "test id", "body": {"test_key": "test_value"}, "queue": "test queue"}]'
        )
        self.assertRaises(exceptions.ValidationError, message.loads, message_json)

    def test_missing_body(self):
        """Assert proper exception is raised when body is missing."""
        message_json = (
            '[{"topic": "test topic", "headers": {"fedora_messaging_schema": '
            '"base.message", "fedora_messaging_severity": 30}, "id": "test id", '
            '"queue": "test queue"}]'
        )
        self.assertRaises(exceptions.ValidationError, message.loads, message_json)

    def test_missing_id(self):
        """Assert proper exception is raised when id is missing."""
        message_json = (
            '[{"topic": "test topic", "headers": {"fedora_messaging_schema": '
            '"base.message", "fedora_messaging_severity": 30}, '
            '"body": {"test_key": "test_value"}, "queue": "test queue"}]'
        )
        self.assertRaises(exceptions.ValidationError, message.loads, message_json)

    def test_missing_queue(self):
        """Assert message without queue is returned and warning is printed."""
        message_json = (
            '{"topic": "test topic", "headers": {"fedora_messaging_schema": '
            '"base.message", "fedora_messaging_severity": 30}, "id": "test id", '
            '"body": {"test_key": "test_value"}}'
        )
        self.assertRaises(exceptions.ValidationError, message.loads, message_json)

    def test_missing_topic(self):
        """Assert proper exception is raised when topic is missing."""
        message_json = (
            '[{"headers": {"fedora_messaging_schema": "base.message", '
            '"fedora_messaging_severity": 30}, "id": "test id", '
            '"body": {"test_key": "test_value"}, "queue": "test queue"}]'
        )
        self.assertRaises(exceptions.ValidationError, message.loads, message_json)

    def test_missing_severity(self):
        """Assert proper exception is raised when topic severity missing."""
        message_json = (
            '[{"topic": "test topic", "headers": {"fedora_messaging_schema": '
            '"base.message"}, "id": "test id", "body": {"test_key": "test_value"},'
            '"queue": "test queue"}]'
        )
        self.assertRaises(exceptions.ValidationError, message.loads, message_json)

    def test_validation_failure(self):
        """Assert proper exception is raised when message validation failed."""
        message_json = (
            '[{"topic": "test topic", "headers": {"fedora_messaging_schema": "base.message", '
            '"fedora_messaging_severity": 41}, "id": "test id", '
            '"body": {"test_key": "test_value"}, "queue": "test queue"}]'
        )
        self.assertRaises(exceptions.ValidationError, message.loads, message_json)


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

    def test_equality_different_sent_at(self):
        """Assert the "sent-at" key is not included in the equality check."""
        m1 = message.Message(topic="test.topic", body={"my": "key"})
        m2 = message.Message(topic="test.topic", body={"my": "key"})
        m2._headers["sent-at"] = datetime.datetime(1970, 1, 2).isoformat()

        self.assertEqual(m1, m2)

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
            msg._properties.headers["fedora_messaging_schema"], "base.message"
        )

    def test_headers(self):
        msg = message.Message(headers={"foo": "bar"})
        self.assertIn("foo", msg._properties.headers)
        self.assertEqual(msg._properties.headers["foo"], "bar")
        # The fedora_messaging_schema key must also be added when headers are given.
        self.assertEqual(
            msg._properties.headers["fedora_messaging_schema"], "base.message"
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

    def test_containers(self):
        """The containers attribute must exist and be a list."""
        self.assertEqual(message.Message().containers, [])

    def test_modules(self):
        """The modules attribute must exist and be a list."""
        self.assertEqual(message.Message().modules, [])

    def test_flatpaks(self):
        """The flatpaks attribute must exist and be a list."""
        self.assertEqual(message.Message().flatpaks, [])

    @mock.patch("fedora_messaging.message.warnings")
    def test_body_deprecation_setter(self, mock_warning):
        """Assert a deprecation warning is printed on _body setter usage."""
        m = message.Message(body={})
        m._body["old"] = "interface"

        mock_warning.warn.assert_called_once_with(
            "The '_body' property has been renamed to 'body'.",
            DeprecationWarning,
            stacklevel=2,
        )

    @mock.patch("fedora_messaging.message.warnings")
    def test_body_deprecation_getter(self, mock_warning):
        """Assert a deprecation warning is printed on _body getter usage."""
        m = message.Message(body={"old": "interface"})
        self.assertEqual(m._body["old"], "interface")

        mock_warning.warn.assert_called_once_with(
            "The '_body' property has been renamed to 'body'.",
            DeprecationWarning,
            stacklevel=2,
        )

    def test_topic_prefix(self):
        """Assert the topic prefix is used in the encoded routing key."""
        with mock.patch.dict(message.config.conf, {"topic_prefix": "prefix"}):
            msg = message.Message(topic="test.topic")
            self.assertEqual(msg._encoded_routing_key, b"prefix.test.topic")


class CustomMessage(message.Message):
    """Test class that returns values for filter properties."""

    @property
    def usernames(self):
        try:
            return self.body["users"]
        except KeyError:
            return []

    @property
    def packages(self):
        try:
            return self.body["packages"]
        except KeyError:
            return []

    @property
    def containers(self):
        try:
            return self.body["containers"]
        except KeyError:
            return []
        pass

    @property
    def modules(self):
        try:
            return self.body["modules"]
        except KeyError:
            return []

    @property
    def flatpaks(self):
        try:
            return self.body["flatpaks"]
        except KeyError:
            return []


@mock.patch.dict(message._class_to_schema_name, {CustomMessage: "custom_id"})
class CustomMessageTests(unittest.TestCase):
    """Tests for a Message subclass that provides filter headers"""

    def test_usernames(self):
        """Assert usernames are placed in the message headers."""
        msg = CustomMessage(body={"users": ["jcline", "abompard"]})

        self.assertEqual(msg.usernames, ["jcline", "abompard"])
        self.assertIn("fedora_messaging_user_jcline", msg._headers)
        self.assertIn("fedora_messaging_user_abompard", msg._headers)

    def test_packages(self):
        """Assert RPM packages are placed in the message headers."""
        msg = CustomMessage(body={"packages": ["kernel", "python-requests"]})

        self.assertEqual(msg.packages, ["kernel", "python-requests"])
        self.assertIn("fedora_messaging_rpm_kernel", msg._headers)
        self.assertIn("fedora_messaging_rpm_python-requests", msg._headers)

    def test_containers(self):
        """Assert containers are placed in the message headers."""
        msg = CustomMessage(body={"containers": ["rawhide:latest", "f29"]})

        self.assertEqual(msg.containers, ["rawhide:latest", "f29"])
        self.assertIn("fedora_messaging_container_rawhide:latest", msg._headers)
        self.assertIn("fedora_messaging_container_f29", msg._headers)

    def test_modules(self):
        """Assert modules are placed in the message headers."""
        msg = CustomMessage(body={"modules": ["nodejs", "ripgrep"]})

        self.assertEqual(msg.modules, ["nodejs", "ripgrep"])
        self.assertIn("fedora_messaging_module_nodejs", msg._headers)
        self.assertIn("fedora_messaging_module_ripgrep", msg._headers)

    def test_flatpaks(self):
        """Assert flatpaks are placed in the message headers."""
        msg = CustomMessage(body={"flatpaks": ["firefox", "hexchat"]})

        self.assertEqual(msg.flatpaks, ["firefox", "hexchat"])
        self.assertIn("fedora_messaging_flatpak_firefox", msg._headers)
        self.assertIn("fedora_messaging_flatpak_hexchat", msg._headers)


class ClassRegistryTests(unittest.TestCase):
    """Tests for the :func:`fedora_messaging.message.load_message_classes`."""

    def test_load_message_name_to_class(self):
        """Assert the entry point name maps to the class object."""
        with mock.patch.dict(message._schema_name_to_class, {}, clear=True):
            message.load_message_classes()
            self.assertIn("base.message", message._schema_name_to_class)
            self.assertTrue(
                message._schema_name_to_class["base.message"] is message.Message
            )

    def test_load_message_class_to_name(self):
        """Assert the entry point name maps to the class object."""
        with mock.patch.dict(message._class_to_schema_name, {}, clear=True):
            message.load_message_classes()
            self.assertIn(message.Message, message._class_to_schema_name)
            self.assertEqual(
                "base.message", message._class_to_schema_name[message.Message]
            )

    @mock.patch("fedora_messaging.message._registry_loaded", False)
    def test_get_class_autoload(self):
        """Assert the registry is automatically loaded."""
        with mock.patch.dict(message._schema_name_to_class, {}, clear=True):
            self.assertEqual(message.get_class("base.message"), message.Message)

    @mock.patch("fedora_messaging.message._registry_loaded", True)
    def test_get_class_default(self):
        """Assert the base class is returns if the class is unknown."""
        with mock.patch.dict(message._schema_name_to_class, {}, clear=True):
            self.assertEqual(message.get_class("no.such.message"), message.Message)

    @mock.patch("fedora_messaging.message._registry_loaded", False)
    def test_get_name_autoload(self):
        """Assert the registry is automatically loaded."""
        with mock.patch.dict(message._class_to_schema_name, {}, clear=True):
            self.assertEqual(message.get_name(message.Message), "base.message")

    @mock.patch("fedora_messaging.message._registry_loaded", True)
    def test_get_name_autoload_once(self):
        """Assert the registry doesn't repeatedly load."""
        with mock.patch.dict(message._class_to_schema_name, {}, clear=True):
            self.assertRaises(TypeError, message.get_name, "this.is.not.an.entrypoint")
