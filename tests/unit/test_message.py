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
from unittest import mock

import jsonschema
import pika
import pytest

from fedora_messaging import exceptions, message


class DeprecatedMessage(message.Message):
    deprecated = True


class TestGetMessage:
    """Tests for the :func:`fedora_messaging.message.get_message` function."""

    def test_missing_severity(self):
        """Assert the default severity is INFO if it's not in the headers."""
        msg = message.Message(severity=message.ERROR)
        del msg._headers["fedora_messaging_severity"]

        recv_msg = message.get_message("", msg._properties, b"{}")
        assert recv_msg.severity == message.INFO

    def test_invalid_severity(self):
        """Assert the invalid severity fails validation."""
        msg = message.Message()
        msg._headers["fedora_messaging_severity"] = 42

        with pytest.raises(exceptions.ValidationError):
            message.get_message("", msg._properties, b"{}")

    def test_missing_headers(self):
        """Assert missing headers results in a default message."""
        msg = message.Message()
        msg._headers = None

        received_msg = message.get_message(
            msg._encoded_routing_key, msg._properties, msg._encoded_body
        )
        assert isinstance(received_msg, message.Message)

    @mock.patch.dict(
        message._class_to_schema_name, {DeprecatedMessage: "deprecated_message_id"}
    )
    @mock.patch.dict(
        message._schema_name_to_class, {"deprecated_message_id": DeprecatedMessage}
    )
    def test_deprecated(self, caplog):
        """Assert a deprecation warning is produced when indicated."""
        msg = DeprecatedMessage(topic="dummy.topic")
        received_msg = message.get_message(
            msg.topic, msg._properties, msg._encoded_body
        )
        assert isinstance(received_msg, DeprecatedMessage)
        assert len(caplog.messages) == 1
        assert caplog.messages[0] == (
            "A message with a deprecated schema (tests.unit.test_message.DeprecatedMessage) "
            "has been received on topic 'dummy.topic'. You should check "
            "the emitting application's documentation to upgrade to the newer schema version."
        )


class TestMessageDumps:
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
            priority=2,
        )
        test_msg = message.Message(
            body=test_body, topic=test_topic, properties=test_properties
        )

        test_msg.queue = test_queue
        expected_json = (
            '{"body": {"test_key": "test_value"}, "headers": {"fedora_messaging_schema": '
            '"base.message", "fedora_messaging_severity": 30}, "id": "test id", '
            '"priority": 2, "queue": "test queue", "topic": "test topic"}\n'
        )
        assert expected_json == message.dumps(test_msg)

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
            '"base.message", "fedora_messaging_severity": 30}, "id": "test id", '
            '"priority": 0, "queue": "test queue", "topic": "test topic"}\n'
            '{"body": {"test_key": "test_value"}, "headers": {"fedora_messaging_schema": '
            '"base.message", "fedora_messaging_severity": 30}, "id": "test id", '
            '"priority": 0, "queue": "test queue", "topic": "test topic"}\n'
        )

        assert expected_json == message.dumps([test_msg, test_msg2])

    def test_improper_messages(self):
        """Assert TypeError is raised when improper messages are provided"""
        messages = ["m1", "m2"]
        with pytest.raises(exceptions.ValidationError):
            message.dumps(messages)


class TestMessageLoads:
    """Tests for the :func:`fedora_messaging.message.loads` function."""

    def test_proper_json(self):
        """Assert loading single message from json work."""
        message_json = (
            '{"topic": "test topic", "headers": {"fedora_messaging_schema": "base.message", '
            '"fedora_messaging_severity": 30}, "id": "test id", "body": '
            '{"test_key": "test_value"}, "priority": 2, "queue": "test queue"}\n'
        )
        messages = message.loads(message_json)
        assert len(messages) == 1
        test_message = messages[0]
        assert isinstance(test_message, message.Message)
        assert "test topic" == test_message.topic
        assert "test id" == test_message.id
        assert {"test_key": "test_value"} == test_message.body
        assert "test queue" == test_message.queue
        assert 2 == test_message.priority
        assert message.WARNING == test_message._headers["fedora_messaging_severity"]
        assert "base.message" == test_message._headers["fedora_messaging_schema"]

    def test_improper_json(self):
        """Assert proper exception is raised when improper json is provided."""
        message_json = "improper json"
        with pytest.raises(exceptions.ValidationError):
            message.loads(message_json)

    def test_missing_headers(self):
        """Assert no exception is raised when headers are missing."""
        message_dict = {
            "topic": "test topic",
            "id": "test id",
            "body": {"test_key": "test_value"},
            "queue": "test queue",
        }
        test_message = message.load_message(message_dict)
        assert test_message._headers["fedora_messaging_schema"] == "base.message"
        assert test_message._headers["fedora_messaging_severity"] == message.INFO
        assert "sent-at" in test_message._headers

    def test_missing_messaging_schema(self):
        """Assert the default schema is used when messaging schema is missing."""
        message_dict = {
            "id": "test id",
            "topic": "test topic",
            "headers": {"fedora_messaging_severity": 30},
            "body": {"test_key": "test_value"},
            "queue": "test queue",
        }
        test_message = message.load_message(message_dict)
        assert isinstance(test_message, message.Message)

    def test_missing_body(self):
        """Assert proper exception is raised when body is missing."""
        message_dict = {
            "id": "test id",
            "topic": "test topic",
            "headers": {
                "fedora_messaging_schema": "base.message",
                "fedora_messaging_severity": 30,
            },
            "queue": "test queue",
        }
        with pytest.raises(exceptions.ValidationError):
            message.load_message(message_dict)

    @mock.patch("fedora_messaging.message.uuid")
    def test_missing_id(self, uuid):
        """Assert proper exception is raised when id is missing."""
        uuid.uuid4.return_value = "dummy-uuid"
        message_dict = {
            "topic": "test topic",
            "headers": {
                "fedora_messaging_schema": "base.message",
                "fedora_messaging_severity": 30,
            },
            "body": {"test_key": "test_value"},
            "queue": "test queue",
        }
        test_message = message.load_message(message_dict)
        assert test_message.id == "dummy-uuid"

    def test_missing_queue(self):
        """Assert message without queue is accepted and the queue is set to None."""
        message_dict = {
            "id": "test id",
            "topic": "test topic",
            "headers": {
                "fedora_messaging_schema": "base.message",
                "fedora_messaging_severity": 30,
            },
            "body": {"test_key": "test_value"},
        }
        test_message = message.load_message(message_dict)
        assert test_message.queue is None

    def test_missing_topic(self):
        """Assert proper exception is raised when topic is missing."""
        message_dict = {
            "id": "test id",
            "headers": {
                "fedora_messaging_schema": "base.message",
                "fedora_messaging_severity": 30,
            },
            "body": {"test_key": "test_value"},
            "queue": "test queue",
        }
        with pytest.raises(exceptions.ValidationError):
            message.load_message(message_dict)

    def test_missing_severity(self):
        """Assert no exception is raised when severity is missing."""
        message_dict = {
            "topic": "test topic",
            "headers": {"fedora_messaging_schema": "base.message"},
            "id": "test id",
            "body": {"test_key": "test_value"},
            "queue": "test queue",
        }
        test_message = message.load_message(message_dict)
        assert test_message.severity == message.INFO

    def test_missing_priority(self):
        """Assert message without priority is accepted and the priority is set to zero."""
        message_dict = {
            "id": "test id",
            "topic": "test topic",
            "headers": {
                "fedora_messaging_schema": "base.message",
                "fedora_messaging_severity": 30,
            },
            "body": {"test_key": "test_value"},
            "queue": "test queue",
        }
        test_message = message.load_message(message_dict)
        assert test_message.priority == 0

    def test_validation_failure(self):
        """Assert proper exception is raised when message validation failed."""
        message_dict = {
            "id": "test id",
            "topic": "test topic",
            "headers": {
                "fedora_messaging_schema": "base.message",
                "fedora_messaging_severity": 41,
            },
            "body": {"test_key": "test_value"},
            "queue": "test queue",
        }
        with pytest.raises(exceptions.ValidationError):
            message.load_message(message_dict)


class TestMessage:
    """Tests for the :class:`fedora_messaging.message.Message` class."""

    def test_summary(self):
        """Assert message summaries default to the message topic."""
        msg = message.Message(topic="test.topic")
        assert msg.topic == msg.summary

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
        assert expected == str(msg)

    def test_equality(self):
        """
        Assert two messages of the same class with the same topic, headers, and
        body are equivalent.
        """
        assert message.Message(
            topic="test.topic", body={"my": "key"}
        ) == message.Message(topic="test.topic", body={"my": "key"})

    def test_equality_different_sent_at(self):
        """Assert the "sent-at" key is not included in the equality check."""
        m1 = message.Message(topic="test.topic", body={"my": "key"})
        m2 = message.Message(topic="test.topic", body={"my": "key"})
        m2._headers["sent-at"] = datetime.datetime(1970, 1, 2).isoformat()

        assert m1 == m2

    def test_repr(self):
        """Assert the message produces a valid representation of the message."""
        msg = message.Message(topic="test.topic", body={"my": "key"})
        expected = "Message(id='{}', topic='test.topic', body={{'my': 'key'}})".format(
            msg.id
        )
        assert expected == repr(msg)

    def test_valid_message(self):
        """Assert that the default schema allows objects for the header and body."""
        message.Message(topic="test.topic", headers={}, body={}).validate()

    def test_invalid_message(self):
        """Assert that a non-object body raises a ValidationError on validation."""
        msg = message.Message(topic="test.topic", headers={}, body="text")
        with pytest.raises(jsonschema.ValidationError):
            msg.validate()

    def test_default_message(self):
        """Assert the default message is valid."""
        message.Message().validate()

    def test_properties_default(self):
        msg = message.Message()
        assert msg._properties.content_type == "application/json"
        assert msg._properties.content_encoding == "utf-8"
        assert msg._properties.delivery_mode == 2
        assert "sent-at" in msg._properties.headers
        assert "fedora_messaging_schema" in msg._properties.headers
        assert msg._properties.headers["fedora_messaging_schema"] == "base.message"

    def test_headers(self):
        msg = message.Message(headers={"foo": "bar"})
        assert "foo" in msg._properties.headers
        assert msg._properties.headers["foo"] == "bar"
        # The fedora_messaging_schema key must also be added when headers are given.
        assert msg._properties.headers["fedora_messaging_schema"] == "base.message"

    def test_severity_default_header_set(self):
        """Assert the default severity is placed in the header if unspecified."""
        assert message.Message.severity == message.INFO
        msg = message.Message()
        assert msg._headers["fedora_messaging_severity"] == message.INFO

    def test_severity_custom_header_set(self):
        """Assert custom severity setting is placed in the header."""
        assert message.Message.severity == message.INFO
        msg = message.Message(severity=message.ERROR)
        assert msg._headers["fedora_messaging_severity"] == message.ERROR

    def test_sent_at(self):
        """Assert a timestamp is inserted and contains explicit timezone information."""
        mock_datetime = mock.Mock()
        mock_datetime.utcnow.return_value = datetime.datetime(1970, 1, 1, 0, 0, 0)

        with mock.patch("datetime.datetime", mock_datetime):
            msg = message.Message()

        assert "1970-01-01T00:00:00+00:00" == msg._headers["sent-at"]

    def test_priority(self):
        """Assert is set correctly."""
        msg = message.Message()
        assert 0 == msg.priority
        assert 0 == msg._headers["priority"]
        msg.priority = 42
        assert 42 == msg.priority
        assert 42 == msg._headers["priority"]
        msg.priority = None
        assert 0 == msg.priority
        assert 0 == msg._properties.priority
        assert 0 == msg._headers["priority"]

    def test_properties(self):
        properties = object()
        msg = message.Message(properties=properties)
        assert msg._properties == properties

    def test_encoded_routing_key(self):
        """Assert encoded routing key is correct."""
        msg = message.Message(topic="test.topic")
        assert msg._encoded_routing_key == b"test.topic"

    def test_encoded_body(self):
        """Assert encoded body is correct."""
        body = {"foo": "barr\u00e9"}
        msg = message.Message(body=body)
        assert msg._encoded_body == json.dumps(body).encode("utf-8")

    def test_url(self):
        # The url property must exist and defaults to None
        assert message.Message().url is None

    def test_app_name(self):
        # The app_name property must exist and defaults to None
        assert message.Message().app_name is None

    def test_app_icon(self):
        # The app_icon property must exist and defaults to None
        assert message.Message().app_icon is None

    def test_agent_name(self):
        # The agent_name property must exist and defaults to None
        assert message.Message().agent_name is None

    def test_agent_avatar(self):
        # The agent_avatar property must exist and defaults to None
        assert message.Message().agent_avatar is None

    def test_usernames(self):
        # The usenames property must exist and be a list
        assert message.Message().usernames == []

    def test_groups(self):
        # The groups property must exist and be a list
        assert message.Message().groups == []

    def test_packages(self):
        # The packages property must exist and be a list
        assert message.Message().packages == []

    def test_containers(self):
        """The containers attribute must exist and be a list."""
        assert message.Message().containers == []

    def test_modules(self):
        """The modules attribute must exist and be a list."""
        assert message.Message().modules == []

    def test_flatpaks(self):
        """The flatpaks attribute must exist and be a list."""
        assert message.Message().flatpaks == []

    def test_topic_prefix(self):
        """Assert the topic prefix is used in the encoded routing key."""
        with mock.patch.dict(message.config.conf, {"topic_prefix": "prefix"}):
            msg = message.Message(topic="test.topic")
            assert msg._encoded_routing_key == b"prefix.test.topic"


class CustomMessage(message.Message):
    """Test class that returns values for filter properties."""

    @property
    def usernames(self):
        try:
            return self.body["users"]
        except KeyError:
            return []

    @property
    def groups(self):
        try:
            return self.body["groups"]
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
class TestCustomMessage:
    """Tests for a Message subclass that provides filter headers"""

    def test_usernames(self):
        """Assert usernames are placed in the message headers."""
        msg = CustomMessage(body={"users": ["jcline", "abompard"]})

        assert msg.usernames == ["jcline", "abompard"]
        assert "fedora_messaging_user_jcline" in msg._headers
        assert "fedora_messaging_user_abompard" in msg._headers

    def test_groups(self):
        """Assert groups are placed in the message headers."""
        msg = CustomMessage(body={"groups": ["fedora-infra", "copr"]})

        assert msg.groups == ["fedora-infra", "copr"]
        assert "fedora_messaging_group_fedora-infra" in msg._headers
        assert "fedora_messaging_group_copr" in msg._headers

    def test_packages(self):
        """Assert RPM packages are placed in the message headers."""
        msg = CustomMessage(body={"packages": ["kernel", "python-requests"]})

        assert msg.packages == ["kernel", "python-requests"]
        assert "fedora_messaging_rpm_kernel" in msg._headers
        assert "fedora_messaging_rpm_python-requests" in msg._headers

    def test_containers(self):
        """Assert containers are placed in the message headers."""
        msg = CustomMessage(body={"containers": ["rawhide:latest", "f29"]})

        assert msg.containers == ["rawhide:latest", "f29"]
        assert "fedora_messaging_container_rawhide:latest" in msg._headers
        assert "fedora_messaging_container_f29" in msg._headers

    def test_modules(self):
        """Assert modules are placed in the message headers."""
        msg = CustomMessage(body={"modules": ["nodejs", "ripgrep"]})

        assert msg.modules == ["nodejs", "ripgrep"]
        assert "fedora_messaging_module_nodejs" in msg._headers
        assert "fedora_messaging_module_ripgrep" in msg._headers

    def test_flatpaks(self):
        """Assert flatpaks are placed in the message headers."""
        msg = CustomMessage(body={"flatpaks": ["firefox", "hexchat"]})

        assert msg.flatpaks == ["firefox", "hexchat"]
        assert "fedora_messaging_flatpak_firefox" in msg._headers
        assert "fedora_messaging_flatpak_hexchat" in msg._headers


class CustomValidatedMessage(message.Message):
    """Test class where filter properties depend on the validation."""

    body_schema = {
        "id": "http://fedoraproject.org/message-schema/custom-validated#",
        "$schema": "http://json-schema.org/draft-04/schema#",
        "type": "object",
        "properties": {
            "users": {
                "type": "array",
                "items": {
                    "type": "string",
                },
            },
        },
        "required": ["user"],
    }

    @property
    def usernames(self):
        return self.body["users"]


@mock.patch.dict(
    message._class_to_schema_name, {CustomValidatedMessage: "custom_validated_id"}
)
class TestCustomValidatedMessage:
    """Tests for CustomValidatedMessage"""

    def test_usernames(self):
        """Assert the instantiation works even if the message is invalid."""
        try:
            msg = CustomValidatedMessage(body={})
        except KeyError:
            pytest.fail(
                "Error in filter properties prevented the message from being instanciated."
            )
        with pytest.raises(jsonschema.ValidationError):
            msg.validate()


class TestClassRegistry:
    """Tests for the :func:`fedora_messaging.message.load_message_classes`."""

    def test_load_message_name_to_class(self):
        """Assert the entry point name maps to the class object."""
        with mock.patch.dict(message._schema_name_to_class, {}, clear=True):
            message.load_message_classes()
            assert "base.message" in message._schema_name_to_class
            assert message._schema_name_to_class["base.message"] is message.Message

    def test_load_message_class_to_name(self):
        """Assert the entry point name maps to the class object."""
        with mock.patch.dict(message._class_to_schema_name, {}, clear=True):
            message.load_message_classes()
            assert message.Message in message._class_to_schema_name
            assert "base.message" == message._class_to_schema_name[message.Message]

    @mock.patch("fedora_messaging.message._registry_loaded", False)
    def test_get_class_autoload(self):
        """Assert the registry is automatically loaded."""
        with mock.patch.dict(message._schema_name_to_class, {}, clear=True):
            assert message.get_class("base.message") == message.Message

    @mock.patch("fedora_messaging.message._registry_loaded", True)
    def test_get_class_default(self):
        """Assert the base class is returns if the class is unknown."""
        with mock.patch.dict(message._schema_name_to_class, {}, clear=True):
            assert message.get_class("no.such.message") == message.Message

    @mock.patch("fedora_messaging.message._registry_loaded", False)
    def test_get_name_autoload(self):
        """Assert the registry is automatically loaded."""
        with mock.patch.dict(message._class_to_schema_name, {}, clear=True):
            assert message.get_name(message.Message) == "base.message"

    @mock.patch("fedora_messaging.message._registry_loaded", True)
    def test_get_name_autoload_once(self):
        """Assert the registry doesn't repeatedly load."""
        with mock.patch.dict(message._class_to_schema_name, {}, clear=True):
            with pytest.raises(TypeError):
                message.get_name("this.is.not.an.entrypoint")
