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

import unittest

import jsonschema
import mock

from fedora_messaging import message
from fedora_messaging.message import Message


class MessageTests(unittest.TestCase):
    """Tests for the :mod:`fedora_messaging.message` module."""

    def test_summary(self):
        """Assert message summaries default to the message topic."""
        msg = message.Message(topic='test.topic')
        self.assertEqual(msg.topic, msg.summary())

    def test_str(self):
        """Assert calling str on a message produces a human-readable result."""
        expected = 'Topic: test.topic\n\nHeaders: {}\n\nBody: {\n    "my": "key"\n}'
        msg = message.Message(topic='test.topic', body={'my': 'key'})
        self.assertEqual(expected, str(msg))

    def test_equality(self):
        """
        Assert two messages of the same class with the same topic, headers, and
        body are equivalent.
        """
        self.assertEqual(message.Message(topic='test.topic', body={'my': 'key'}),
                         message.Message(topic='test.topic', body={'my': 'key'}))

    def test_repr(self):
        """Assert the message produces a valid representation of the message."""
        expected = "Message(body={'my': 'key'}, headers={}, topic='test.topic')"
        msg = message.Message(topic='test.topic', body={'my': 'key'})
        self.assertEqual(expected, repr(msg))
        self.assertEqual(msg, eval(repr(msg)))

    def test_valid_message(self):
        """Assert that the default schema allows objects for the header and body."""
        message.Message(topic='test.topic', headers={}, body={}).validate()

    def test_invalid_message(self):
        """Assert that a non-object body raises a ValidationError on validation."""
        msg = message.Message(topic='test.topic', headers={}, body='text')
        self.assertRaises(jsonschema.ValidationError, msg.validate)

    def test_default_message(self):
        """Assert the default message is valid."""
        message.Message().validate()




class ClassRegistryTests(unittest.TestCase):
    """Tests for the :func:`fedora_messaging.message.load_message_classes`."""

    def test_load_message(self):
        with mock.patch.dict(message._class_registry, {}, clear=True):
            message.load_message_classes()
            self.assertIn('fedora_messaging.message:Message', message._class_registry)
            self.assertTrue(
            message._class_registry['fedora_messaging.message:Message'] is message.Message)

    @mock.patch('fedora_messaging.message._registry_loaded', False)
    def test_get_class_autoload(self):
        """Assert the registry is automatically loaded."""
        with mock.patch.dict(message._class_registry, {}, clear=True):
            self.assertEqual(
                message.get_class('fedora_messaging.message:Message'), message.Message)

    @mock.patch('fedora_messaging.message._registry_loaded', False)
    def test_get_class_autoload_first_call(self):
        """Assert the registry loads classes on first call to get_class."""
        with mock.patch.dict(message._class_registry, {}, clear=True):
            self.assertEqual(
                message.get_class('fedora_messaging.message:Message'), message.Message)

    @mock.patch('fedora_messaging.message._registry_loaded', True)
    def test_get_class_autoload_once(self):
        """Assert the registry doesn't repeatedly load."""
        with mock.patch.dict(message._class_registry, {}, clear=True):
            self.assertRaises(
                KeyError, message.get_class, 'fedora_messaging.message:Message')
