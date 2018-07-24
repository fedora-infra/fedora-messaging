# Copyright (C) 2018  Red Hat, Inc.
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
"""Unit tests for the message schema."""
import unittest

from jsonschema import ValidationError
from .. import schema


class MailmanMessageTests(unittest.TestCase):
    """A set of unit tests to ensure the schema works as expected."""

    def test_minimal_message(self):
        """
        Assert the message schema validates a message with the minimal number
        of required fields.
        """
        minimal_message = {
            "mlist": {"list_name": "infrastructure"},
            "msg": {
                "from": "JD <jd@example.com>",
                "subject": "A sample email",
                "to": "infrastructure@lists.fedoraproject.org",
                "body": "hello world",
            }
        }
        message = schema.MailmanMessage(body=minimal_message)

        message.validate()

    def test_full_message(self):
        """Assert a message with all fields passes validation."""
        full_message = {
            "mlist": {"list_name": "infrastructure"},
            "msg": {
                "from": "Me <me@example.com>",
                'cc': 'them@example.com',
                'to': 'you@example.com',
                'delivered-to': 'someone@example.com',
                'x-mailman-rule-hits': '3',
                'x-mailman-rule-misses': '0',
                'x-message-id-hash': 'potatoes',
                'references': '<abc-123@example.com>',
                'in-reply-to': '<abc-123@example.com',
                'message-id': '12345',
                "subject": 'A sample email',
                'body': 'This is a good email',
            }
        }
        message = schema.MailmanMessage(body=full_message)

        message.validate()

    def test_missing_fields(self):
        """Assert an exception is actually raised on validation failure."""
        minimal_message = {
            "mlist": {"list_name": "infrastructure"},
            "msg": {
                "from": "JD <jd@example.com>",
            }
        }
        message = schema.MailmanMessage(body=minimal_message)
        self.assertRaises(ValidationError, message.validate)

    def test_str(self):
        """Assert __str__ produces a human-readable message."""
        body = {
            "mlist": {"list_name": "infrastructure"},
            "msg": {
                "from": "JD <jd@example.com>",
                "subject": "A sample email",
                "to": "infrastructure@lists.fedoraproject.org",
                "body": "hello world",
            }
        }
        expected_str = 'Subject: A sample email\n\nhello world\n'
        message = schema.MailmanMessage(body=body)

        message.validate()
        self.assertEqual(expected_str, str(message))

    def test_summary(self):
        """Assert the summary matches the message subject."""
        body = {
            "mlist": {"list_name": "infrastructure"},
            "msg": {
                "from": "JD <jd@example.com>",
                "subject": "A sample email",
                "to": "infrastructure@lists.fedoraproject.org",
                "body": "hello world",
            }
        }
        expected_summary = 'A sample email'
        message = schema.MailmanMessage(body=body)

        self.assertEqual(expected_summary, message.summary())
