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
"""This is an example of a message schema."""

from fedora_messaging import message


class BaseMessage(message.Message):
    """
    You should create a super class that each schema version inherits from.
    This lets consumers perform ``isinstance(msg, BaseMessage)`` if they are
    receiving multiple message types and allows the publisher to change the
    schema as long as they preserve the Python API.
    """

    def __str__(self):
        """Return a complete human-readable representation of the message."""
        return 'Subject: {subj}\n{body}\n'.format(
            subj=self.subject, body=self.body)

    def summary(self):
        """Return a summary of the message."""
        return self.subject

    @property
    def subject(self):
        """The email's subject."""
        return 'Message did not implement "subject" property'

    @property
    def body(self):
        """The email message body."""
        return 'Message did not implement "body" property'


class MessageV1(BaseMessage):
    """
    A sub-class of a Fedora message that defines a message schema for messages
    published by Mailman when it receives mail to send out.
    """
    body_schema = {
        'id': 'http://fedoraproject.org/message-schema/mailman#',
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'description': 'Schema for message sent to mailman',
        'type': 'object',
        'properties': {
            'mlist': {
                'type': 'object',
                'properties': {
                    'list_name': {
                        'type': 'string',
                        'description': 'The name of the mailing list',
                    },
                }
            },
            'msg': {
                'description': 'An object representing the email',
                'type': 'object',
                'properties': {
                    'delivered-to': {'type': 'string'},
                    'from': {'type': 'string'},
                    'cc': {'type': 'string'},
                    'to': {'type': 'string'},
                    'x-mailman-rule-hits': {'type': 'string'},
                    'x-mailman-rule-misses': {'type': 'string'},
                    'x-message-id-hash': {'type': 'string'},
                    'references': {'type': 'string'},
                    'in-reply-to': {'type': 'string'},
                    'message-id': {'type': 'string'},
                    'subject': {'type': 'string'},
                    'body': {'type': 'string'},
                },
                'required': ['from', 'to', 'subject', 'body'],
            },
        },
        'required': ['mlist', 'msg'],
    }

    @property
    def subject(self):
        """The email's subject."""
        return self._body['msg']['subject']

    @property
    def body(self):
        """The email message body."""
        return self._body['msg']['body']


class MessageV2(BaseMessage):
    """
    This is a revision from the MessageV1 schema which flattens the message
    structure into a single object, but is backwards compatible for any users
    that make use of the properties (``subject`` and ``body``).
    """

    body_schema = {
        'id': 'http://fedoraproject.org/message-schema/mailman#',
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'description': 'Schema for message sent to mailman',
        'type': 'object',
        'required': ['mailing_list', 'from', 'to', 'subject', 'body'],
        'properties': {
            'mailing_list': {
                    'type': 'string',
                    'description': 'The name of the mailing list',
            },
            'delivered-to': {'type': 'string'},
            'from': {'type': 'string'},
            'cc': {'type': 'string'},
            'to': {'type': 'string'},
            'x-mailman-rule-hits': {'type': 'string'},
            'x-mailman-rule-misses': {'type': 'string'},
            'x-message-id-hash': {'type': 'string'},
            'references': {'type': 'string'},
            'in-reply-to': {'type': 'string'},
            'message-id': {'type': 'string'},
            'subject': {'type': 'string'},
            'body': {'type': 'string'},
        },
    }

    @property
    def subject(self):
        """The email's subject."""
        return self._body['subject']

    @property
    def body(self):
        """The email message body."""
        return self._body['body']
