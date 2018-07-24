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
from fedora_messaging import message


class MailmanMessage(message.Message):
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

    def __str__(self):
        """Return a complete human-readable representation of the message."""
        return 'Subject: {subj}\n\n{body}\n'.format(
            subj=self.body['msg']['subject'], body=self.body['msg']['body'])

    def summary(self):
        """Return a summary of the message."""
        return self.body['msg']['subject']
