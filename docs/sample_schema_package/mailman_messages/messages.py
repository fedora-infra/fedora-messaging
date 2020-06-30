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

from email.utils import parseaddr

from fedora_messaging import message, schema_utils


class BaseMessage(message.Message):
    """
    You should create a super class that each schema version inherits from.
    This lets consumers perform ``isinstance(msg, BaseMessage)`` if they are
    receiving multiple message types and allows the publisher to change the
    schema as long as they preserve the Python API.
    """

    def __str__(self):
        """Return a complete human-readable representation of the message."""
        return "Subject: {subj}\n{body}\n".format(
            subj=self.subject, body=self.email_body
        )

    @property
    def summary(self):
        """Return a summary of the message.

        By convention, in Fedora all schemas should provide this property.
        """
        return self.subject

    @property
    def subject(self):
        """The email's subject."""
        return 'Message did not implement "subject" property'

    @property
    def email_body(self):
        """The email message body."""
        return 'Message did not implement "email_body" property'

    @property
    def url(self):
        """An URL to the email in HyperKitty

        By convention, in Fedora all schemas should provide this property.

        Returns:
            str or None: A relevant URL.
        """
        base_url = "https://lists.fedoraproject.org/archives"
        archived_at = self._get_archived_at()
        if archived_at and archived_at.startswith("<"):
            archived_at = archived_at[1:]
        if archived_at and archived_at.endswith(">"):
            archived_at = archived_at[:-1]
        if archived_at and archived_at.startswith("http"):
            return archived_at
        elif archived_at:
            return base_url + archived_at
        else:
            return None

    @property
    def app_icon(self):
        """A URL to the icon of the application that generated the message.

        By convention, in Fedora all schemas should provide this property.
        """
        return "https://apps.fedoraproject.org/img/icons/hyperkitty.png"

    @property
    def usernames(self):
        """List of users affected by the action that generated this message."""
        return []

    @property
    def packages(self):
        """List of packages affected by the action that generated this message."""
        return []

    def _get_avatar_from_from_header(self, from_header):
        """Converts a From email header to an avatar."""
        # Extract the username
        addr = parseaddr(from_header)[1]
        username = addr.split("@")[0]
        return schema_utils.user_avatar_url(username)


class MessageV1(BaseMessage):
    """
    A sub-class of a Fedora message that defines a message schema for messages
    published by Mailman when it receives mail to send out.
    """

    body_schema = {
        "id": "http://fedoraproject.org/message-schema/mailman#",
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "Schema for message sent to mailman",
        "type": "object",
        "properties": {
            "mlist": {
                "type": "object",
                "properties": {
                    "list_name": {
                        "type": "string",
                        "description": "The name of the mailing list",
                    }
                },
            },
            "msg": {
                "description": "An object representing the email",
                "type": "object",
                "properties": {
                    "delivered-to": {"type": "string"},
                    "from": {"type": "string"},
                    "cc": {"type": "string"},
                    "to": {"type": "string"},
                    "x-mailman-rule-hits": {"type": "string"},
                    "x-mailman-rule-misses": {"type": "string"},
                    "x-message-id-hash": {"type": "string"},
                    "references": {"type": "string"},
                    "in-reply-to": {"type": "string"},
                    "message-id": {"type": "string"},
                    "archived-at": {"type": "string"},
                    "subject": {"type": "string"},
                    "body": {"type": "string"},
                },
                "required": ["from", "to", "subject", "body"],
            },
        },
        "required": ["mlist", "msg"],
    }

    @property
    def subject(self):
        """The email's subject."""
        return self.body["msg"]["subject"]

    @property
    def email_body(self):
        """The email message body."""
        return self.body["msg"]["body"]

    @property
    def agent_avatar(self):
        """An URL to the avatar of the user who caused the action."""
        return self._get_avatar_from_from_header(self.body["msg"]["from"])

    def _get_archived_at(self):
        return self.body["msg"]["archived-at"]


class MessageV2(BaseMessage):
    """
    This is a revision from the MessageV1 schema which flattens the message
    structure into a single object, but is backwards compatible for any users
    that make use of the properties (``subject`` and ``body``).
    """

    body_schema = {
        "id": "http://fedoraproject.org/message-schema/mailman#",
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "Schema for message sent to mailman",
        "type": "object",
        "required": ["mailing_list", "from", "to", "subject", "body"],
        "properties": {
            "mailing_list": {
                "type": "string",
                "description": "The name of the mailing list",
            },
            "delivered-to": {"type": "string"},
            "from": {"type": "string"},
            "cc": {"type": "string"},
            "to": {"type": "string"},
            "x-mailman-rule-hits": {"type": "string"},
            "x-mailman-rule-misses": {"type": "string"},
            "x-message-id-hash": {"type": "string"},
            "references": {"type": "string"},
            "in-reply-to": {"type": "string"},
            "message-id": {"type": "string"},
            "archived-at": {"type": "string"},
            "subject": {"type": "string"},
            "body": {"type": "string"},
        },
    }

    @property
    def subject(self):
        """The email's subject."""
        return self.body["subject"]

    @property
    def email_body(self):
        """The email message body."""
        return self.body["body"]

    @property
    def agent_avatar(self):
        """An URL to the avatar of the user who caused the action."""
        return self._get_avatar_from_from_header(self.body["from"])

    def _get_archived_at(self):
        return self.body["archived-at"]
