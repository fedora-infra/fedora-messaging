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
"""Tests for :mod:`fedora_messaging.example`."""

from io import StringIO
from unittest import mock

from fedora_messaging import api, example


def test_printer():
    """Assert the printer callback prints messages."""
    message = api.Message(body="Hello world", topic="hi")
    message._headers = {
        "fedora_messaging_schema": "fedora_messaging.message:Message",
        "sent-at": "2019-07-30T19:12:22+00:00",
    }
    message.id = "95383db8-8cdc-4464-8276-d482ac28b0b6"
    expected_stdout = (
        "Id: 95383db8-8cdc-4464-8276-d482ac28b0b6\n"
        "Topic: hi\n"
        "Headers: {\n"
        '    "fedora_messaging_schema": "fedora_messaging.message:Message",\n'
        '    "sent-at": "2019-07-30T19:12:22+00:00"\n'
        "}\n"
        'Body: "Hello world"\n'
    )

    with mock.patch("sys.stdout", new_callable=StringIO) as mock_stdout:
        example.printer(message)

    assert expected_stdout == mock_stdout.getvalue()
