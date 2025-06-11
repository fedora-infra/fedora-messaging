# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

"""Tests for :mod:`fedora_messaging.example`."""

from io import StringIO
from unittest import mock

from fedora_messaging import api, example


def test_printer():
    """Assert the printer callback prints messages."""
    message = api.Message(body={"msg": "Hello world"}, topic="hi")
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
        "Body: {\n"
        '    "msg": "Hello world"\n'
        "}\n"
    )

    with mock.patch("sys.stdout", new_callable=StringIO) as mock_stdout:
        example.printer(message)

    assert expected_stdout == mock_stdout.getvalue()
