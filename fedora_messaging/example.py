# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

"""Example consumers that can be used when starting out with the library to test."""

from fedora_messaging.api import Message


def printer(message: Message) -> None:
    """
    A simple callback that prints the message to standard output.

    Usage: ``fedora-messaging consume --callback="fedora_messaging.example:printer"``

    Args:
        message: The message that was received.
    """
    print(str(message))
