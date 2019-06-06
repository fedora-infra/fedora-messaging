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
"""
Once you've written code to publish or consume messages, you'll probably want
to test it. The :mod:`fedora_messaging.testing` module has utilities for common
test patterns.

If you find yourself implementing a pattern over and over in your test code,
consider contributing it here!
"""

from contextlib import contextmanager
import inspect

try:
    from unittest import mock
except ImportError:
    import mock


@contextmanager
def mock_sends(*expected_messages):
    """
    Assert a block of code results in the provided messages being sent without
    actually sending them.

    This is intended for unit tests. The call to publish is mocked out and messages
    are captured and checked at the end of the ``with``.

    For example:

        >>> from fedora_messaging import api, testing
        >>> def publishes():
        ...     api.publish(api.Message(body={"Hello": "world"}))
        ...
        >>> with testing.mock_sends(api.Message, api.Message(body={"Hello": "world"})):
        ...     publishes()
        ...     publishes()
        ...
        >>> with testing.mock_sends(api.Message(body={"Goodbye": "everybody"})):
        ...     publishes()
        ...
        AssertionError

    Args:
        *expected_messages: The messages you expect to be sent. These can be classes
            instances of classes derived from :class:`fedora_messaging.message.Message`.
            If the class is provided, the message is checked to make sure it is an
            instance of that class and that it passes schema validation. If an instance
            is provided, it is checked for equality with the sent message.

    Raises:
        AssertionError: If the messages published don't match the messages asserted.
    """
    with mock.patch("fedora_messaging.api.crochet"):
        with mock.patch("fedora_messaging.api._twisted_publish") as mock_pub:
            yield

    messages = [call[0][0] for call in mock_pub.call_args_list]
    if len(expected_messages) != len(messages):
        raise AssertionError(
            "Expected {} messages to be sent, but {} were sent".format(
                len(expected_messages), len(messages)
            )
        )
    for msg, expected in zip(messages, expected_messages):
        if inspect.isclass(expected):
            if not isinstance(msg, expected):
                raise AssertionError(
                    "Expected message of type {}, but {} was sent".format(
                        expected, msg.__class__
                    )
                )
        else:
            assert msg == expected
        msg.validate()
