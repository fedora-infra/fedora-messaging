# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

"""
Once you've written code to publish or consume messages, you'll probably want
to test it. The :mod:`fedora_messaging.testing` module has utilities for common
test patterns.

If you find yourself implementing a pattern over and over in your test code,
consider contributing it here!
"""

import inspect
from collections.abc import Generator
from contextlib import contextmanager
from typing import Union
from unittest import mock

from .message import Message


@contextmanager
def mock_sends(*expected_messages: Union[Message, type[Message]]) -> Generator[list[Message]]:
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

    sent: list[Message] = []
    with mock.patch("fedora_messaging.api.crochet"):
        with mock.patch("fedora_messaging.api._twisted_publish_wrapper") as mock_pub:
            yield sent

    messages = [call[0][0] for call in mock_pub.call_args_list]
    sent.extend(messages)
    if len(expected_messages) != len(messages):
        raise AssertionError(
            f"Expected {len(expected_messages)} messages to be sent, but {len(messages)} were sent"
        )
    for msg, expected in zip(messages, expected_messages):
        if inspect.isclass(expected):
            if not isinstance(msg, expected):
                raise AssertionError(
                    f"Expected message of type {expected}, but {msg.__class__} was sent"
                )
        else:
            assert msg.topic == expected.topic
            assert msg.body == expected.body
            assert msg == expected
        msg.validate()
