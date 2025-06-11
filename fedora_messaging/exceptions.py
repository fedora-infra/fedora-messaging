# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

"""Exceptions raised by Fedora Messaging."""

from typing import Any, Optional, Union

import jsonschema
import jsonschema.exceptions
from pika.exceptions import AMQPError


class BaseException(Exception):
    """The base class for all exceptions raised by fedora_messaging."""


class NoFreeChannels(BaseException):
    """Raised when a connection has reached its channel limit"""


class PermissionException(BaseException):
    """
    Generic permissions exception.

    Args:
        obj_type: The type of object being accessed that caused the
            permission error. May be None if the cause is unknown.
        description: The description of the object, if any. May be None.
        reason: The reason the server gave for the permission error, if
            any. If no reason is supplied by the server, this should be the best
            guess for what caused the error.
    """

    def __init__(
        self,
        obj_type: Optional[str] = None,
        description: Optional[object] = None,
        reason: Optional[Union[str, AMQPError]] = None,
    ):
        self.obj_type = obj_type
        self.description = description
        self.reason = reason

    def __str__(self) -> str:
        return str(self.description)

    def __repr__(self) -> str:
        return (
            f"PermissionException(obj_type={self.obj_type}, description={self.description}, "
            f"reason={self.reason})"
        )


class BadDeclaration(PermissionException):
    """
    Raised when declaring an object in AMQP fails.

    Args:
        obj_type: The type of object being declared. One of "binding",
            "queue", or "exchange".
        description: The description of the object.
        reason: The reason the server gave for rejecting the declaration.
    """

    def __str__(self) -> str:
        return (
            f"Unable to declare the {self.obj_type} object ({self.description}) "
            f"because {self.reason}"
        )

    def __repr__(self) -> str:
        return (
            f"BadDeclaration(obj_type={self.obj_type}, description={self.description}, "
            f"reason={self.reason})"
        )


class ConfigurationException(BaseException):
    """
    Raised when there's an invalid configuration setting

    Args:
        message: A detailed description of the configuration problem
                       which is presented to the user.
    """

    def __init__(self, message: str):
        self.message = message

    def __str__(self) -> str:
        return "Configuration error: " + self.message


class PublishException(BaseException):
    """Base class for exceptions related to publishing."""

    def __init__(self, reason: Optional[Union[AMQPError, str]] = None, **kwargs: Any):
        super().__init__(**kwargs)
        self.reason = reason

    def __str__(self) -> str:
        return str(self.reason)


class PublishReturned(PublishException):
    """
    Raised when the broker rejects and returns the message to the publisher.

    You may handle this exception by logging it and resending or discarding the
    message.
    """


class PublishTimeout(PublishException):
    """
    Raised when the message could not be published in the given timeout.

    This means the message was either never delivered to the broker, or that it
    was delivered, but never acknowledged by the broker.
    """


class PublishForbidden(PublishException):
    """
    Raised when the broker rejects the message due to permission errors.

    You may handle this exception by logging it and discarding the message,
    as it is likely a permanent error.
    """


class ConnectionException(BaseException):
    """
    Raised if a general connection error occurred.

    You may handle this exception by logging it and resending or discarding the
    message.
    """

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args)
        self.reason = kwargs.get("reason")


class ConsumeException(BaseException):
    """Base class for exceptions related to consuming."""


class ConsumerCanceled(ConsumeException):
    """
    Raised when the server has canceled the consumer.

    This can happen when the queue the consumer is subscribed to is deleted,
    or when the node the queue is located on fails.
    """


class Nack(ConsumeException):
    """
    Consumer callbacks should raise this to indicate they wish the message they
    are currently processing to be re-queued.
    """


class Drop(ConsumeException):
    """
    Consumer callbacks should raise this to indicate they wish the message they
    are currently processing to be dropped.
    """


class HaltConsumer(ConsumeException):
    """
    Consumer callbacks should raise this exception if they wish the consumer to
    be shut down.

    Args:
        exit_code: The exit code to use when halting.
        reason: A reason for halting, presented to the user.
        requeue: If true, the message is re-queued for later processing.
    """

    def __init__(
        self,
        exit_code: int = 0,
        reason: Optional[str] = None,
        requeue: bool = False,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.exit_code = exit_code
        self.reason = reason
        self.requeue = requeue


class ValidationError(BaseException):
    """
    This error is raised when a message fails validation with its JSON schema

    This exception can be raised on an incoming or outgoing message. No need to
    catch this exception when publishing, it should warn you during development
    and testing that you're trying to publish a message with a different
    format, and that you should either fix it or update the schema.
    """

    @property
    def summary(self) -> str:
        """A short summary of the error."""
        original_exception = self.args[0]
        if isinstance(original_exception, jsonschema.exceptions.ValidationError):
            return original_exception.message
        return str(original_exception)
