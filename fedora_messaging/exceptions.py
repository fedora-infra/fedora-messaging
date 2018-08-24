"""Exceptions raised by Fedora Messaging."""


class BaseException(Exception):
    """The base class for all exceptions raised by fedora_messaging."""


class ConfigurationException(BaseException):
    """
    Raised when there's an invalid configuration setting

    Args:
        message (str): A detailed description of the configuration problem
                       which is presented to the user.
    """

    def __init__(self, message):
        self.message = message

    def __str__(self):
        return "Configuration error: " + self.message


class PublishException(BaseException):
    """Base class for exceptions related to publishing."""

    def __init__(self, reason=None, **kwargs):
        super(PublishException, self).__init__(**kwargs)
        self.reason = reason


class PublishReturned(PublishException):
    """
    Raised when the broker rejects and returns the message to the publisher.

    You may handle this exception by logging it and resending or discarding the
    message.
    """


class ConnectionException(BaseException):
    """
    Raised if a general connection error occurred.

    You may handle this exception by logging it and resending or discarding the
    message.
    """

    def __init__(self, reason=None, **kwargs):
        super(ConnectionException, self).__init__(**kwargs)
        self.reason = reason


class ConsumeException(BaseException):
    """Base class for exceptions related to consuming."""


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
        exit_code (int): The exit code to use when halting.
        reason (str): A reason for halting, presented to the user.
        requeue (bool): If true, the message is re-queued for later processing.
    """

    def __init__(self, exit_code=0, reason=None, requeue=False, **kwargs):
        super(HaltConsumer, self).__init__(**kwargs)
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
