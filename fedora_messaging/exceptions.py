"""Exceptions raised by Fedora Messaging."""


class BaseException(Exception):
    """The base class for all exceptions raised by fedora_messaging."""


class NoFreeChannels(BaseException):
    """Raised when a connection has reached its channel limit"""


class PermissionException(BaseException):
    """
    Generic permissions exception.

    Args:
        obj_type (str): The type of object being accessed that caused the
            permission error. May be None if the cause is unknown.
        description (object): The description of the object, if any. May be None.
        reason (str): The reason the server gave for the permission error, if
            any. If no reason is supplied by the server, this should be the best
            guess for what caused the error.
    """

    def __init__(self, obj_type=None, description=None, reason=None):
        self.obj_type = obj_type
        self.description = description
        self.reason = reason

    def __str__(self):
        return self.description

    def __repr__(self):
        return "PermissionException(obj_type={}, description={}, reason={})".format(
            self.obj_type, self.description, self.reason
        )


class BadDeclaration(PermissionException):
    """
    Raised when declaring an object in AMQP fails.

    Args:
        obj_type (str): The type of object being declared. One of "binding",
            "queue", or "exchange".
        description (dict): The description of the object.
        reason (str): The reason the server gave for rejecting the declaration.
    """

    def __str__(self):
        return "Unable to declare the {} object ({}) because {}".format(
            self.obj_type, self.description, self.reason
        )

    def __repr__(self):
        return "BadDeclaration(obj_type={}, description={}, reason={})".format(
            self.obj_type, self.description, self.reason
        )


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

    def __str__(self):
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


class ConnectionException(BaseException):
    """
    Raised if a general connection error occurred.

    You may handle this exception by logging it and resending or discarding the
    message.
    """

    def __init__(self, *args, **kwargs):
        super(ConnectionException, self).__init__(*args)
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
