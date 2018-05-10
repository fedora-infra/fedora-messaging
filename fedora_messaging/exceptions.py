"""Exceptions raised by Fedora Messaging."""


class BaseException(Exception):
    """The base class for all exceptions raised by fedora_messaging."""


class PublishException(BaseException):
    """Base class for exceptions related to publishing."""


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
    """
