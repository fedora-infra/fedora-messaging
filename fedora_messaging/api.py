"""The API for publishing messages and consuming from message queues."""

from . import _session
from .signals import pre_publish_signal, publish_signal, publish_failed_signal
from .message import Message


__all__ = (
    'Message',
    'consume',
    'publish',
    'pre_publish_signal',
    'publish_signal',
    'publish_failed_signal',
)

_session_cache = None


def consume(callback, bindings=None):
    """
    Start a message consumer that executes the provided callback when messages are
    received.

    This API is blocking and will not return until the process receives a signal
    from the operating system.

    The callback receives a single positional argument, the message:

    >>> from fedora_messaging import api
    >>> def my_callback(message):
    ...     print(message)
    >>> bindings = [{'exchange': 'amq.topic', 'queue': 'demo', 'routing_keys': ['#']}]
    >>> api.consume(my_callback, bindings=bindings)

    For complete documentation on writing consumers, see the :ref:`consumers`
    documentation.

    Args:
        callback (callable): A callable object that accepts one positional argument,
            a :class:`Message`.
        bindings (dict or list of dict): The bindings to use when consuming. This
            should be the same format as the :ref:`conf-bindings` configuration.
    """
    if isinstance(bindings, dict):
        bindings = [bindings]
    session = _session.ConsumerSession()
    session.consume(callback, bindings)


def publish(message, exchange=None):
    """
    Publish a message to an exchange.

    This is a synchronous call, meaning that when this function returns, an
    acknowledgment has been received from the message broker and you can be
    certain the message was published successfully.

    There are some cases where an error occurs despite your message being
    successfully published. For example, if a network partition occurs after
    the message is received by the broker. Therefore, you may publish duplicate
    messages. For complete details, see the :ref:`publishing` documentation.

    >>> from fedora_messaging import api
    >>> message = api.Message(body={'Hello': 'world'}, topic='Hi')
    >>> api.publish(message)

    Args:
        message (message.Message): The message to publish.
        exchange (str): The name of the AMQP exchange to publish to; defaults to
            :ref:`conf-publish-exchange`

    Raises:
        fedora_messaging.exceptions.PublishReturned: Raised if the broker rejects the
            message.
        fedora_messaging.exceptions.ConnectionError: Raised if a connection error
            occurred before the publish confirmation arrived.

    # TODO doc retry behavior, when messages could get double published, etc.
    """
    # TODO make thread-local registry, probably
    global _session_cache
    if _session_cache is None:
        _session_cache = _session.PublisherSession(exchange)
    _session_cache.publish(message, exchange=exchange)
