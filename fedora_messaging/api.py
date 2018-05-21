"""The API for publishing messages and consuming from message queues."""

from ._session import ConsumerSession, PublisherSession

_session = None


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
    session = ConsumerSession()
    session.consume(callback, bindings)


def publish(message):
    """
    Publish a message to an exchange.

    This is a synchronous call, meaning that when this function returns, an
    acknowledgment has been received from the message broker and you can be
    certain the message was published successfully.

    Args:
        message (message.Message): The message to publish.

    Raises:
        Exception: Some useful exceptions

    # TODO should this accept a dict and wrap it in the default Message class?
    # TODO should this API accept arguments to override the config?
    # TODO doc retry behavior, when messages could get double published, etc.
    """
    # TODO make thread-local registry, probably
    global _session
    if _session is None:
        _session = PublisherSession()
    _session.publish(message)
