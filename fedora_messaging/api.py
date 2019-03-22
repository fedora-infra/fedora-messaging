"""The API for publishing messages and consuming from message queues."""
from __future__ import absolute_import

import threading
import inspect

from twisted.internet import reactor

from . import _session, exceptions, config
from .signals import pre_publish_signal, publish_signal, publish_failed_signal
from .message import Message, SEVERITIES  # noqa: F401
from .twisted import service
from .twisted.consumer import Consumer  # noqa: F401


__all__ = (
    "Message",
    "consume",
    "publish",
    "twisted_consume",
    "Consumer",
    "pre_publish_signal",
    "publish_signal",
    "publish_failed_signal",
)

# Sessions aren't thread-safe, so each thread gets its own
_session_cache = threading.local()

# The Twisted service that consumers are registered with.
_twisted_service = None


def _check_callback(callback):
    """
    Turns a callback that is potentially a class into a callable object.

    Args:
        callback (object): An object that might be a class, method, or function.
        if the object is a class, this creates an instance of it.

    Raises:
        ValueError: If an instance can't be created or it isn't a callable object.
        TypeError: If the class requires arguments to be instantiated.

    Returns:
        callable: A callable object suitable for use as the consumer callback.
    """
    # If the callback is a class, create an instance of it first
    if inspect.isclass(callback):
        callback_object = callback()
        if not callable(callback_object):
            raise ValueError(
                "Callback must be a class that implements __call__ or a function."
            )
    elif callable(callback):
        callback_object = callback
    else:
        raise ValueError(
            "Callback must be a class that implements __call__ or a function."
        )

    return callback_object


def twisted_consume(callback, bindings=None, queues=None):
    """
    Start a consumer using the provided callback and run it using the Twisted
    event loop (reactor).

    .. note:: Callbacks run in a Twisted-managed thread pool using the
        :func:`twisted.internet.threads.deferToThread` API to avoid them blocking
        the event loop. If you wish to use Twisted APIs in your callback you must
        use the :func:`twisted.internet.threads.blockingCallFromThread` or
        :class:`twisted.internet.interfaces.IReactorFromThreads` APIs.

    This API expects the caller to start the reactor.

    Args:
        callback (callable): A callable object that accepts one positional argument,
            a :class:`.Message` or a class object that implements the ``__call__``
            method. The class will be instantiated before use.
        bindings (dict or list of dict): Bindings to declare before consuming. This
            should be the same format as the :ref:`conf-bindings` configuration.
        queues (dict): The queue to declare and consume from. Each key in this
            dictionary should be a queue name to declare, and each value should
            be a dictionary with the "durable", "auto_delete", "exclusive", and
            "arguments" keys.
    Returns:
        twisted.internet.defer.Deferred:
            A deferred that fires with the list of one or more
            :class:`.Consumer` objects. Each consumer object has a
            :attr:`.Consumer.result` instance variable that is a Deferred that
            fires or errors when the consumer halts. Note that this API is
            meant to survive network problems, so consuming will continue until
            :meth:`.Consumer.cancel` is called or a fatal server error occurs.
            The deferred returned by this function may error back with a
            :class:`fedora_messaging.exceptions.BadDeclaration` queues or
            bindings cannot be declared on the broker, or
            :class:`fedora_messaging.exceptions.ConnectionException` if the TLS
            or AMQP handshake fails.

    """
    if isinstance(bindings, dict):
        bindings = [bindings]
    callback = _check_callback(callback)

    global _twisted_service
    if _twisted_service is None:
        _twisted_service = service.FedoraMessagingServiceV2(config.conf["amqp_url"])
        reactor.callWhenRunning(_twisted_service.startService)
        # Twisted is killing the underlying connection before stopService gets
        # called, so we need to add it as a pre-shutdown event to gracefully
        # finish up messages in progress.
        reactor.addSystemEventTrigger(
            "before", "shutdown", _twisted_service.stopService
        )

    return _twisted_service._service.factory.consume(callback, bindings, queues)


def consume(callback, bindings=None, queues=None):
    """
    Start a message consumer that executes the provided callback when messages are
    received.

    This API is blocking and will not return until the process receives a signal
    from the operating system.

    .. warning:: This API is runs the callback in the IO loop thread. This means
        if your callback could run for a length of time near the heartbeat interval,
        which is likely on the order of 60 seconds, the broker will kill the TCP
        connection and the message will be re-delivered on start-up.

        For now, use the :func:`twisted_consume` API which runs the
        callback in a thread and continues to handle AMQP events while the
        callback runs if you have a long-running callback.

    The callback receives a single positional argument, the message:

    >>> from fedora_messaging import api
    >>> def my_callback(message):
    ...     print(message)
    >>> bindings = [{'exchange': 'amq.topic', 'queue': 'demo', 'routing_keys': ['#']}]
    >>> queues = {
    ...     "demo": {"durable": False, "auto_delete": True, "exclusive": True, "arguments": {}}
    ... }
    >>> api.consume(my_callback, bindings=bindings, queues=queues)

    If the bindings and queue arguments are not provided, they will be loaded from
    the configuration.

    For complete documentation on writing consumers, see the :ref:`consumers`
    documentation.

    Args:
        callback (callable): A callable object that accepts one positional argument,
            a :class:`Message` or a class object that implements the ``__call__``
            method. The class will be instantiated before use.
        bindings (dict or list of dict): Bindings to declare before consuming. This
            should be the same format as the :ref:`conf-bindings` configuration.
        queues (dict): The queue or queues to declare and consume from. This should be
            in the same format as the :ref:`conf-queues` configuration dictionary where
            each key is a queue name and each value is a dictionary of settings for that
            queue.

    Raises:
        fedora_messaging.exceptions.HaltConsumer: If the consumer requests that
            it be stopped.
        ValueError: If the consumer provide callback that is not a class that
            implements __call__ and is not a function, if the bindings argument
            is not a dict or list of dicts with the proper keys, or if the queues
            argument isn't a dict with the proper keys.
    """
    if isinstance(bindings, dict):
        bindings = [bindings]

    if bindings is None:
        bindings = config.conf["bindings"]
    else:
        try:
            config.validate_bindings(bindings)
        except exceptions.ConfigurationException as e:
            raise ValueError(e.message)

    if queues is None:
        queues = config.conf["queues"]
    else:
        try:
            config.validate_queues(queues)
        except exceptions.ConfigurationException as e:
            raise ValueError(e.message)

    session = _session.ConsumerSession()
    session.consume(callback, bindings=bindings, queues=queues)


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

    If an attempt to publish fails because the broker rejects the message, it
    is not retried. Connection attempts to the broker can be configured using
    the "connection_attempts" and "retry_delay" options in the broker URL. See
    :class:`pika.connection.URLParameters` for details.

    Args:
        message (message.Message): The message to publish.
        exchange (str): The name of the AMQP exchange to publish to; defaults to
            :ref:`conf-publish-exchange`

    Raises:
        fedora_messaging.exceptions.PublishReturned: Raised if the broker rejects the
            message.
        fedora_messaging.exceptions.ConnectionException: Raised if a connection error
            occurred before the publish confirmation arrived.
        fedora_messaging.exceptions.ValidationError: Raised if the message
            fails validation with its JSON schema. This only depends on the
            message you are trying to send, the AMQP server is not involved.
    """
    pre_publish_signal.send(publish, message=message)

    if exchange is None:
        exchange = config.conf["publish_exchange"]

    global _session_cache
    if not hasattr(_session_cache, "session"):
        _session_cache.session = _session.PublisherSession()

    try:
        _session_cache.session.publish(message, exchange=exchange)
        publish_signal.send(publish, message=message)
    except exceptions.PublishException as e:
        publish_failed_signal.send(publish, message=message, reason=e)
        raise
