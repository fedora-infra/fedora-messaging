"""The API for publishing messages and consuming from message queues."""
from __future__ import absolute_import

import inspect
import logging

from twisted.internet import reactor, defer
import crochet

from . import exceptions, config
from .signals import pre_publish_signal, publish_signal, publish_failed_signal
from .message import (
    Message,
    SEVERITIES,
    loads,
    dumps,
    SERIALIZED_MESSAGE_SCHEMA,
)  # noqa: F401
from .twisted import service
from .twisted.consumer import Consumer  # noqa: F401

_log = logging.getLogger(__name__)


__all__ = (
    "Message",
    "consume",
    "publish",
    "twisted_consume",
    "Consumer",
    "pre_publish_signal",
    "publish_signal",
    "publish_failed_signal",
    "loads",
    "dumps",
    "SERIALIZED_MESSAGE_SCHEMA",
    "SEVERITIES",
)

# The Twisted service that consumers are registered with.
_twisted_service = None


def _init_twisted_service():
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

    Raises:
        ValueError: If the callback, bindings, or queues are invalid.

    Returns:
        twisted.internet.defer.Deferred:
            A deferred that fires with the list of one or more
            :class:`.Consumer` objects. Each consumer object has a
            :attr:`.Consumer.result` instance variable that is a Deferred that
            fires or errors when the consumer halts. Note that this API is
            meant to survive network problems, so consuming will continue until
            :meth:`.Consumer.cancel` is called or a fatal server error occurs.
            The deferred returned by this function may error back with a
            :class:`fedora_messaging.exceptions.BadDeclaration` if queues or
            bindings cannot be declared on the broker, a
            :class:`fedora_messaging.exceptions.PermissionException` if the user
            doesn't have access to the queue, or
            :class:`fedora_messaging.exceptions.ConnectionException` if the TLS
            or AMQP handshake fails.
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

    callback = _check_callback(callback)

    _init_twisted_service()
    return _twisted_service._service.factory.consume(
        callback, bindings or config.conf["bindings"], queues or config.conf["queues"]
    )


@crochet.run_in_reactor
@defer.inlineCallbacks
def _twisted_consume_wrapper(callback, bindings, queues):
    """
    Wrap the :func:`twisted_consume` function for a synchronous API.

    Returns:
        defer.Deferred: Fires with the consumers once all consumers have halted
            or a consumer encounters an error.
    """
    consumers = yield twisted_consume(callback, bindings=bindings, queues=queues)
    try:
        yield defer.gatherResults([c.result for c in consumers])
    except defer.FirstError as e:
        e.subFailure.raiseException()
    defer.returnValue(consumers)


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
        ValueError: If the consumer provides a callback that is not a class that
            implements __call__ and is not a function, if the bindings argument
            is not a dict or list of dicts with the proper keys, or if the queues
            argument isn't a dict with the proper keys.
    """
    crochet.setup()
    eventual_result = _twisted_consume_wrapper(callback, bindings, queues)
    try:
        # Waiting indefinitely on crochet is deprecated, but this is a common value
        # used in the code base which is equivalent to 68 years, which I believe to
        # be an acceptably long amount of time. If you hit this limit you
        # should know that I'll feel no sympathy for you as I'll almost
        # certainly be dead.
        eventual_result.wait(timeout=2 ** 31)
    except (ValueError, exceptions.HaltConsumer):
        raise
    except Exception:
        # https://crochet.readthedocs.io/en/stable/workarounds.html#missing-tracebacks
        _log.error(
            "Consuming raised an unexpected error, please report a bug:\n%s",
            eventual_result.original_failure().getTraceback(),
        )
        raise


@crochet.run_in_reactor
@defer.inlineCallbacks
def _twisted_publish(message, exchange):
    """
    Wrapper to provide a synchronous API for publishing messages via Twisted.

    Returns:
        defer.Deferred: A deferred that fires when a message has been published
            and confirmed by the broker.
    """
    _init_twisted_service()
    try:
        yield _twisted_service._service.factory.publish(message, exchange=exchange)
    except defer.CancelledError:
        _log.debug("Canceled publish of %r to %s due to timeout", message, exchange)


def publish(message, exchange=None, timeout=30):
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
        timeout (int): The maximum time in seconds to wait before giving up attempting
            to publish the message. If the timeout is reached, a PublishTimeout exception
            is raised.

    Raises:
        fedora_messaging.exceptions.PublishReturned: Raised if the broker rejects the
            message.
        fedora_messaging.exceptions.PublishTimeout: Raised if the broker could not be
            contacted in the given timeout time.
        fedora_messaging.exceptions.ValidationError: Raised if the message
            fails validation with its JSON schema. This only depends on the
            message you are trying to send, the AMQP server is not involved.
    """
    crochet.setup()
    pre_publish_signal.send(publish, message=message)

    if exchange is None:
        exchange = config.conf["publish_exchange"]

    eventual_result = _twisted_publish(message, exchange)
    try:
        eventual_result.wait(timeout=timeout)
        publish_signal.send(publish, message=message)
    except crochet.TimeoutError:
        eventual_result.cancel()
        wrapper = exceptions.PublishTimeout(
            "Publishing timed out after waiting {} seconds.".format(timeout)
        )
        publish_failed_signal.send(publish, message=message, reason=wrapper)
        raise wrapper
    except Exception as e:
        _log.error(eventual_result.original_failure().getTraceback())
        publish_failed_signal.send(publish, message=message, reason=e)
        raise
