# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

"""
The ``fedora-messaging`` `Click`_ CLI.

.. _Click: http://click.pocoo.org/
"""

import errno
import importlib
import logging
import logging.config
import os
import sys
from collections.abc import Sequence
from typing import Any, cast, IO, TYPE_CHECKING

import click
import requests
from twisted.internet import asyncioreactor, error
from twisted.python.failure import Failure

from fedora_messaging import message


try:
    asyncioreactor.install()
except error.ReactorAlreadyInstalledError:
    # The tests install a reactor before importing this module
    from twisted.internet import reactor

    if not isinstance(reactor, asyncioreactor.AsyncioSelectorReactor):
        raise

from twisted.internet import reactor
from twisted.python import log as legacy_twisted_log

from . import api, config, exceptions
from .message import dumps, loads


if TYPE_CHECKING:
    from click._termui_impl import ProgressBar

    from .twisted.consumer import Consumer


_log = logging.getLogger(__name__)

_conf_help = (
    "Path to a valid configuration file to use in place of the "
    "configuration in /etc/fedora-messaging/config.toml."
)
_app_name_help = (
    "The name of the application, used by the AMQP client to identify itself to "
    "the broker. This is purely for administrator convenience to determine what "
    "applications are connected and own particular resources."
)
_callback_help = (
    "The Python path to the callable object to execute when a message arrives. "
    "The Python path should be in the format ``module.path:object_in_module`` "
    "and should point to either a function or a class. Consult the API "
    "documentation for the interface required for these objects."
)
_callback_file_help = (
    "The path of a Python file that contains the callable object to"
    " execute when the message arrives. This should be in the format "
    '"my/script.py:object_in_file". Providing this overrides the callback set '
    'by the "--callback" argument or configuration file. It executes the entire '
    "Python file to load the callable object."
)
_routing_key_help = (
    "The AMQP routing key to use with the queue. This controls what messages are "
    "delivered to the consumer. Can be specified multiple times; any message "
    "that matches at least one will be placed in the message queue. "
)
_queue_name_help = (
    "The name of the message queue in AMQP. Can contain ASCII letters, digits, "
    "hyphen, underscore, period, or colon. If one is not specified, a unique "
    "name will be created for you."
)
_exchange_help = (
    "The name of the exchange to bind the queue to. Can contain ASCII letters, "
    "digits, hyphen, underscore, period, or colon."
)
_publish_exchange_help = (
    "The name of the exchange to publish to. Can contain ASCII letters, "
    "digits, hyphen, underscore, period, or colon."
)
_datagrepper_help = (
    "The URL of the datagreeper instance to use, " "defaults to the production environment"
)
_limit_help = "The maximum number of messages to record."


# Global variable used to set the exit code in error handlers, then let
# the reactor shut down gracefully, then exit with the proper code.
_exit_code = 0


@click.group()
@click.option("--conf", envvar="FEDORA_MESSAGING_CONF", help=_conf_help)
def cli(conf: str) -> None:
    """The fedora-messaging command line interface."""
    if conf:
        if not os.path.isfile(conf):
            raise click.exceptions.BadParameter(f"{conf} is not a file")
        try:
            config.conf.load_config(config_path=conf)
        except exceptions.ConfigurationException as e:
            raise click.exceptions.BadParameter(str(e)) from e
    twisted_observer = legacy_twisted_log.PythonLoggingObserver()
    twisted_observer.start()
    config.conf.setup_logging()


@cli.command()
@click.option("--app-name", help=_app_name_help)
@click.option("--callback-file", help=_callback_file_help)
@click.option("--callback", help=_callback_help)
@click.option("--routing-key", help=_routing_key_help, multiple=True)
@click.option("--queue-name", help=_queue_name_help)
@click.option("--exchange", help=_exchange_help)
def consume(
    exchange: str,
    queue_name: str,
    routing_key: list[str],
    callback: str,
    callback_file: str,
    app_name: str,
) -> None:
    """Consume messages from an AMQP queue using a Python callback."""
    # The configuration validates these are not null and contain all required keys
    # when it is loaded.
    if callback_file:
        python_callback = _callback_from_filesystem(callback_file)
    else:
        python_callback = _callback_from_python_path(callback)
    _consume(exchange, queue_name, routing_key, python_callback, app_name)


def _consume(
    exchange: str,
    queue_name: str,
    routing_key: list[str],
    callback: config.CallbackType,
    app_name: str,
) -> None:
    """
    The actual consume code, which expects an actual callable object.
    This lets various consume-based commands share the setup code. Anything
    that accepts None loads the defaults from the configuration.

    Args:
        exchange: The AMQP message exchange to bind to, or None.
        queue_name: The queue name to use, or None.
        routing_key: The routing key to use, or None.
        callback: A callable object to use for the callback.
        app_name: The application name to use, or None.
    """
    bindings: list[config.BindingConfig] = config.conf["bindings"]
    queues: dict[str, config.QueueConfig] = config.conf["queues"]

    if queue_name:
        queues = {queue_name: config.conf["queues"][config._default_queue_name]}
        for binding in bindings:
            binding["queue"] = queue_name

    if exchange:
        for binding in bindings:
            binding["exchange"] = exchange

    if routing_key:
        for binding in bindings:
            binding["routing_keys"] = routing_key

    if app_name:
        config.conf["client_properties"]["app"] = app_name

    try:
        deferred_consumers = api.twisted_consume(callback, bindings=bindings, queues=queues)
        deferred_consumers.addCallback(_consume_callback)
        deferred_consumers.addErrback(_consume_errback)
    except ValueError as e:
        raise click.exceptions.BadOptionUsage("callback", str(e)) from e

    # https://github.com/twisted/twisted/issues/9909
    cast(asyncioreactor.AsyncioSelectorReactor, reactor).run()
    sys.exit(_exit_code)


def _callback_from_filesystem(callback_file: str) -> config.CallbackType:
    """
    Load a callable from a Python script on the file system.

    Args:
        callback_file: The callback as a filesystem path and callable name
            separated by a ":". For example, "my/python/file.py:printer".

    Raises:
        click.ClickException: If the object cannot be loaded.

    Returns:
        The callable object.
    """
    try:
        file_path, callable_name = callback_file.strip().split(":")
    except ValueError as e:
        raise click.ClickException(
            "Unable to parse the '--callback-file' option; the "
            'expected format is "path/to/file.py:callable_object" where '
            '"callable_object" is the name of the function or class in the '
            "Python file"
        ) from e

    try:
        file_namespace: dict[str, Any] = {}
        with open(file_path, "rb") as fd:
            try:
                # Using "exec" is generally a Bad Idea (TM), so bandit is upset at
                # us. In this case, it seems like a Good Idea (TM), but I might be
                # wrong. Sorry.
                exec(compile(fd.read(), file_path, "exec"), file_namespace)  # noqa: S102
            except Exception as e:
                raise click.ClickException(
                    f"The {file_path} file raised the following exception during execution: {e}"
                ) from e

        if callable_name not in file_namespace:
            err = f"The '{callable_name}' object was not found in the '{file_path}' file."
            raise click.ClickException(err)
        else:
            callback_obj: config.CallbackType = file_namespace[callable_name]
            return callback_obj
    except OSError as e:
        raise click.ClickException(f"An IO error occurred: {e}") from e


def _callback_from_python_path(callback: str) -> config.CallbackType:
    """
    Load a callable from a Python path.

    Args:
        callback_file: The callback as a Python path and callable name
            separated by a ":". For example, "my_package.my_module:printer".

    Raises:
        click.ClickException: If the object cannot be loaded.

    Returns:
        The callable object.
    """
    callback_path = callback or config.conf["callback"]
    if not callback_path:
        raise click.ClickException(
            "A Python path to a callable object that accepts the message must be provided"
            ' with the "--callback" command line option or in the configuration file'
        )
    try:
        module, cls = callback_path.strip().split(":")
    except ValueError as e:
        raise click.ClickException(
            f"Unable to parse the callback path ({callback_path}); the "
            'expected format is "my_package.module:'
            'callable_object"'
        ) from e
    try:
        module = importlib.import_module(module)
    except ImportError as e:
        provider = "--callback argument" if callback else "configuration file"
        raise click.ClickException(
            f"Failed to import the callback module ({e}) provided in the {provider}"
        ) from e

    try:
        callback_object: config.CallbackType = getattr(module, cls)
    except AttributeError as e:
        raise click.ClickException(
            f"Unable to import {callback_path} ({e}); is the package installed? The python "
            'path should be in the format "my_package.module:callable_object"'
        ) from e
    _log.info("Starting consumer with %s callback", callback_path)
    return callback_object


def _consume_errback(failure: Failure) -> None:
    """Handle any errors that occur during consumer registration."""
    global _exit_code
    # Wouldn't it be nice if failure.check() was a TypeGuard, don't you think Twisted?
    if failure.check(exceptions.BadDeclaration):
        failure.value = cast(exceptions.BadDeclaration, failure.value)
        _log.error(
            "Unable to declare the %s object on the AMQP broker. The "
            "broker responded with %s. Check permissions for your user.",
            failure.value.obj_type,
            failure.value.reason,
        )
        _exit_code = 10
    elif failure.check(exceptions.PermissionException):
        failure.value = cast(exceptions.PermissionException, failure.value)
        _exit_code = 15
        _log.error(
            "The consumer could not proceed because of a permissions problem: %s",
            str(failure.value),
        )
    elif failure.check(exceptions.ConnectionException):
        failure.value = cast(exceptions.ConnectionException, failure.value)
        _exit_code = 14
        _log.error(failure.value.reason)
    else:
        _exit_code = 11
        _log.exception(
            "An unexpected error (%r) occurred while registering the "
            "consumer, please report this bug.",
            failure.value,
        )
    try:
        cast(asyncioreactor.AsyncioSelectorReactor, reactor).stop()
    except error.ReactorNotRunning:
        pass


def _consume_callback(consumers: Sequence["Consumer"]) -> None:
    """
    Callback when consumers are successfully registered.

    This simply registers callbacks for consumer.result deferred object which
    fires when the consumer stops.

    Args
        consumers: The list of consumers that were successfully created.
    """
    for consumer in consumers:

        def errback(failure: Failure, consumer: "Consumer" = consumer) -> None:
            global _exit_code
            if failure.check(exceptions.HaltConsumer):
                failure.value = cast(exceptions.HaltConsumer, failure.value)
                _exit_code = failure.value.exit_code
                if _exit_code:
                    _log.error(
                        "Consumer halted with non-zero exit code (%d): %s",
                        _exit_code,
                        str(failure.value.reason),
                    )
            elif failure.check(exceptions.ConsumerCanceled):
                failure.value = cast(exceptions.ConsumerCanceled, failure.value)
                _exit_code = 12
                _log.error(
                    "The consumer was canceled server-side, check with system administrators."
                )
            elif failure.check(exceptions.PermissionException):
                failure.value = cast(exceptions.PermissionException, failure.value)
                _exit_code = 15
                _log.error(
                    "The consumer could not proceed because of a permissions problem: %s",
                    str(failure.value),
                )
            else:
                _exit_code = 13
                _log.error("Unexpected error occurred in consumer %r: %r", consumer, failure)
            try:
                cast(asyncioreactor.AsyncioSelectorReactor, reactor).stop()
            except error.ReactorNotRunning:
                pass

        def callback(consumer: "Consumer") -> None:
            _log.info("The %r consumer halted.", consumer)
            if all([c.result.called for c in consumers]):
                _log.info("All consumers have stopped; shutting down.")
                try:
                    # Last consumer out shuts off the lights
                    cast(asyncioreactor.AsyncioSelectorReactor, reactor).stop()
                except error.ReactorNotRunning:
                    pass

        consumer.result.addCallbacks(callback, errback)


@cli.command()
@click.option("--exchange", help=_publish_exchange_help)
@click.argument("file", type=click.File("r"))
def publish(exchange: str, file: IO[str]) -> None:
    """Publish messages to an AMQP exchange from a file."""
    for msgs_json_str in file:
        try:
            messages = loads(msgs_json_str)
        except exceptions.ValidationError as e:
            raise click.BadArgumentUsage(f"Unable to validate message: {e}") from e

        for msg in messages:
            click.echo(f"Publishing message with topic {msg.topic}")
            try:
                api.publish(msg, exchange)
            except (exceptions.PublishReturned, exceptions.PublishForbidden) as e:
                click.echo(f"Unable to publish message: {e}")
                sys.exit(errno.EREMOTEIO)
            except exceptions.PublishTimeout as e:
                click.echo(f"Unable to connect to the message broker: {e}")
                sys.exit(errno.ECONNREFUSED)
            except exceptions.PublishException as e:
                click.echo(f"A general publish exception occurred: {e}")
                sys.exit(1)


class Recorder:
    """
    A simple callback class that records messages.

    Attributes:
        counter (int): The number of messages this callback has recorded.

    Args:
        limit: The maximum number of messages to record.
        file: The file object with write rights to store the messages.
    """

    def __init__(self, limit: int, file: IO[str]):
        self.counter = 0
        self._limit = limit
        self._file = file
        if limit:
            self._bar: ProgressBar[message.Message] = click.progressbar(length=limit)

    def collect_message(self, message: message.Message) -> None:
        """
        Collect received messages.

        Args:
            message: The received message.

        Raises:
            fedora_messaging.exceptions.HaltConsumer: Raised if the number of received
                messages reach the limit or if collected messages serialization is impossible.
        """
        try:
            json_str = dumps(message)
        except exceptions.ValidationError as e:
            click.echo(f"Unable to save messages to file: {e}")
            raise exceptions.HaltConsumer(exit_code=1, requeue=False) from e
        else:
            self._file.write(json_str)

        self.counter += 1
        if self._limit:
            self._bar.update(1)
            if self._limit <= self.counter:
                raise exceptions.HaltConsumer(exit_code=0, requeue=False)


@cli.command()
@click.argument("file", type=click.File("w"))
@click.option("--limit", help=_limit_help, type=click.IntRange(1))
@click.option("--app-name", help=_app_name_help, default="recorder")
@click.option("--routing-key", help=_routing_key_help, multiple=True)
@click.option("--queue-name", help=_queue_name_help)
@click.option("--exchange", help=_exchange_help)
def record(
    exchange: str, queue_name: str, routing_key: list[str], app_name: str, limit: int, file: IO[str]
) -> None:
    """Record messages from an AMQP queue to provided file."""
    messages_recorder = Recorder(limit, file)
    _consume(exchange, queue_name, routing_key, messages_recorder.collect_message, app_name)


DEFAULT_DATAGREPPER_URL = "https://apps.fedoraproject.org/datagrepper"


def _get_message(message_id: str, datagrepper_url: str) -> dict[str, Any]:
    """Fetch a message by ID from Datagreeper"""
    url = f"{datagrepper_url}/v2/id?id={message_id}&is_raw=true"
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        return cast(dict[str, Any], response.json())
    except requests.exceptions.RequestException as e:
        raise click.ClickException(f"Failed to retrieve message from Datagrepper: {e}") from e


@cli.command()
@click.argument("message_id")
@click.option(
    "--datagrepper-url",
    help=_datagrepper_help,
    default=DEFAULT_DATAGREPPER_URL,
    show_default=True,
)
def replay(message_id: str, datagrepper_url: str) -> None:
    """Replay a message from Datagrepper by its message ID"""
    message_data = _get_message(message_id, datagrepper_url)
    if message_data:
        # Disable the topic prefix, the loaded message already has everything.
        config.conf["topic_prefix"] = ""
        api.publish(message.load_message(message_data))
        click.echo(f"Message with ID {message_id} has been successfully replayed.")


@cli.command()
@click.argument("message_id", type=click.types.UUIDParameterType())
@click.option("--callback-file", help=_callback_file_help, type=click.Path())
@click.option("--callback", help=_callback_help)
@click.option(
    "--datagrepper-url",
    help=_datagrepper_help,
    default=DEFAULT_DATAGREPPER_URL,
    show_default=True,
)
def reconsume(
    datagrepper_url: str,
    callback: str,
    callback_file: str,
    message_id: str,
) -> None:
    """
    Re-consume a message from Datagrepper.

    This command will load a message from Datagrepper and then pass it
    to the configured consumer. This can be useful when testing a new
    consumer, or in the event that your consumer accidentally acknowledged
    a message it failed to properly process.
    """
    if callback_file:
        python_callback = _callback_from_filesystem(callback_file)
    else:
        python_callback = _callback_from_python_path(callback)
    # It's not a public API, but hopefully we will not break ourselves.
    callback_instance = api._check_callback(python_callback)
    msg_dict = _get_message(message_id, datagrepper_url)
    config.conf["topic_prefix"] = ""
    msg_obj = message.load_message(msg_dict)
    callback_instance(msg_obj)
