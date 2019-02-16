# This file is part of fedora_messaging.
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
"""
The ``fedora-messaging`` `Click`_ CLI.

.. _Click: http://click.pocoo.org/
"""
from __future__ import absolute_import

import importlib
import logging
import logging.config
import os
import sys
import errno

from twisted.python import log as legacy_twisted_log
from twisted.internet import reactor, error
import click
import pkg_resources

from . import config, api, exceptions
from .message import dumps, loads

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
_limit_help = "The maximum number of messages to record."


# Global variable used to set the exit code in error handlers, then let
# the reactor shut down gracefully, then exit with the proper code.
_exit_code = 0


@click.group()
@click.option("--conf", envvar="FEDORA_MESSAGING_CONF", help=_conf_help)
def cli(conf):
    """The fedora-messaging command line interface."""
    if conf:
        if not os.path.isfile(conf):
            raise click.exceptions.BadParameter("{} is not a file".format(conf))
        try:
            config.conf.load_config(config_path=conf)
        except exceptions.ConfigurationException as e:
            raise click.exceptions.BadParameter(str(e))
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
def consume(exchange, queue_name, routing_key, callback, callback_file, app_name):
    """Consume messages from an AMQP queue using a Python callback."""
    # The configuration validates these are not null and contain all required keys
    # when it is loaded.
    if callback_file:
        callback = _callback_from_filesystem(callback_file)
    else:
        callback = _callback_from_python_path(callback)
    _consume(exchange, queue_name, routing_key, callback, app_name)


def _consume(exchange, queue_name, routing_key, callback, app_name):
    """
    The actual consume code, which expects an actual callable object.
    This lets various consume-based commands share the setup code. Anything
    that accepts None loads the defaults from the configuration.

    Args:
        exchange (str): The AMQP message exchange to bind to, or None.
        queue_name (str): The queue name to use, or None.
        routing_key (str): The routing key to use, or None.
        callback (callable): A callable object to use for the callback.
        app_name (str): The application name to use, or None.
    """
    bindings = config.conf["bindings"]
    queues = config.conf["queues"]

    # The CLI and config.DEFAULTS have different defaults for the queue
    # settings at the moment.  We should select a universal default in the
    # future and remove this. Unfortunately that will break backwards compatibility.
    if queues == config.DEFAULTS["queues"]:
        queues[config._default_queue_name]["durable"] = True
        queues[config._default_queue_name]["auto_delete"] = False

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
        deferred_consumers = api.twisted_consume(
            callback, bindings=bindings, queues=queues
        )
        deferred_consumers.addCallback(_consume_callback)
        deferred_consumers.addErrback(_consume_errback)
    except ValueError as e:
        click_version = pkg_resources.get_distribution("click").parsed_version
        if click_version < pkg_resources.parse_version("7.0"):
            raise click.exceptions.BadOptionUsage(str(e))
        else:
            raise click.exceptions.BadOptionUsage("callback", str(e))

    reactor.run()
    sys.exit(_exit_code)


def _callback_from_filesystem(callback_file):
    """
    Load a callable from a Python script on the file system.

    Args:
        callback_file (str): The callback as a filesystem path and callable name
            separated by a ":". For example, "my/python/file.py:printer".

    Raises:
        click.ClickException: If the object cannot be loaded.

    Returns:
        callable: The callable object.
    """
    try:
        file_path, callable_name = callback_file.strip().split(":")
    except ValueError:
        raise click.ClickException(
            "Unable to parse the '--callback-file' option; the "
            'expected format is "path/to/file.py:callable_object" where '
            '"callable_object" is the name of the function or class in the '
            "Python file"
        )

    try:
        file_namespace = {}
        with open(file_path, "rb") as fd:
            try:
                # Using "exec" is generally a Bad Idea (TM), so bandit is upset at
                # us. In this case, it seems like a Good Idea (TM), but I might be
                # wrong. Sorry.
                exec(compile(fd.read(), file_path, "exec"), file_namespace)  # nosec
            except Exception as e:
                raise click.ClickException(
                    "The {} file raised the following exception during execution:"
                    " {}".format(file_path, str(e))
                )

        if callable_name not in file_namespace:
            err = "The '{}' object was not found in the '{}' file.".format(
                callable_name, file_path
            )
            raise click.ClickException(err)
        else:
            return file_namespace[callable_name]
    except IOError as e:
        raise click.ClickException("An IO error occurred: {}".format(str(e)))


def _callback_from_python_path(callback):
    """
    Load a callable from a Python path.

    Args:
        callback_file (str): The callback as a Python path and callable name
            separated by a ":". For example, "my_package.my_module:printer".

    Raises:
        click.ClickException: If the object cannot be loaded.

    Returns:
        callable: The callable object.
    """
    callback_path = callback or config.conf["callback"]
    if not callback_path:
        raise click.ClickException(
            "A Python path to a callable object that accepts the message must be provided"
            ' with the "--callback" command line option or in the configuration file'
        )
    try:
        module, cls = callback_path.strip().split(":")
    except ValueError:
        raise click.ClickException(
            "Unable to parse the callback path ({}); the "
            'expected format is "my_package.module:'
            'callable_object"'.format(callback_path)
        )
    try:
        module = importlib.import_module(module)
    except ImportError as e:
        provider = "--callback argument" if callback else "configuration file"
        raise click.ClickException(
            "Failed to import the callback module ({}) provided in the {}".format(
                str(e), provider
            )
        )

    try:
        callback_object = getattr(module, cls)
    except AttributeError as e:
        raise click.ClickException(
            "Unable to import {} ({}); is the package installed? The python path should "
            'be in the format "my_package.module:callable_object"'.format(
                callback_path, str(e)
            )
        )
    _log.info("Starting consumer with %s callback", callback_path)
    return callback_object


def _consume_errback(failure):
    """Handle any errors that occur during consumer registration."""
    global _exit_code
    if failure.check(exceptions.BadDeclaration):
        _log.error(
            "Unable to declare the %s object on the AMQP broker. The "
            "broker responded with %s. Check permissions for your user.",
            failure.value.obj_type,
            failure.value.reason,
        )
        _exit_code = 10
    elif failure.check(exceptions.PermissionException):
        _exit_code = 15
        _log.error(
            "The consumer could not proceed because of a permissions problem: %s",
            str(failure.value),
        )
    elif failure.check(exceptions.ConnectionException):
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
        reactor.stop()
    except error.ReactorNotRunning:
        pass


def _consume_callback(consumers):
    """
    Callback when consumers are successfully registered.

    This simply registers callbacks for consumer.result deferred object which
    fires when the consumer stops.

    Args
        consumers (list of fedora_messaging.api.Consumer):
            The list of consumers that were successfully created.
    """
    for consumer in consumers:

        def errback(failure):
            global _exit_code
            if failure.check(exceptions.HaltConsumer):
                _exit_code = failure.value.exit_code
                if _exit_code:
                    _log.error(
                        "Consumer halted with non-zero exit code (%d): %s",
                        _exit_code,
                        str(failure.value.reason),
                    )
            elif failure.check(exceptions.ConsumerCanceled):
                _exit_code = 12
                _log.error(
                    "The consumer was canceled server-side, check with system administrators."
                )
            elif failure.check(exceptions.PermissionException):
                _exit_code = 15
                _log.error(
                    "The consumer could not proceed because of a permissions problem: %s",
                    str(failure.value),
                )
            else:
                _exit_code = 13
                _log.error(
                    "Unexpected error occurred in consumer %r: %r", consumer, failure
                )
            try:
                reactor.stop()
            except error.ReactorNotRunning:
                pass

        def callback(consumer):
            _log.info("The %r consumer halted.", consumer)
            if all([c.result.called for c in consumers]):
                _log.info("All consumers have stopped; shutting down.")
                try:
                    # Last consumer out shuts off the lights
                    reactor.stop()
                except error.ReactorNotRunning:
                    pass

        consumer.result.addCallbacks(callback, errback)


@cli.command()
@click.option("--exchange", help=_publish_exchange_help)
@click.argument("file", type=click.File("r"))
def publish(exchange, file):
    """Publish messages to an AMQP exchange from a file."""
    for msgs_json_str in file:
        try:
            messages = loads(msgs_json_str)
        except exceptions.ValidationError as e:
            raise click.BadArgumentUsage(
                "Unable to validate message: {}".format(str(e))
            )

        for msg in messages:
            click.echo("Publishing message with topic {}".format(msg.topic))
            try:
                api.publish(msg, exchange)
            except exceptions.PublishReturned as e:
                click.echo("Unable to publish message: {}".format(str(e)))
                sys.exit(errno.EREMOTEIO)
            except exceptions.PublishTimeout as e:
                click.echo("Unable to connect to the message broker: {}".format(str(e)))
                sys.exit(errno.ECONNREFUSED)
            except exceptions.PublishException as e:
                click.echo("A general publish exception occurred: {}".format(str(e)))
                sys.exit(1)


class Recorder:
    """
    A simple callback class that records messages.

    Attributes:
        counter (int): The number of messages this callback has recorded.
        messages (list): The list of messages received.

    Args:
        limit (int): The maximum number of messages to record.
        file (file): The file object with write rights.
    """

    def __init__(self, limit, file):
        self.counter = 0
        self._limit = limit
        self._file = file
        if limit:
            self._bar = click.progressbar(length=limit)

    def collect_message(self, message):
        """
        Collect received messages.

        Args:
            message (message.Message): The received message.

        Raises:
            fedora_messaging.exceptions.HaltConsumer: Raised if the number of received
                messages reach the limit or if collected messages serialization is impossible.
        """
        try:
            json_str = dumps(message)
        except exceptions.ValidationError as e:
            click.echo("Unable to save messages to file: {}".format(str(e)))
            raise exceptions.HaltConsumer(exit_code=1, requeue=False)
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
def record(exchange, queue_name, routing_key, app_name, limit, file):
    """Record messages from an AMQP queue to provided file."""
    messages_recorder = Recorder(limit, file)
    _consume(
        exchange, queue_name, routing_key, messages_recorder.collect_message, app_name
    )
