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

from twisted.python import log as legacy_twisted_log
from twisted.internet import reactor, error
import click
import pkg_resources

from . import config, api, exceptions

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
    "digits, hyphen, underscore, period, or colon. If one is not specified, the "
    "default is the ``amq.topic`` exchange."
)


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
@click.option("--callback", help=_callback_help)
@click.option("--routing-key", help=_routing_key_help, multiple=True)
@click.option("--queue-name", help=_queue_name_help)
@click.option("--exchange", help=_exchange_help)
def consume(exchange, queue_name, routing_key, callback, app_name):
    """Consume messages from an AMQP queue using a Python callback."""
    # The configuration validates these are not null and contain all required keys
    # when it is loaded.

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
        callback = getattr(module, cls)
    except AttributeError as e:
        raise click.ClickException(
            "Unable to import {} ({}); is the package installed? The python path should "
            'be in the format "my_package.module:callable_object"'.format(
                callback_path, str(e)
            )
        )

    if app_name:
        config.conf["client_properties"]["app"] = app_name

    _log.info("Starting consumer with %s callback", callback_path)
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
