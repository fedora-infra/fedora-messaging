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

import click

from . import config, api

_log = logging.getLogger(__name__)


@click.group()
@click.option('--conf', envvar='FEDORA_MESSAGING_CONF')
def cli(conf):
    """The fedora-messaging command line interface."""
    if conf:
        try:
            config.conf.load_config(filename=conf)
        except ValueError as e:
            raise click.exceptions.BadParameter(e)


@cli.command()
@click.option('--app-name')
@click.option('--callback')
@click.option('--routing-key')
@click.option('--queue-name')
@click.option('--exchange')
@click.option('--amqp-url')
def consume(amqp_url, exchange, queue_name, routing_key, callback, app_name):

    amqp_url = amqp_url or config.conf['amqp_url']
    if exchange and queue_name and routing_key:
        bindings = [{
            'exchange': exchange,
            'queue_name': queue_name,
            'routing_key': routing_key
        }]
    elif not exchange and not queue_name and not routing_key:
        bindings = config.conf['bindings']
    else:
        raise click.ClickException(
            'You must define all three of exchange, queue_name and'
            ' routing_key, or none of them to use the configuration')
    if not bindings:
        raise click.ClickException(
            'No bindings are defined in the configuration file'
            ' and none were provided as arguments!')

    callback_path = callback or config.conf['callback']
    if not callback_path:
        raise click.ClickException('"callback" must be the Python path to a'
                                   ' callable to consume')
    try:
        module, cls = callback_path.strip().split(':')
    except ValueError as e:
        raise click.ClickException('Unable to parse the callback path ({}); the '
                                   'expected format is "my_package.module:'
                                   'callable_object"'.format(str(e)))
    try:
        module = importlib.import_module(module)
    except ImportError as e:
        raise click.ClickException('Failed to import the callback module ({})'.format(str(e)))

    try:
        callback = getattr(module, cls)
    except AttributeError as e:
        raise click.ClickException(
            'Unable to import {} ({}); is the package installed? The python path should '
            'be in the format "my_package.module:callable_object"'.format(
                callback_path, str(e)))

    if app_name:
        config.conf['client_properties']['app'] = app_name

    _log.info('Starting consumer with %s callback', callback_path)
    return api.consume(callback, bindings)
