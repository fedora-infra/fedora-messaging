# This file is part of fedora_messaging.
# Copyright (C) 2019 Red Hat, Inc.
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
"""This module contains a set of callbacks that are generally useful."""
from __future__ import print_function

import logging
import subprocess

import six

from . import config, exceptions

_log = logging.getLogger(__name__)


def printer(message):
    """
    A simple callback that prints the message to standard output.

    Usage: ``fedora-messaging consume --callback="fedora_messaging.callbacks:printer"``

    Args:
        message (fedora_messaging.api.Message): The message that was received.
    """
    print(six.text_type(message))


def run(message):
    """
    Run a command in a subprocess.

    **Configuration Options**

    This consumer uses the following configuration keys in the :ref:`conf-consumer-config`
    section of the configuration file:

    * ``command`` (string or list of strings): The arguments to launch the process
        as a string or list of strings.
    * ``shell`` (boolean): Whether or not to execute the command in a shell. Refer to the
        `security considerations
        <https://docs.python.org/3/library/subprocess.html#security-considerations>`_
        before setting this to true. Defaults to false.
    * ``timeout`` (integer): The length of time, in seconds, to wait on the command
        before halting the command and returning the message to the queue for later
        processing. Defaults to no timeout.
    """
    try:
        subprocess.check_call(  # nosec
            config.conf["consumer_config"]["command"],
            shell=config.conf["consumer_config"].get("shell", False),
            timeout=config.conf["consumer_config"].get("timeout"),
        )
    except subprocess.CalledProcessError:
        raise exceptions.Drop()
    except subprocess.TimeoutExpired as e:
        _log.error(
            "{} failed to return after {} seconds; returning {} to the queue",
            e.cmd,
            e.timeout,
            str(message.id),
        )
        raise exceptions.Nack()
