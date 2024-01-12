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
"""Tests for the ``fedora-messaging`` command-line interface."""
import os
import shutil
import subprocess
import sys
import uuid

import pytest
import pytest_twisted
import requests
from twisted.internet import threads

from fedora_messaging import api, exceptions, message

from .utils import RABBITMQ_HOST, sleep


@pytest.fixture
def cli_conf(fixtures_dir):
    return os.path.join(fixtures_dir, "cli_integration.toml")


def halt_exit_0(message):
    """Exit with code 0 when it gets a message"""
    raise exceptions.HaltConsumer()


def halt_exit_42(message):
    """Exit with code 42 when it gets a message"""
    raise exceptions.HaltConsumer(
        exit_code=42, reason="Life, the universe, and everything"
    )


@pytest.fixture
def queue(scope="function"):
    queue = str(uuid.uuid4())
    yield queue
    requests.delete(
        f"http://{RABBITMQ_HOST}:15672/api/queues/%2F/{queue}",
        auth=("guest", "guest"),
        timeout=3,
    )


@pytest.mark.parametrize(
    "callback,exit_code,msg",
    [
        ("halt_exit_0", 0, b"Consumer indicated it wishes consumption to halt"),
        ("halt_exit_42", 42, b"Life, the universe, and everything"),
    ],
)
@pytest_twisted.inlineCallbacks
def test_consume_halt_with_exitcode(callback, exit_code, msg, queue, cli_conf):
    """Assert user execution halt with reason and exit_code is reported."""
    cmd = shutil.which("fedora-messaging")
    args = [
        sys.executable,
        cmd,
        f"--conf={cli_conf}",
        "consume",
        f"--callback=tests.integration.test_cli:{callback}",
        f"--queue-name={queue}",
        "--exchange=amq.topic",
        "--routing-key=#",
    ]

    process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    yield sleep(5)

    yield threads.deferToThread(api.publish, message.Message())
    for _ in range(5):
        yield sleep(1)
        if process.poll() is not None:
            break
    else:
        process.kill()
        pytest.fail(f"Process never stopped!: {process.stdout.read()}")

    assert process.returncode == exit_code
    assert msg in process.stdout.read()
