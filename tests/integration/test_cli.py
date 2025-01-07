# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

"""Tests for the ``fedora-messaging`` command-line interface."""
import os
import shutil
import subprocess
import sys
import uuid

import pytest
import pytest_twisted
import requests
from twisted.internet import error, reactor, threads
from twisted.web.client import Agent, readBody

from fedora_messaging import api, exceptions, message

from .utils import RABBITMQ_HOST, sleep


@pytest.fixture
def cli_conf(fixtures_dir, tmp_path, available_port):
    config_path = tmp_path.joinpath("config.toml")
    with open(config_path, "w") as config_fh:
        with open(os.path.join(fixtures_dir, "cli_integration.toml")) as ref_fh:
            config_fh.write(ref_fh.read())
        config_fh.write("\n")
        config_fh.write(f"[monitoring]\nport = {available_port}\n")
    return config_path


def halt_exit_0(message):
    """Exit with code 0 when it gets a message"""
    raise exceptions.HaltConsumer()


def halt_exit_42(message):
    """Exit with code 42 when it gets a message"""
    raise exceptions.HaltConsumer(exit_code=42, reason="Life, the universe, and everything")


def fail_processing(message):
    """Fail processing message"""
    raise ValueError()


def execute_action(message):
    if message.body.get("action") == "halt":
        raise exceptions.HaltConsumer()
    elif message.body.get("action") == "drop":
        raise exceptions.Drop()
    elif message.body.get("action") == "reject":
        raise exceptions.Nack()
    elif message.body.get("action") == "fail":
        raise ValueError()


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
        ("fail_processing", 13, b"Unexpected error occurred in consumer"),
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

    process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)  # noqa: S603
    yield sleep(5)

    yield threads.deferToThread(api.publish, message.Message())
    for _ in range(5):
        yield sleep(1)
        if process.poll() is not None:
            break
    else:
        process.kill()
        pytest.fail(f"Process never stopped!: {process.stdout.read()}")

    assert process.returncode == exit_code, process.stderr.read()
    assert msg in process.stdout.read()


@pytest_twisted.inlineCallbacks
def test_consume_monitoring(queue, cli_conf, available_port):
    """Assert the monitoring works."""
    cmd = shutil.which("fedora-messaging")
    args = [
        sys.executable,
        cmd,
        f"--conf={cli_conf}",
        "consume",
        "--callback=tests.integration.test_cli:execute_action",
        f"--queue-name={queue}",
        "--exchange=amq.topic",
        "--routing-key=#",
    ]

    http_client = Agent(reactor)
    base_url = f"http://localhost:{available_port}".encode("ascii")

    # Monitoring not available yet
    with pytest.raises(error.ConnectionRefusedError):
        yield http_client.request(b"GET", base_url + b"/live")

    # Start the consumer
    process = subprocess.Popen(args)  # noqa: S603

    # Wait for the consumer to start up
    for _ in range(5):
        yield sleep(1)
        try:
            response = yield http_client.request(b"GET", base_url + b"/live")
        except error.ConnectionRefusedError:
            continue
        # Check the monitoring on startup
        body = yield readBody(response)
        assert body == b'{"status": "OK"}\n'
        response = yield http_client.request(b"GET", base_url + b"/ready")
        body = yield readBody(response)
        assert body == (
            b'{"consuming": true, "published": 0, "consumed": {"received": 0, "processed": 0, "dropped": 0, '
            b'"rejected": 0, "failed": 0}}\n'
        )
        break
    else:
        pytest.fail(f"Monitoring didn't start: {process.stdout.read()} -- {process.stderr.read()}")

    # Publish a message
    yield threads.deferToThread(api.publish, message.Message())
    yield sleep(0.5)

    # Check stats
    response = yield http_client.request(b"GET", base_url + b"/ready")
    body = yield readBody(response)
    assert body == (
        b'{"consuming": true, "published": 0, "consumed": {"received": 1, "processed": 1, "dropped": 0, '
        b'"rejected": 0, "failed": 0}}\n'
    )

    # Publish a message and drop it
    yield threads.deferToThread(api.publish, message.Message(body={"action": "drop"}))
    yield sleep(0.5)

    # Check stats
    response = yield http_client.request(b"GET", base_url + b"/ready")
    body = yield readBody(response)
    assert body == (
        b'{"consuming": true, "published": 0, "consumed": {"received": 2, "processed": 1, "dropped": 1, '
        b'"rejected": 0, "failed": 0}}\n'
    )

    # Don't check the reject action or the message will be put back in the queue and loop forever
    # Don't check a generic processing failure because it stops the consumer instantly and we can't look
    # at the stats

    # Now stop
    yield threads.deferToThread(api.publish, message.Message(body={"action": "halt"}))

    for _ in range(5):
        yield sleep(1)
        if process.poll() is not None:
            break
    else:
        process.kill()
        pytest.fail(f"Process never stopped!: {process.stdout.read()}")

    assert process.returncode == 0, process.stderr.read()
