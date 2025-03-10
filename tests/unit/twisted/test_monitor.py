# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

import pytest
from twisted.application.service import MultiService
from twisted.web.client import Agent, readBody

from fedora_messaging.twisted.monitor import monitor_service
from fedora_messaging.twisted.stats import ConsumerStatistics


try:
    import pytest_twisted
except ImportError:
    pytest.skip("pytest-twisted is missing, skipping tests", allow_module_level=True)


@pytest.fixture
def service(available_port):
    srv = MultiService()
    monitor_service(srv, address="127.0.0.1", port=available_port)
    srv.startService()
    yield srv
    srv.stopService()


@pytest.fixture
def client():
    from twisted.internet import reactor

    return Agent(reactor)


class TestMonitorService:

    @pytest_twisted.inlineCallbacks
    def test_liveness(self, available_port, service, client):
        response = yield client.request(
            b"GET", f"http://localhost:{available_port}/live".encode("ascii")
        )
        body = yield readBody(response)
        assert body == b'{"status": "OK"}\n'

    @pytest_twisted.inlineCallbacks
    def test_readiness(self, available_port, service, client):
        service.consuming = True
        service.stats = ConsumerStatistics()
        service.stats.received = 42
        response = yield client.request(
            b"GET", f"http://localhost:{available_port}/ready".encode("ascii")
        )
        body = yield readBody(response)
        assert body == (
            b'{"consuming": true, "received": 42, "processed": 0, "dropped": 0, "rejected": 0, '
            b'"failed": 0}\n'
        )
