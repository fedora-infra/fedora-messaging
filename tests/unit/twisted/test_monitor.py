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
