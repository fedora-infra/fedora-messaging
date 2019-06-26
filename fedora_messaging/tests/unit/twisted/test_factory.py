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

from __future__ import absolute_import, unicode_literals

import pika
import pytest

from fedora_messaging.twisted.factory import FedoraMessagingFactory


@pytest.mark.parametrize(
    "parameters,confirms,msg",
    [
        (
            pika.ConnectionParameters(),
            True,
            (
                "FedoraMessagingFactory(parameters=<ConnectionParameters host=localhost"
                " port=5672 virtual_host=/ ssl=False>, confirms=True)"
            ),
        ),
        (
            pika.ConnectionParameters(
                host="example.com",
                credentials=pika.PlainCredentials("user", "secret"),
                port=5671,
                virtual_host="/pub",
            ),
            True,
            (
                "FedoraMessagingFactory(parameters=<ConnectionParameters host=example.com"
                " port=5671 virtual_host=/pub ssl=False>, confirms=True)"
            ),
        ),
    ],
)
def test_repr(parameters, confirms, msg):
    """Assert __repr__ prints useful information"""
    f = FedoraMessagingFactory(parameters, confirms)
    assert repr(f) == msg
