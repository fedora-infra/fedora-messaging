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

import mock
import pika
import pytest
import pytest_twisted
from twisted.internet import defer

from fedora_messaging.exceptions import ConnectionException
from fedora_messaging.twisted.protocol import FedoraMessagingProtocol, _add_timeout


@pytest_twisted.inlineCallbacks
def test_consume_connection_exception():
    """If consuming fails due to a non-permission error, a ConnectionException happens."""
    proto = FedoraMessagingProtocol(None)
    mock_channel = mock.Mock()
    mock_channel.basic_consume.side_effect = pika.exceptions.ChannelClosed(
        400, "Bad Request!"
    )
    deferred_channel = defer.succeed(mock_channel)
    proto._allocate_channel = mock.Mock(return_value=deferred_channel)

    try:
        yield proto.consume(lambda x: x, "test_queue")
    except Exception as e:
        assert isinstance(e, ConnectionException)


@pytest_twisted.inlineCallbacks
def test_twisted12_timeout():
    """Assert timeouts work for Twisted 12.2 (EL7)"""
    d = defer.Deferred()
    d.addTimeout = mock.Mock(side_effect=AttributeError())
    _add_timeout(d, 0.1)

    try:
        yield d
        pytest.fail("Expected an exception")
    except Exception as e:
        assert isinstance(e, defer.CancelledError)


def test_twisted12_cancel_cancel_callback():
    """Assert canceling the cancel call for Twisted 12.2 (EL7) works."""
    d = defer.Deferred()
    d.addTimeout = mock.Mock(side_effect=AttributeError())
    d.cancel = mock.Mock()
    with mock.patch("fedora_messaging.twisted.protocol.reactor") as mock_reactor:
        _add_timeout(d, 1)
        delayed_cancel = mock_reactor.callLater.return_value
        d.callback(None)
        delayed_cancel.cancel.assert_called_once_with()
