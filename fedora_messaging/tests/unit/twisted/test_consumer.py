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


from unittest import mock, TestCase

import pika
import pytest
from fedora_messaging.twisted.consumer import Consumer

from .utils import MockChannel

try:
    import pytest_twisted
except ImportError:
    pytest.skip("pytest-twisted is missing, skipping tests", allow_module_level=True)


class ConsumerTests(TestCase):
    """Unit tests for the Consumer class."""

    def test_cancel(self):
        """The cancel method must call the corresponding channel methods"""
        proto = mock.Mock()
        channel = MockChannel()
        consumer = Consumer(queue="queue")
        consumer._protocol = proto
        consumer._channel = channel

        def check(_):
            proto._forget_consumer.assert_called_with("queue")
            channel.basic_cancel.assert_called_with(consumer_tag=consumer._tag)
            channel.close.assert_called_with()

        d = consumer.cancel()
        d.addCallback(check)
        return pytest_twisted.blockon(d)

    def test_cancel_channel_error(self):
        """Assert channel errors are caught; a closed channel cancels consumers."""
        consumer = Consumer("my_queue", lambda _: _)
        consumer._channel = mock.Mock()
        consumer._channel.basic_cancel.side_effect = pika.exceptions.AMQPChannelError()
        consumer._protocol = mock.Mock()
        consumer._protocol._consumers = {"my_queue": consumer}

        def _check(_):
            consumer._protocol._forget_consumer.assert_called_with("my_queue")
            consumer._channel.basic_cancel.assert_called_once_with(
                consumer_tag=consumer._tag
            )

        d = consumer.cancel()
        d.addCallback(_check)

        return pytest_twisted.blockon(d)
