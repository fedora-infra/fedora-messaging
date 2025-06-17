# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

from typing import Any
from unittest import mock

import pika
import pika.frame
from twisted.internet import defer

from fedora_messaging.twisted.consumer import QueueContent
from fedora_messaging.twisted.protocol import FedoraMessagingProtocolV2


class MockChannel(mock.Mock):
    """A mock object with Channel-specific methods that return Deferreds."""

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        deferred_methods = (
            "basic_qos",
            "confirm_delivery",
            "exchange_declare",
            "queue_bind",
            "basic_ack",
            "basic_nack",
            "basic_publish",
            "basic_cancel",
            "close",
        )
        for method in deferred_methods:
            setattr(
                self,
                method,
                mock.Mock(side_effect=lambda *a, **kw: defer.succeed(None)),
            )
        self.queue_declare = mock.Mock(
            side_effect=lambda **kw: defer.succeed(
                pika.frame.Method(0, pika.spec.Queue.DeclareOk(queue=kw["queue"]))
            )
        )
        # self.queue_object = mock.Mock(name="queue_object")
        self.queue_object: defer.DeferredQueue[QueueContent] = defer.DeferredQueue()
        self.basic_consume = mock.Mock(
            side_effect=lambda **kw: defer.succeed((self.queue_object, "consumer-tag"))
        )


class MockProtocol(FedoraMessagingProtocolV2):
    """A Protocol object that mocks the underlying channel and impl."""

    _channel: MockChannel
    channel: mock.Mock

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._impl = mock.Mock(name="_impl")
        self._impl.is_closed = True
        self._channel = MockChannel(  # pyright: ignore [reportIncompatibleVariableOverride]
            name="_channel"
        )
        self.channel = mock.Mock(  # pyright: ignore [reportIncompatibleMethodOverride]
            name="channel", side_effect=lambda: defer.succeed(self._channel)
        )
        self.factory = mock.Mock()
