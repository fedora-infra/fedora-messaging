# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

from unittest import mock

import pika
from twisted.internet import defer

from fedora_messaging.twisted.protocol import FedoraMessagingProtocolV2


class MockChannel(mock.Mock):
    """A mock object with Channel-specific methods that return Deferreds."""

    def __init__(self, *args, **kwargs):
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
        self.queue_object = defer.DeferredQueue()
        self.basic_consume = mock.Mock(
            side_effect=lambda **kw: defer.succeed((self.queue_object, "consumer-tag"))
        )


class MockProtocol(FedoraMessagingProtocolV2):
    """A Protocol object that mocks the underlying channel and impl."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._impl = mock.Mock(name="_impl")
        self._impl.is_closed = True
        self._channel = MockChannel(name="_channel")
        self.channel = mock.Mock(name="channel", side_effect=lambda: defer.succeed(self._channel))

    def _register_consumer(self, consumer):
        consumer._protocol = self
        consumer._channel = self._channel
        self._consumers[consumer.queue] = consumer
