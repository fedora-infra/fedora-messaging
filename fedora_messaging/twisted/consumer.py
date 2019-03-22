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
from __future__ import absolute_import

import uuid

import pika

from twisted.internet import defer


class Consumer(object):
    """
    Represents a Twisted AMQP consumer and is returned from the call to
    :func:`fedora_messaging.api.twisted_consume`.

    Attributes:
        queue (str): The AMQP queue this consumer is subscribed to.
        callback (callable): The callback to run when a message arrives.
        result (twisted.internet.defer.Deferred):
            A deferred that runs the callbacks if the consumer exits gracefully
            after being canceled by a call to :meth:`Consumer.cancel` and
            errbacks if the consumer stops for any other reason. The reasons a
            consumer could stop are: a
            :class:`fedora_messaging.exceptions.PermissionExecption` if the
            consumer does not have permissions to read from the queue it is
            subscribed to, a :class:`.HaltConsumer` is raised by the consumer
            indicating it wishes to halt, an unexpected :class:`Exception` is
            raised by the consumer, or if the consumer is canceled by the
            server which happens if the queue is deleted by an administrator or
            if the node the queue lives on fails.
    """

    def __init__(self, queue=None, callback=None):
        self.queue = queue
        self.callback = callback
        self.result = defer.Deferred()

        # The current channel used by this consumer.
        self._channel = None
        # The unique ID for the AMQP consumer.
        self._tag = str(uuid.uuid4())
        # Used in the consumer read loop to know when it's being canceled.
        self._running = True
        # The current read loop
        self._read_loop = None
        # The protocol that currently runs this consumer, used when cancel is
        # called to remove itself from the protocol and its factory so it doesn't
        # restart on the next connection.
        self._protocol = None

    def __repr__(self):
        return "Consumer(queue={}, callback={})".format(self.queue, self.callback)

    @defer.inlineCallbacks
    def cancel(self):
        """
        Cancel the consumer and clean up resources associated with it.
        Consumers that are canceled are allowed to finish processing any
        messages before halting.

        Returns:
            defer.Deferred: A deferred that fires when the consumer has finished
            processing any message it was in the middle of and has been successfully
            canceled.
        """
        # Remove it from protocol and factory so it doesn't restart later.
        try:
            del self._protocol._consumers[self.queue]
        except (KeyError, AttributeError):
            pass
        try:
            del self._protocol.factory._consumers[self.queue]
        except (KeyError, AttributeError):
            pass
        # Signal to the _read loop it's time to stop and wait for it to finish
        # with whatever message it might be working on, then wait for the deferred
        # to fire which indicates it is done.
        self._running = False
        yield self._read_loop
        try:
            yield self._channel.basic_cancel(consumer_tag=self._tag)
        except pika.exceptions.AMQPChannelError:
            # Consumers are tied to channels, so if this channel is dead the
            # consumer should already be canceled (and we can't get to it anyway)
            pass
        try:
            yield self._channel.close()
        except pika.exceptions.AMQPChannelError:
            pass
        if not self.result.called:
            self.result.callback(self)
