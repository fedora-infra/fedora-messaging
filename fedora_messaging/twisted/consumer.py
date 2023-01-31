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


import asyncio
import logging
import uuid

import pika
from twisted.internet import defer, error, reactor, threads
from twisted.python.failure import Failure

from .. import config
from ..exceptions import (
    ConnectionException,
    ConsumerCanceled,
    Drop,
    HaltConsumer,
    Nack,
    PermissionException,
    ValidationError,
)
from ..message import get_message


_std_log = logging.getLogger(__name__)


def _add_timeout(deferred, timeout):
    """
    Add a timeout to the given deferred. This is designed to work with both old
    Twisted and versions of Twisted with the addTimeout API. This is
    exclusively to support EL7.

    The deferred will errback with a :class:`defer.CancelledError` if the
    version of Twisted being used doesn't have the
    ``defer.Deferred.addTimeout`` API, otherwise it will errback with the
    normal ``error.TimeoutError``
    """
    try:
        deferred.addTimeout(timeout, reactor)
    except AttributeError:
        # Twisted 12.2 (in EL7) does not have the addTimeout API, so make do with
        # the slightly more annoying approach of scheduling a call to cancel which
        # is then canceled if the deferred succeeds before the timeout is up.
        delayed_cancel = reactor.callLater(timeout, deferred.cancel)

        def cancel_cancel_call(result):
            """Halt the delayed call to cancel if the deferred fires before the timeout."""
            if not delayed_cancel.called:
                delayed_cancel.cancel()
            return result

        deferred.addBoth(cancel_cancel_call)


def is_coro(func_or_obj):
    """Tests if a function is a coroutine function or a callable coroutine object."""
    # Until Python 3.10, inspect.iscoroutinefunction() will fail to identify AsyncMocks
    # as coroutines: https://github.com/python/cpython/issues/84753
    # Use asyncio.iscoroutinefunction() instead.
    return asyncio.iscoroutinefunction(func_or_obj) or (
        callable(func_or_obj) and asyncio.iscoroutinefunction(func_or_obj.__call__)
    )


class Consumer:
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
        return f"Consumer(queue={self.queue}, callback={self.callback})"

    @defer.inlineCallbacks
    def consume(self):
        yield self._channel.basic_qos(
            prefetch_count=config.conf["qos"]["prefetch_count"],
            prefetch_size=config.conf["qos"]["prefetch_size"],
        )
        try:
            queue_object, _ = yield self._channel.basic_consume(
                queue=self.queue, consumer_tag=self._tag
            )
        except pika.exceptions.ChannelClosed as exc:
            if exc.args[0] == 403:
                raise PermissionException(
                    obj_type="queue", description=self.queue, reason=exc.args[1]
                )
            else:
                raise ConnectionException(reason=exc)

        try:
            self._channel.add_on_cancel_callback(self._on_cancel_callback)
        except AttributeError:
            pass  # pika 1.0.0+

        self._read_loop = self._read(queue_object)
        self._read_loop.addErrback(self._read_loop_errback)

    @defer.inlineCallbacks
    def _read(self, queue_object):
        """
        The loop that reads from the message queue and calls the callback
        wrapper.

        Serialized Processing
        ---------------------
        This loop processes messages serially. This is because a second
        ``queue_object.get()`` operation can only occur after the Deferred from
        the callback completes. Thus, we can be sure that callbacks
        never run concurrently in two different threads.

        This is done rather than saturating the Twisted thread pool as the
        documentation for callbacks (in fedmsg and here) has never indicated
        that they are not thread-safe. In the future we can add a flag for users
        who are confident in their ability to write thread-safe code.

        Gracefully Halting
        ------------------
        This is a loop that only exits when the ``self._running`` variable is
        set to False. The call to cancel will set this to false, and will then
        wait for the Deferred from this function to call back in order to
        ensure the message finishes processing.

        The Deferred object only completes when this method returns, so we need
        to periodically check the status of ``self._running``. That's why
        there's a short timeout on the call to ``queue_object.get``.

        queue_object (pika.adapters.twisted_connection.ClosableDeferredQueue):
            The AMQP queue the consumer is bound to.
        """
        while self._running:
            yield self._read_one(queue_object)

    @defer.inlineCallbacks
    def _read_one(self, queue_object):
        try:
            deferred_get = queue_object.get()
            _add_timeout(deferred_get, 1)
            channel, delivery_frame, properties, body = yield deferred_get
        except (defer.TimeoutError, defer.CancelledError):
            return

        _std_log.debug(
            "Message arrived with delivery tag %s for %r",
            delivery_frame.delivery_tag,
            self._tag,
        )
        try:
            message = get_message(delivery_frame.routing_key, properties, body)
            message.queue = self.queue
        except ValidationError:
            _std_log.warning(
                "Message id %s did not pass validation; ignoring message",
                properties.message_id,
            )
            yield channel.basic_nack(
                delivery_tag=delivery_frame.delivery_tag, requeue=False
            )
            return

        try:
            _std_log.info(
                "Consuming message from topic %s (message id %s)",
                message.topic,
                properties.message_id,
            )
            if is_coro(self.callback):
                d = defer.Deferred.fromFuture(
                    asyncio.ensure_future(self.callback(message))
                )
            else:
                d = threads.deferToThread(self.callback, message)
            yield d
        except Nack:
            _std_log.warning(
                "Returning message id %s to the queue", properties.message_id
            )
            yield channel.basic_nack(
                delivery_tag=delivery_frame.delivery_tag, requeue=True
            )
        except Drop:
            _std_log.warning(
                "Consumer requested message id %s be dropped", properties.message_id
            )
            yield channel.basic_nack(
                delivery_tag=delivery_frame.delivery_tag, requeue=False
            )
        except HaltConsumer as e:
            _std_log.info(
                "Consumer indicated it wishes consumption to halt, shutting down"
            )
            if e.requeue:
                yield channel.basic_nack(
                    delivery_tag=delivery_frame.delivery_tag, requeue=True
                )
            else:
                yield channel.basic_ack(delivery_tag=delivery_frame.delivery_tag)
            raise e
        except Exception as e:
            _std_log.exception("Received unexpected exception from consumer %r", self)
            yield channel.basic_nack(delivery_tag=0, multiple=True, requeue=True)
            raise e
        else:
            _std_log.info(
                "Successfully consumed message from topic %s (message id %s)",
                message.topic,
                properties.message_id,
            )
            yield channel.basic_ack(delivery_tag=delivery_frame.delivery_tag)

    def _on_cancel_callback(self, frame):
        """
        Called when the consumer is canceled server-side.

        This can happen, for example, when the queue is deleted.
        To handle this, we do the necessary book-keeping to remove the consumer
        and then fire the errback on the consumer so the caller of
        :func:`fedora_messaging.api.consume` can decide what to do.

        Args:
            frame (pika.frame.Method): The cancel method from the server,
                unused here because we already know what consumer is being
                canceled.
        """
        _std_log.error("%r was canceled by the AMQP broker!", self)

        self._protocol._forget_consumer(self.queue)
        self._running = False
        self.result.errback(fail=ConsumerCanceled())

    def _read_loop_errback(self, failure):
        """
        Handle errors coming out of the read loop.

        There are two basic categories of errors: ones where the ``consumer.result``
        Deferred needs to be fired because the error is not recoverable, ones
        where we can recover from by letting the connection restart, and ones
        which are fatal for this consumer only (the queue was deleted by an
        administrator).

        Args:
            failure (twisted.python.failure.Failure): The exception raised by
                the read loop encapsulated in a Failure.
        """
        exc = failure.value
        if failure.check(pika.exceptions.ConsumerCancelled):
            # Pika 1.0.0+ raises this exception. To support previous versions
            # we register a callback (called below) ourselves with the channel.
            self._on_cancel_callback(None)
        elif failure.check(pika.exceptions.ChannelClosed):
            if exc.args[0] == 403:
                # This is a mis-configuration, the consumer can register itself,
                # but it doesn't have permissions to read from the queue,
                # so no amount of restarting will help.
                e = PermissionException(
                    obj_type="queue",
                    description=self.queue,
                    reason=failure.value.args[1],
                )
                self.result.errback(Failure(e, PermissionException))
                self.cancel()
            else:
                _std_log.exception(
                    "Consumer halted (%r) unexpectedly; "
                    "the connection should restart.",
                    failure,
                )
        elif failure.check(error.ConnectionDone, error.ConnectionLost):
            _std_log.warning(
                "The connection to the broker was lost (%r), consumer halted; "
                "the connection should restart and consuming will resume.",
                exc,
            )
        elif failure.check(pika.exceptions.AMQPError):
            _std_log.exception(
                "An unexpected AMQP error occurred; the connection should "
                "restart, but please report this as a bug."
            )
        else:
            self.result.errback(failure)
            self.cancel()

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
            self._protocol._forget_consumer(self.queue)
        except AttributeError:
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
