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

import json

import jsonschema
import pika
from pika.adapters import twisted_connection
from twisted.internet import interfaces, reactor, defer, protocol
from twisted.logger import Logger
from zope.interface import implementer

from .. import config
from ..message import get_class, Message
from ..exceptions import Nack, Drop, HaltConsumer


log = Logger()


@implementer(interfaces.IPushProducer)
class MessageProducer:
    """
    Twisted `Producer`_ that consumes messages from the AMQP server and passes
    them to a callback.

    .. Producer: https://twistedmatrix.com/documents/current/core/howto/producers.html

    Attributes:
        producing (Deferred): Deferred that will fire when production
            has stopped. The callback value is undefined for now. It is
            ``None`` when the production has not started yet.
    """

    def __init__(self, callback, bindings=None):
        """
        Initializes the producer.

        Args:
            callback (callable): the function that will be called with the
                received message as only argument.
            bindings (dict): the exchanges, queues and bindings to set. If
                ``None``, the configuration file will be used.
        """
        self._consumer_callback = callback
        self._bindings = bindings or config.conf["bindings"]
        self._parameters = pika.URLParameters(config.conf['amqp_url'])
        if self._parameters.client_properties is None:
            self._parameters.client_properties = config.conf['client_properties']
        self._connection = None
        self._channel = None
        self._running = False
        self.producing = None

    @defer.inlineCallbacks
    def resumeProducing(self):
        """
        Starts or resumes the connection to the server.

        This method starts receiving messages from the server, they will be
        passed to the consumer callback.

        If necessary, the connection is established, the channel is open and
        the configured queues and exchanges are declared and bound.

        Returns:
            Deferred: fired when the production is ready to start
        """
        # Connect to server
        if self._connection is None:
            cc = protocol.ClientCreator(
                reactor,
                twisted_connection.TwistedProtocolConnection,
                self._parameters
            )
            if self._parameters.ssl:
                # TODO: SSL certs?
                connectMethod = cc.connectSSL
            else:
                connectMethod = cc.connectTCP
            self._connection = yield connectMethod(self._parameters.host, self._parameters.port)
            yield self._connection.ready
            log.debug("Connected to AMQP server")
        # Create channel
        if self._channel is None:
            self._channel = yield self._connection.channel()
            log.debug("AMQP channel created")
            yield self._channel.basic_qos(prefetch_count=0, prefetch_size=0)
        # Declare exchanges and queues
        queues = set()
        for binding in self._bindings:
            yield self._channel.exchange_declare(
                exchange=binding['exchange'],
                exchange_type='topic', durable=True)
            result = yield self._channel.queue_declare(
                queue=binding['queue_name'],
                durable=True,
                auto_delete=binding.get("queue_auto_delete", False),
                arguments=binding.get('queue_arguments'),
            )
            queue_name = result.method.queue
            yield self._channel.queue_bind(
                queue=queue_name,
                exchange=binding['exchange'],
                routing_key=binding['routing_key'],
            )
            queues.add(queue_name)
        log.debug("AMQP bindings declared")
        # Start consuming
        queue_objects = []
        for queue_name in queues:
            queue_object, consumer_tag = yield self._channel.basic_consume(
                queue=queue_name)
            queue_objects.append(queue_object)
        self._running = True
        self.producing = defer.DeferredList([
            self._read(qo) for qo in queue_objects
        ])
        log.info('AMQP consumer is ready')

    @defer.inlineCallbacks
    def pauseProducing(self):
        """
        Pause the reception of messages. Does not disconnect from the server.

        Message reception can be resumed with :meth:`resumeProducing`.

        Returns:
            Deferred: fired when the production is paused.
        """
        if self._channel is None:
            return
        for tag in self._channel.consumer_tags:
            yield self._channel.basic_cancel(tag)
        self._running = True
        log.debug("Producing paused.")

    @defer.inlineCallbacks
    def stopProducing(self):
        """
        Stop producing messages and disconnect from the server.

        Returns:
            Deferred: fired when the production is stopped.
        """
        if not self._running:
            return
        if self._channel:
            log.info('Halting {tag} consumer sessions', tag=self._channel.consumer_tags)
        self._running = False
        yield self._connection.close()
        self._channel = None
        self._connection = None
        self.producing = None

    @defer.inlineCallbacks
    def _read(self, queue_object):
        while self._running:
            try:
                channel, delivery_frame, properties, body = yield queue_object.get()
            except pika.exceptions.ChannelClosed as e:
                log.error(repr(e))
                break
            except Exception:
                log.failure("Failed getting the next message in the queue. Stopping.")
                break
            if body:
                yield self._on_message(channel, delivery_frame, properties, body)
        if self._running:
            # We broke the loop, something went wrong
            self.stopProducing()

    @defer.inlineCallbacks
    def _on_message(self, channel, delivery_frame, properties, body):
        """
        Callback when a message is received from the server.

        This method wraps a user-registered callback for message delivery. It
        decodes the message body, determines the message schema to validate the
        message with, and validates the message before passing it on to the user
        callback.

        This also handles acking, nacking, and rejecting messages based on
        exceptions raised by the consumer callback. For detailed documentation
        on the user-provided callback, see the user guide on consuming.

        Args:
            channel (pika.channel.Channel): The channel from which the message
                was received.
            delivery_frame (pika.spec.Deliver): The delivery frame which includes
                details about the message like content encoding and its delivery
                tag.
            properties (pika.spec.BasicProperties): The message properties like
                the message headers.
            body (bytes): The message payload.

        Returns:
            Deferred: fired when the message has been handled.
        """
        log.debug('Message arrived with delivery tag {tag}',
                  tag=delivery_frame.delivery_tag)
        if properties.content_encoding is None:
            log.error('Message arrived without a content encoding')
            properties.content_encoding = 'utf-8'
        try:
            body = body.decode(properties.content_encoding)
        except UnicodeDecodeError:
            log.error(
                'Unable to decode message body {body!r} with {encoding!r} '
                'content encoding',
                body=body,
                encoding=delivery_frame.content_encoding
            )
        try:
            body = json.loads(body)
        except ValueError:
            log.failure('Failed to load message body {body!r}', body)
        try:
            MessageClass = get_class(properties.headers['fedora_messaging_schema'])
        except TypeError:
            log.warn(
                'Message (body={body!r}) arrived without headers. '
                'A publisher is misbehaving!', body=body
            )
            MessageClass = Message
        except KeyError:
            log.warn(
                'Message (headers={headers!r}, body={body!r}) arrived without '
                'a schema header. A publisher is misbehaving!',
                headers=properties.headers, body=body
            )
            MessageClass = Message

        message = MessageClass(
            body=body, headers=properties.headers, topic=delivery_frame.routing_key)
        try:
            message.validate()
            log.debug('Successfully validated message {msg!r}', msg=message)
        except jsonschema.exceptions.ValidationError:
            log.failure('Message validation of {msg!r} failed', message)
        try:
            log.info('Consuming message from topic {topic!r} (id {msgid})',
                     topic=message.topic,
                     msgid=properties.message_id)
            yield defer.maybeDeferred(self._consumer_callback, message)
            yield channel.basic_ack(delivery_tag=delivery_frame.delivery_tag)
        except Nack:
            log.info('Returning message id {msgid} to the queue',
                     msgid=properties.message_id)
            yield channel.basic_nack(delivery_tag=delivery_frame.delivery_tag, requeue=True)
        except Drop:
            log.info('Dropping message id {msgid}', msgid=properties.message_id)
            yield channel.basic_nack(delivery_tag=delivery_frame.delivery_tag, requeue=False)
        except HaltConsumer:
            log.warn('Consumer indicated it wishes consumption to halt, shutting down')
            yield self.stopProducing()
        except Exception:
            log.failure("Received unexpected exception from consumer callback")
            yield channel.basic_nack(delivery_tag=0, multiple=True, requeue=True)
            yield self.stopProducing()
