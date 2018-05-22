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
import logging
import signal
import uuid

import pika
import jsonschema

from . import config
from .message import _schema_name, get_class, Message
from .signals import pre_publish_signal, publish_signal, publish_failed_signal
from .exceptions import Nack, Drop, HaltConsumer, ValidationError

_log = logging.getLogger(__name__)


class PublisherSession(object):
    """A session with blocking APIs for publishing to an AMQP broker."""

    def __init__(self, amqp_url=None, exchange=None, confirms=True):
        self._exchange = exchange or config.conf['publish_exchange']
        self._parameters = pika.URLParameters(config.conf['amqp_url'])
        if self._parameters.client_properties is None:
            self._parameters.client_properties = config.conf['client_properties']
        self._confirms = confirms
        # TODO break this out to handle automatically reconnecting and failing over, errors, etc.
        self._connection = pika.BlockingConnection(self._parameters)
        self._channel = self._connection.channel()
        if self._confirms:
            self._channel.confirm_delivery()

    def publish(self, message, exchange=None):
        """
        Publish a :class:`fedora_messaging.message.Message` to an `exchange`_ on
        the message broker.

        This is a blocking API that will retry a configurable number of times to
        publish the message.


        >>> from fedora_messaging import session, message
        >>> msg = message.Message(topic='test', body={'test':'message'})
        >>> sess = session.BlockingSession()
        >>> sess.publish(msg)

        Args:
            message (message.Message): The message to publish.
            exchange (str): The name of the AMQP exchange to publish to; defaults
                to :ref:`conf-publish-exchange`
        Raises:
            exceptions.PublishError: If publishing failed.

        .. _exchange: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges
        """
        pre_publish_signal.send(self, message=message)

        # Consumers use this to determine what schema to use and if they're out of date
        message.headers['fedora_messaging_schema'] = _schema_name(message.__class__)
        message.headers['fedora_messaging_schema_version'] = message.schema_version

        # Since the message can be mutated by signal listeners, validate just before sending
        message.validate()

        properties = pika.BasicProperties(
            content_type='application/json', content_encoding='utf-8', delivery_mode=2,
            headers=message.headers, message_id=str(uuid.uuid4()))
        try:
            self._channel.publish(exchange or self._exchange, message.topic.encode('utf-8'),
                                  json.dumps(message.body).encode('utf-8'), properties)
            publish_signal.send(self, message=message)
            # TODO actual error handling
        except pika.exceptions.NackError as e:
            _log.error('Message got nacked!')
            publish_failed_signal.send(self, message=message, error=e)
        except pika.exceptions.UnroutableError as e:
            _log.error('Message is unroutable!')
            publish_failed_signal.send(self, message=e, error=e)
        except pika.exceptions.ConnectionClosed as e:
            _log.warning('Connection closed to %s; attempting to reconnect...',
                         self._parameters.host)
            self._connection = pika.BlockingConnection(self._parameters)
            self._channel = self._connection.channel()
            if self._confirms:
                self._channel.confirm_delivery()
            _log.info('Successfully opened connection to %s', self._parameters.host)
            self._channel = self._connection.channel()
            self._channel.publish(exchange or self._exchange, message.topic.encode('utf-8'),
                                  json.dumps(message.body).encode('utf-8'), properties)
            publish_signal.send(self, message=message)


class ConsumerSession(object):
    """A session using the asynchronous APIs offered by Pika."""

    def __init__(self, retries=-1, retry_max_interval=60):
        self._parameters = pika.URLParameters(config.conf['amqp_url'])
        if self._parameters.client_properties is None:
            self._parameters.client_properties = config.conf['client_properties']
        self._connection = None
        self._channel = None
        self._bindings = {}
        self._retries = retries
        self._max_retry_interval = retry_max_interval
        self._retry_interval = 3
        self._retries_left = self._retries
        self._current_retry_interval = self._retry_interval
        self._running = False

        def _signal_handler(signum, frame):
            """
            Signal handler that gracefully shuts down the consumer

            Args:
                signum (int): The signal this process received.
                frame (frame): The current stack frame (unused).
            """
            if signum in (signal.SIGTERM, signal.SIGINT):
                self._shutdown()

        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)

    def _shutdown(self):
        """Gracefully shut down the consumer and exit."""
        if self._channel:
            _log.info('Halting %r consumer sessions', self._channel.consumer_tags)
        self._running = False
        self._connection.close()

    def _on_cancelok(self, cancel_frame):
        """
        Called when the server acknowledges a cancel request.

        Args:
            cancel_frame (pika.spec.Basic.CancelOk): The cancelok frame from
                the server.
        """
        _log.info('Consumer canceled; returning all unprocessed messages to the queue')
        self._channel.basic_nack(delivery_tag=0, multiple=True, requeue=True)

    def _on_channel_open(self, channel):
        """
        Callback used when a channel is opened.

        This registers all the channel callbacks.

        Args:
            channel (pika.channel.Channel): The channel that successfully opened.
        """
        channel.add_on_close_callback(self._on_channel_close)
        channel.add_on_cancel_callback(self._on_cancel)

        channel.basic_qos(self._on_qosok, **config.conf['qos'])

    def _on_qosok(self, qosok_frame):
        """
        Callback invoked when the server acknowledges the QoS settings.

        Asserts or creates the exchanges and queues exist.

        Args:
            qosok_frame (pika.spec.Basic.Qos): The frame send from the server.
        """
        for name, args in self._exchanges.items():
            self._channel.exchange_declare(
                self._on_exchange_declareok, name, exchange_type=args['type'],
                durable=args['durable'], auto_delete=args['auto_delete'],
                arguments=args['arguments'])
        for name, args in self._queues.items():
            self._channel.queue_declare(
                self._on_queue_declareok, queue=name, durable=args['durable'],
                auto_delete=args['auto_delete'], exclusive=args['exclusive'],
                arguments=args['arguments'])

    def _on_channel_close(self, channel, reply_code, reply_text):
        """
        Callback invoked when the channel is closed.

        Args:
            channel (pika.channel.Channel): The channel that got closed.
            reply_code (int): The AMQP code indicating why the channel was closed.
            reply_text (str): The human-readable reason for the channel's closure.
        """
        _log.info('Channel %r closed (%d): %s', channel, reply_code, reply_text)
        self._channel = None

    def _on_connection_open(self, connection):
        """
        Callback invoked when the connection is successfully established.

        Args:
            connection (pika.connection.SelectConnection): The newly-estabilished
                connection.
        """
        _log.info('Successfully opened connection to %s', connection.params.host)
        self._current_retries = self._retries
        self._current_retry_interval = self._retry_interval
        self._channel = connection.channel(on_open_callback=self._on_channel_open)

    def _on_connection_close(self, connection, reply_code, reply_text):
        """
        Callback invoked when a previously-opened connection is closed.

        Args:
            connection (pika.connection.SelectConnection): The connection that
                was just closed.
            reply_code (int): The AMQP code indicating why the connection was closed.
            reply_text (str): The human-readable reason the connection was closed.
        """
        self._channel = None
        if reply_code == 200:
            # Normal shutdown, exit the consumer.
            _log.info('Server connection closed (%s), shutting down', reply_text)
        else:
            _log.warning('Connection to %s closed unexpectedly (%d): %s',
                         connection.params.host, reply_code, reply_text)
            self._reconnect()

    def _on_connection_error(self, connection, error_message):
        """
        Callback invoked when the connection failed to be established.

        Args:
            connection (pika.connection.SelectConnection): The connection that
                failed to open.
            error_message (str): The reason the connection couldn't be opened.
        """
        self._channel = None
        _log.error(error_message)
        self._reconnect()

    def _reconnect(self):
        """
        Reconnect to the broker, a configurable number of times, with a backoff.

        The connection object has a reconnect setting with an interval, but this
        backs off up to a configurable limit.
        """
        if self._retries_left != 0:
            _log.info('Reconnecting in %d seconds', self._current_retry_interval)
            self._connection.add_timeout(
                self._current_retry_interval, self._connection.connect)
            self._retries_left -= 1
            self._current_retry_interval *= 2
            if self._current_retry_interval > self._max_retry_interval:
                self._current_retry_interval = self._max_retry_interval

    def _on_exchange_declareok(self, declare_frame):
        """
        Callback invoked when an exchange is successfully declared.

        It will declare the queues in the bindings dictionary with the
        :meth:`_on_queue_declareok` callback.

        Args:
            frame (pika.spec.Exchange.DeclareOk): The DeclareOk frame from the
                server.
        """
        _log.info('Exchange declared successfully')

    def _on_queue_declareok(self, frame):
        """
        Callback invoked when a queue is successfully declared.

        Args:
            frame (pika.frame.Method): The message sent from the server.
        """
        _log.info('Successfully declared the %s queue', frame.method.queue)
        for binding in self._bindings:
            if binding['queue'] == frame.method.queue:
                for key in binding['routing_keys']:
                    _log.info('Asserting %s is bound to %s with the %s key',
                              binding['queue'], binding['exchange'], key)
                    self._channel.queue_bind(None, binding['queue'],
                                             binding['exchange'], key)
                self._channel.basic_consume(self._on_message, frame.method.queue)

    def _on_cancel(self, cancel_frame):
        """
        Callback used when the server sends a consumer cancel frame.

        Args:
            cancel_frame (pika.spec.Basic.Cancel): The cancel frame from
                the server.
        """
        _log.info('Server canceled consumer')

    def consume(self, callback, bindings=None, queues=None, exchanges=None):
        """
        Consume messages from a message queue.

        Simply define a callable to be used as the callback when messages are
        delivered and specify the queue bindings. This call blocks. The callback
        signature should accept a single positional argument which is an
        instance of a :class:`Message` (or a sub-class of it).

        Args:
            callback (callable): The callable to pass the message to when one
                arrives.
            bindings (dict): A dictionary of bindings for queues. Refer to the
                :ref:`conf-bindings` configuration documentation for the format.
            queues (dict): A dictionary of queues to ensure exist. Refer to the
                :ref:`conf-queues` configuration documentation for the format.
            exchanges (dict): A dictionary of exchanges to ensure exist. Refer
                to the :ref:`conf-exchanges` configuration documentation for the
                format.

        Raises:
            HaltConsumer: Raised when the consumer halts.
            ValidationError: When a message fails schema validation.
        """
        self._connection = pika.SelectConnection(
            self._parameters,
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_error,
            on_close_callback=self._on_connection_close,
            stop_ioloop_on_close=True,
        )
        self._bindings = bindings or config.conf['bindings']
        self._queues = queues or config.conf['queues']
        self._exchanges = exchanges or config.conf['exchanges']
        self._consumer_callback = callback
        self._running = True
        while self._running:
            self._connection.ioloop.start()

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

        Raises:
            HaltConsumer: Raised when the consumer halts.
            ValidationError: When a message fails schema validation.
        """
        _log.debug('Message arrived with delivery tag %s', delivery_frame.delivery_tag)
        try:
            MessageClass = get_class(properties.headers['fedora_messaging_schema'])
        except KeyError:
            _log.error('Message (headers=%r, body=%r) arrived without a schema header.'
                       ' A publisher is misbehaving!', properties.headers, body)
            MessageClass = Message

        if properties.content_encoding is None:
            _log.error('Message arrived without a content encoding')
            properties.content_encoding = 'utf-8'
        try:
            body = body.decode(properties.content_encoding)
        except UnicodeDecodeError:
            _log.error('Unable to decode message body %r with %s content encoding',
                       body, delivery_frame.content_encoding)

        try:
            body = json.loads(body)
        except ValueError as e:
            _log.error('Failed to load message body %r, %r', body, e)
            raise ValidationError(e)

        message = MessageClass(
            body=body, headers=properties.headers, topic=delivery_frame.routing_key)
        try:
            message.validate()
            _log.debug('Successfully validated message %r', message)
        except jsonschema.exceptions.ValidationError as e:
            _log.error('Message validation of %r failed: %r', message, e)
            raise ValidationError(e)

        try:
            _log.info('Consuming message from topic "%s" (id %s)', message.topic,
                      properties.message_id)
            self._consumer_callback(message)
            channel.basic_ack(delivery_tag=delivery_frame.delivery_tag)
        except Nack:
            _log.info('Returning message id %s to the queue', properties.message_id)
            channel.basic_nack(delivery_tag=delivery_frame.delivery_tag, requeue=True)
        except Drop:
            _log.info('Dropping message id %s', properties.message_id)
            channel.basic_nack(delivery_tag=delivery_frame.delivery_tag, requeue=False)
        except HaltConsumer as e:
            _log.info('Consumer requested halt, returning messages to queue')
            channel.basic_nack(delivery_tag=delivery_frame.delivery_tag, requeue=True)
            self._shutdown()
            if e.exit_code != 0:
                raise
        except Exception as e:
            _log.exception("Received unexpected exception from consumer callback")
            channel.basic_nack(delivery_tag=0, multiple=True, requeue=True)
            self._shutdown()
            raise HaltConsumer(exit_code=1, reason=e)
