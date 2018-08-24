
.. _consumers:

=========
Consumers
=========

This library is aimed at making implementing a message consumer as simple as
possible by implementing common boilerplate code and offering a command line
interface to easily start a consumer as a service under init systems like
systemd.


Introduction
============

`AMQP consumers`_ configure a `queue`_ for their use in the message broker.
When a message is published to an `exchange`_ and matches the `bindings`_ the
consumer has declared, the message is placed in the queue and eventually
delivered to the consumer. Fedora uses a `topic exchange`_ for general-purpose
messages.

Fortunately, you don't need to manage the connection to the broker or configure
the queue. All you need to do is to implement some code to run when a message
is received. The API expects a callable object that accepts a single positional
argument::

    from fedora_messaging import api

    # First, define a function to be used as our callback. This will be called
    # whenever a message is received from the server.
    def printer_callback(message):
        """
        Print the message to standard output.

        Args:
            message (fedora_messaging.message.Message): The message we received
                from the queue.
        """
        print(str(message))

    # Next, we need a queue to consume messages from. We can define
    # the queue configuration in this dictionary
    binding = {
        'exchange': 'amq.topic',  # The AMQP exchange to bind our queue to
        'queue_name': 'demo',  # The unique name of our queue on the AMQP broker
        'routing_key': '#',  # The topics that should be delivered to the queue
    }

    # Start consuming messages using our callback. This call will block until
    # a KeyboardInterrupt is raised, or the process receives a SIGINT or SIGTERM
    # signal.
    api.consume(printer_callback, binding)

In this example, there's one queue and the queue only has one binding, but it's
possible to consume from multiple queues and each queue can have multiple
bindings.


Handling Errors
===============

It's the responsibility of the callback to handle general exceptions. When the
callback encounters an unrecoverable error, it can either opt to place the
message back in the queue for later processing or it can reject the message
which drops it.

To place the message back on the queue (a send "nack" to AMQP), raise the
:class:`fedora_messaging.exceptions.Nack` exception in your callback. To reject
the message and have it dropped, raise the
:class:`fedora_messaging.exceptions.Drop` exception.

If an unexpected exception is raised by the callable object, the API will log
the complete traceback, return all pre-fetched messages to the queue, cancel
the consumer, and exit.


Blocking Calls
==============

The consumer runs in an event loop. Other than the consumer, the only other
tasks in the event loop are connection management tasks, including regular
heartbeats to the broker. Blocking for too long will cause the broker to close
the connection, remove the consumer from the queue, and re-queue any
unacknowledged messages. This is recoverable, but has overhead and can result
in an excessive number of messages being processed multiple times.

Because of this, your callback should ensure there are reasonably short
timeouts on any blocking calls (less than 30 seconds). If timeouts occur,
simply raise the :class:`fedora_messaging.exceptions.Nack` and try again later.


Halting
=======

Consumers can signal that they would like to stop consuming messages by raising
the :class:`fedora_messaging.exceptions.HaltConsumer` exception. When stopping
the message can either be re-queued for reprocessing at a later time, or marked
as successfully processed. Consult the API documentation of ``HaltConsumer`` for
details.


Consumer Configuration
======================

A special section of the configuration will be available for consumers to use
if they need configuration options. Refer to the :ref:`sub-config` in the
Configuration documentation for details.


State Across Messages
=====================

Some consumers need to store state across messages. To do this, you can
implement your consumer callback as a class. The
:class:`fedora_messaging.api.consume` API will create an instance of the class
and use that as the callable. The ``__init__`` function of the class should
accept no arguments and rely on the configuration in
:ref:`conf-consumer-config` for initialization. It must also define the
``__call__`` method which accepts the message as its argument. This will be
called when a message arrives::

    from fedora_messaging import api, config

    class PrintMessage(object):
        """
        A fedora-messaging consumer that prints the message to stdout.

        A single configuration key is used from fedora-messaging's "consumer_config"
        key, "summary", which should be a boolean. If true, just the message summary
        is printed. Place the following in your fedora-messaging configuration file::

            [consumer_config]
            summary = true

        The default is false.
        """

        def __init__(self):
            try:
                self.summary = config.conf['consumer_config']['summary']
            except KeyError:
                self.summary = False

        def __call__(self, message):
            """
            Invoked when a message is received by the consumer.

            Args:
                message (fedora_messaging.api.Message): The message from AMQP.
            """
            if self.summary:
                print(message.summary)
            else:
                print(message)

    api.consume(PrintMessage)


.. _AMQP overview: https://www.rabbitmq.com/tutorials/amqp-concepts.html
.. _RabbitMQ tutorials: https://www.rabbitmq.com/getstarted.html
.. _pika: https://pika.readthedocs.io/
.. _bindings: https://www.rabbitmq.com/tutorials/amqp-concepts.html#bindings
.. _queue: https://www.rabbitmq.com/tutorials/amqp-concepts.html#queues
.. _AMQP consumers: https://www.rabbitmq.com/tutorials/amqp-concepts.html#consumers
.. _exchange: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges
.. _topic exchange: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchange-topic
