=========
Consumers
=========

This library is aimed at making implementing a message consumer as simple as
possible by implementing common boilerplate code and offering a command line
interface to easily start a consumer as a service under init systems like
systemd.

Overview
========

TODO give a brief overview of the players (queues, bindings, point to the
message class)

.. note:: If you're not familiar with AMQP, the `AMQP overview`_  from RabbitMQ is an
          excellent place to start.

Introduction
============

The first step is to implement some code to run when a message is received. The
API expects a callable object that accepts a single positional argument::

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
:class:`fedora_messaging.exceptions.Reject` exception.

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


State Across Messages
=====================

Some consumers need to store state across messages. To do this, simply add a
keyword argument with a mutable default to your callback.  This will be
initialized once when the function is defined and then provided with every
call. As a concrete example, suppose you wish to track how many messages you've
received with a certain topic::

    from collections import defaultdict

    def callback_with_storage(message, counter=defaultdict(int)):
        counter[message.topic] += 1
        print("We've seen {} messages on the {} topic!".format(
            counter[message.topic], message.topic))

Keep in mind that it's up to the callback to ensure the storage doesn't use up
all the available memory. It's not a good idea, for example, to keep a reference
to all the messages the consumer has ever received.


Consumer Configuration
======================

A special section of the configuration will be available for consumers to use
if they need configuration options. Refer to the :ref:`sub-config` in the
Configuration documentation for details.


Using the Command Line
======================

TODO: finalize CLI flags and write a man page with sphinx


.. _AMQP overview: https://www.rabbitmq.com/tutorials/amqp-concepts.html
.. _RabbitMQ tutorials: https://www.rabbitmq.com/getstarted.html
.. _pika: https://pika.readthedocs.io/
