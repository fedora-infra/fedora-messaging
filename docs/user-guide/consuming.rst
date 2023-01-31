
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

    from fedora_messaging import api, config

    # The fedora_messaging API does not automatically configure logging so as
    # to not destroy application logging setup. This is a convenience method
    # to configure the Python logger with the fedora-messaging logging config.
    config.conf.setup_logging()

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
    # the queue and binding configurations in these dictionaries:
    queues = {
        'demo': {
            'durable': False,  # Delete the queue on broker restart
            'auto_delete': True,  # Delete the queue when the client terminates
            'exclusive': False,  # Allow multiple simultaneous consumers
            'arguments': {},
        },
    }
    binding = {
        'exchange': 'amq.topic',  # The AMQP exchange to bind our queue to
        'queue': 'demo',  # The unique name of our queue on the AMQP broker
        'routing_keys': ['#'],  # The topics that should be delivered to the queue
    }

    # Start consuming messages using our callback. This call will block until
    # a KeyboardInterrupt is raised, or the process receives a SIGINT or SIGTERM
    # signal.
    api.consume(printer_callback, bindings=binding, queues=queues)

In this example, there's one queue and the queue only has one binding, but it's
possible to consume from multiple queues and each queue can have multiple
bindings.


Command Line Interface
======================

A command line interface, :ref:`fm-cli`, is included to make running
consumers easier. It's not necessary to write any boilerplate code calling the
API, just run ``fedora-messaging consume`` and provide it the Python path to
your callback::

    $ fedora-messaging consume --callback=fedora_messaging.example:printer

Consult the manual page for complete details on this command line interface.

.. note:: For users of fedmsg, this is roughly equivalent to fedmsg-hub


Consumer API
============

The introduction contains a very minimal callback. This section covers the
complete API for consumers.

The Callback
------------

The callback provided to :func:`fedora_messaging.api.consume` or the command-line
interface can be any callable Python object, so long as it accepts the message
object as a single positional argument.

The API will also accept a Python class, which it will instantiate before
using as a callable object. This allows you to write a callback with easy
one-time initialization or a callback that maintains state between calls::

    import os

    from fedora_messaging import config


    class SaveMessage:
        """
        A fedora-messaging consumer that saves the message to a file.

        A single configuration key is used from fedora-messaging's
        "consumer_config" key, "path", which is where the consumer will save
        the messages::

            [consumer_config]
            path = "/tmp/fedora-messaging/messages.txt"
        """

        def __init__(self):
            """Perform some one-time initialization for the consumer."""
            self.path = config.conf["consumer_config"]["path"]

            # Ensure the path exists before the consumer starts
            if not os.path.exists(os.path.dirname(self.path)):
                os.mkdir(os.path.dirname(self.path))

        def __call__(self, message):
            """
            Invoked when a message is received by the consumer.

            Args:
                message (fedora_messaging.api.Message): The message from AMQP.
            """
            with open(self.path, "a") as fd:
                fd.write(str(message))

When running this type of callback from the command-line interface, specify
the Python path to the class object, not the ``__call__`` method::

    $ fedora-messaging consume --callback=package_name.module:SaveMessage


Exceptions
----------

* Consumers should raise the :class:`fedora_messaging.exceptions.Nack`
  exception if the consumer cannot handle the message at this time. The message
  will be re-queued, and the server will attempt to re-deliver it at a later
  time.

* Consumers should raise the :class:`fedora_messaging.exceptions.Drop` exception
  when they wish to explicitly indicate they do not want handle the message. This
  is similar to simply calling ``return``, but the server is informed the client
  dropped the message. What the server does depends on configuration.

* Consumers should raise the :class:`fedora_messaging.exceptions.HaltConsumer`
  exception if they wish to stop consuming messages.

If a consumer raises any other exception, a traceback will be logged at the
error level, the message being processed and any pre-fetched messages will be
returned to the queue for later delivery, and the consumer will be canceled.

If the CLI is being used, it will halt with a non-zero exit code. If the API
is being used directly, consult the API documentation for exact results, as
the synchronous and asynchronous APIs communicate failures differently.


Synchronous and Asynchronous Calls
----------------------------------

The AMQP consumer runs in a Twisted event loop. When a message arrives, it
calls the callback in a separate Python thread to avoid blocking vital
operations like the connection heartbeat. The callback is free to use any
blocking (synchronous) calls it likes.

.. note:: Your callback does not need to be thread-safe. By default, messages
          are processed serially.

It is safe to start threads to perform IO-blocking work concurrently. If you
wish to make use of a Twisted API, you must use the
:func:`twisted.internet.threads.blockingCallFromThread` or
:class:`twisted.internet.interfaces.IReactorFromThreads` APIs.

You may also use asyncio-based asynchronous callbacks, either via an ``async``
function or via an object that has an async ``__call__()`` method. In this
case, the callback will not be run in a separate thread, it will instead be
scheduled as a regular asyncio task.


Consumer Configuration
----------------------

A special section of the fedora-messaging configuration will be available for
consumers to use if they need configuration options. Refer to the
:ref:`conf-consumer-config` in the Configuration documentation for details.


systemd Service
===============

A systemd service file is also included in the Python package for your
convenience. It is called ``fm-consumer@.service`` and simply runs
``fedora-messaging consume`` with a configuration file from
``/etc/fedora-messaging/`` that matches the service name::

    $ systemctl start fm-consumer@sample.service  # uses /etc/fedora-messaging/sample.toml


.. _AMQP overview: https://www.rabbitmq.com/tutorials/amqp-concepts.html
.. _RabbitMQ tutorials: https://www.rabbitmq.com/getstarted.html
.. _pika: https://pika.readthedocs.io/
.. _bindings: https://www.rabbitmq.com/tutorials/amqp-concepts.html#bindings
.. _queue: https://www.rabbitmq.com/tutorials/amqp-concepts.html#queues
.. _AMQP consumers: https://www.rabbitmq.com/tutorials/amqp-concepts.html#consumers
.. _exchange: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges
.. _topic exchange: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchange-topic
