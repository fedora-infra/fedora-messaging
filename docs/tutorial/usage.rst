Using the API
=============

.. highlight:: python

We will be creating some scripts to publish and subscribe to the bus. First,
create a directory to hold the code you will write, than change to this
directory.

Publishing
----------

To publish on the Fedora Messaging bus, you just need to use the
:py:func:`fedora_messaging.api.publish` function, passing it an
instance of the :py:class:`fedora_messaging.message.Message` class
that represents the message you want to publish.

A message has a schema, a topic, a severity, a body, and a set of headers.
We'll cover the schema later in this tutorial. The headers and the body are
Python dictionaries with JSON-serializable values. The topic is a string
containing elements separated by dots that will be used to route messages.

Create a publishing script called ``publish.py``::

    #!/usr/bin/env python3

    from fedora_messaging.api import publish, Message
    from fedora_messaging.config import conf

    conf.setup_logging()
    message = Message(
        topic="tutorial.topic",
        body={"reason": "test message"}
    )
    publish(message)

Of course, you can make a smarter script that will use command-line arguments,
this is left as an exercice to the reader. Now run it::

    chmod +x publish.py
    ./publish.py

The script should complete without error. If you go to RabbitMQ's web
interface, you'll see that a message has been sent to the ``amq.topic``
exchange. However, since noone is listening to this topic, the message has been
discarded. Now, we'll setup listeners.

Listening
---------

Clients listen on the Fedora Messaging bus by subscribing to a topic or a topic
pattern using the hash (``#``) symbol as a wildcard. For exemple you can
subscribe to ``bodhi.updates.kernel`` but also to ``bodhi.updates.#``. In the
former case you'll get kernel updates, in the latter case you'll get all Bodhi
updates.

After subscription, all messages with a topic matching the pattern will be
routed to a queue on the server, and clients will consume messages from this
queue. In the AMQP language, this is called *binding* a queue to an exchange,
and the topic pattern is called the *routing_key*.

In the configuration file, the ``bindings`` section controls which queues will
be subscribed to which topic patterns. Edit the file so the option looks like
this::

    [[bindings]]
    queue = "tutorial"
    exchange = "amq.topic"
    routing_keys = ["tutorial.#"]

This means that the queue named ``tutorial`` will be created and subcribed to
the ``amq.topic`` exchange using the ``tutorial.#`` pattern. All messages with
a topic starting with ``tutorial.`` will end up in this queue, and no other.

Now configure this new queue's properties in the file using a snippet that
looks like this::

    [queues.tutorial]
    durable = true
    auto_delete = false
    exclusive = false
    arguments = {}

This means that messages in this queue will survive a client's disconnection
and a server restart, and that multiple client can connect to it simultaneously
to consume messages in a round-robin fashion.

.. _consume-script:

Python script
~~~~~~~~~~~~~
Now create the following script, called ``consume.py``::

    #!/usr/bin/env python3

    from fedora_messaging.api import consume
    from fedora_messaging.config import conf

    def print_message(message):
        print(message)

    if __name__ == "__main__":
        conf.setup_logging()
        consume(print_message)

The script should run and wait for new messages. Now run the ``publish.py``
script again in another terminal (remember to activate the virtualenv with
``workon fedora-messaging-tutorial``). You should see the message being printed
where the ``consume.py`` script is running.

Python callback
~~~~~~~~~~~~~~~
You can also just define the callback function and use the ``fedora-messaging``
command-line tool to do the listening::

    fedora-messaging consume --callback="consume:print_message"

This should behave identically.

Round robin
~~~~~~~~~~~
When multiple programs are simulaneously consuming from the same queue, they
get the messages in a round-robin fashion. Try running another instance of the
``consume.py`` script, and run the ``publish.py`` script multiple times. You'll
see that ``consume.py`` instances get a message one after the other.

