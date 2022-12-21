
.. _publishing:

==========
Publishing
==========

Overview
========

Publishing messages is simple. Messages are made up of a topic, some optional
headers, and a body. Messages are encapsulated in a
:class:`fedora_messaging.message.Message` object. For details on defining
messages, see the :ref:`messages` documentation. For details on the publishing
API, see the :ref:`pub-api` API documentation.

Topics
------

`Topics`_ are strings of words separated by the ``.`` character, up to 255
characters. Topics are used by clients to filter messages, so choosing a good
topic helps reduce the number of messages sent to a client. Topics should start
broadly and become more specific.


Headers
-------

Headers are key-value pairs attached that are useful for storing information
about the message itself. This library adds a header to every message with the
``fedora_messaging_schema`` key, pointing to the message schema used.

You should not use any key starting with ``fedora_messaging`` for yourself.

You can write :ref:`header-schema` for your messages to enforce a particular
schema.


Body
----

The only restrictions on the message body is that it must be serializable to a
JSON object. You should write a :ref:`body-schema` for your messages to ensure
you don't change your message format unintentionally.


Introduction
============

To publish a message, first create a :class:`fedora_messaging.message.Message`
object, then pass it to the :func:`fedora_messaging.api.publish` function::

    from fedora_messaging import api, message

    msg = message.Message(topic=u'nice.message', headers={u'niceness': u'very'},
                          body={u'encouragement': u"You're doing great!"})
    api.publish(msg)

The API relies on the :ref:`config` you've provided to connect to the message
broker and publish the message to an exchange.


Handling Errors
===============

Your message might fail to publish for a number of reasons, so you should be
prepared to see (and potentially handle) some errors.


Validation
----------

The message you create may not be successfully validated against its schema.
This is not an error you should catch, since it must be fixed by the developer
and cannot be recovered from.


Connection Errors
-----------------

The publish API will attempt to reconnect to the broker several times before an
exception is raised. Once this occurs it is up to the application to decide what
to do.


Rejected Messages
-----------------

The broker may reject a message. This could occur because the message is too
large, or because the publisher does not have permission to publish messages
with a particular topic, or some other reason.


.. _topics: https://www.rabbitmq.com/amqp-0-9-1-reference.html#queue.bind.routing-key
