==========
Publishing
==========

Overview
========

Publishing messages is simple. Messages are made up of a topic, some optional
headers, and a body. Messages are encapsulated in a
:class:`fedora_messaging.message.Message` object. For details on defining
messages, see the :ref:`messages` documentation. For details on the publishing
API, see the :ref:`pub-api` documentation.

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
following keys:

* ``fedora_messaging_schema`` - This is used to identify the message schema
  used.

* ``fedora_messaging_schema_version`` - This identifies the version of the
  message schema.

You should not use these keys for yourself.

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

The message create may not be successfully validated against its schema. This
is not an error you should catch, since it must be fixed by the developer and
cannot be recovered from.

TODO document other errors, timeouts, etc.

.. _topics: https://www.rabbitmq.com/amqp-0-9-1-reference.html#queue.bind.routing-key
