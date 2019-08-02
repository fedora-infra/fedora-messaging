==============
Message Format
==============

This documentation covers the format of AMQP messages sent by this library. If
you are interested in using a language other than Python to send or receive
messages sent by Fedora applications, this document is for you.

Messages are AMQP `Basic <https://www.rabbitmq.com/amqp-0-9-1-reference.html>`_
content. Basic messages have the content type, content encoding, a table of
headers, delivery mode, priority, correlation ID, reply-to, expiration, message
ID, timestamp, type, user ID, and app ID fields.


Content Type
============

Your messages *MUST* have a content-type of ``application/json`` and they must
be JSON objects. Consult the :ref:`messages` documentation for details on
message format.

Content Encoding
================

Your message *MUST* have the content-encoding property set to ``utf-8`` and they
must be encoding with UTF-8.

Message ID
==========

The message ID field *MUST* be a `version 4 UUID
<https://www.ietf.org/rfc/rfc4122.txt>`_ as a standard hexadecimal digit string
(e.g. f81d4fae-7dec-11d0-a765-00a0c91e6bf6).

Delivery Mode
=============

The delivery mode of your message *SHOULD* be 2 (persistent) unless you know
what you are doing and have a very good reason for setting it to 1 (transient).

Headers
=======

The headers field of AMQP message allows you to set a dictionary (map) of
arbitrary strings. Several header keys are used by Fedora's applications to
determine the message schema, the importance of the message for human beings,
when it was originally sent by the application, what packages or users it
relates to, and more.

Required
--------

Messages must have, at a minimum, the ``fedora_messaging_severity``,
``fedora_messaging_schema``, and ``sent-at`` keys.

The ``fedora_messaging_severity`` key should be set to an integer that
indicates the importance of the message to an end user, with 10 being
debug-level information, 20 being informational, 30 being warning-level, and 40
being critically important.

The ``fedora_messaging_schema`` key should be set to a string that uniquely
identifies the type of message. In the Python library this is the entry point
name, which is mapped to a class containing the schema and a Python API to
interact with the message object.

The ``sent-at`` key should be a ISO8601 date time that should include the UTC
offset and should *not* include microseconds. For example:
``2019-07-30T19:12:22+00:00``.

The header's json-schema is::

   {
      "$schema": "http://json-schema.org/draft-04/schema#",
      "description": "Schema for message headers",
      "type": "object",
      "properties": {
         "fedora_messaging_severity": {
            "type": "number",
            "enum": [10, 20, 30, 40],
         },
         "fedora_messaging_schema": {"type": "string"},
         "sent-at": {"type": "string"},
      },
   }


Optional
--------

In addition to the required headers, there are a number of optional headers you
can set that have special meaning. The general format of these headers is
``fedora_messaging_<object>_<id>`` where the ``<object>`` is one of ``user``,
``rpm``, ``container``, ``module``, or ``flatpak`` and ``<id>`` uniquely
identifies the object. Set these headers when the message pertains to the
referenced object.

For example, if the user ``jcline`` submitted a build for the ``python-requests``
RPM, the message about that event would have ``fedora_messaging_user_jcline``
and ``fedora_messaging_rpm_python-requests`` set.

At this time the value of the header key is not used and should always be set to
a Boolean value of true.

Body
====

The message body must match the content-type and content-encoding. That is, it
must be UTF-8 encoded JSON. Additionally, it must be a JSON Object. Beyond
that, there are no restrictions. Messages should be validated using their JSON
schema.  If you are publishing a new message type, please write a json-schema
for it and provide it to the Fedora infrastructure team. It will be distributed
to applications that wish to consume the message.
