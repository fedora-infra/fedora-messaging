==============
Message Format
==============

This documentation covers the format of AMQP messages sent by this library. If
you are interested in using a language other than Python to send or receive
messages sent by Fedora applications, this document is for you.


Overview
========

Messages are AMQP `Basic <https://www.rabbitmq.com/amqp-0-9-1-reference.html>`_
content. Basic messages have the content type, content encoding, a table of
headers, delivery mode, priority, correlation ID, reply-to, expiration, message
ID, timestamp, type, user ID, and app ID fields.

Your messages *MUST* have a content-type of ``application/json`` and a
content-encoding of ``utf-8``. The message ID should be a `version 4 UUID
<https://www.ietf.org/rfc/rfc4122.txt>`_.

Headers
=======

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
