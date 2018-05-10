.. _messages:

========
Messages
========

Before release your application, you should create a subclass of
:class:`fedora_messaging.message.Message`, define a schema, and implement
some methods.

Schema
======

Defining a message schema is important for several reasons.

First and foremost, if will help you (the developer) ensure you don't
accidentally change your message's format. When messages are being generated
from, say, a database object, it's easy to make a schema change to the database
and unintentionally alter your message, which breaks consumers of your message.
Without a schema, you might not catch this until you deploy your application
and consumers start crashing. With a schema, you'll get an error as you
develop!

Secondly, it allows you to change your message format in a controlled fashion
by versioning your schema. You can then choose to implement methods one way or
another based on the version of the schema used by a message.

Message schema are defined using `JSON Schema`_.


.. _header-schema:

Header Schema
-------------

The default header schema simply declares that the header field must be a JSON
object. You can leave the schema as-is when you define your own message, or
refine it.


.. _body-schema:

Body Schema
-----------

The default body schema simply declares that the header field must be a JSON
object.


Example
=======

An example is worth a thousand words::

    from fedora_messaging import message

    class MailmanMessage(message.Message):
        """
        A sub-class of a Fedora message that defines a message schema for
        messages published by Mailman when it receives mail to send out.
        """
        body_schema = {
            'id': 'http://fedoraproject.org/message-schema/mailman#',
            '$schema': 'http://json-schema.org/draft-04/schema#',
            'description': 'Schema for message sent to mailman',
            'type': 'object',
            'properties': {
                'mlist': {
                    'type': 'object',
                    'properties': {
                        'list_name': {
                            'type': 'string',
                            'description': 'The name of the mailing list',
                        },
                    }
                },
                'msg': {
                    'description': 'An object representing the email',
                    'type': 'object',
                    'properties': {
                        'delivered-to': {'type': 'string'},
                        'from': {'type': 'string'},
                        'cc': {'type': 'string'},
                        'to': {'type': 'string'},
                        'x-mailman-rule-hits': {'type': 'string'},
                        'x-mailman-rule-misses': {'type': 'string'},
                        'x-message-id-hash': {'type': 'string'},
                        'references': {'type': 'string'},
                        'in-reply-to': {'type': 'string'},
                        'message-id': {'type': 'string'},
                        'subject': {'type': 'string'},
                    },
                    'required': ['from', 'to'],
                },
            },
            'required': ['mlist', 'msg'],
        }

    # Define a valid message body
    sample_message = {
        "mlist": {"list_name": "infrastructure"},
        "msg": {
            "archived-at": "<https://lists.fedoraproject.org/cold/storage>",
            "from": "JD <jd@example.com>",
            "subject": "A sample email",
            "to": "infrastructure@lists.fedoraproject.org",
            "x-message-id-hash": "NOTREALLYAHASH",
        }
    }
    msg = MailmanMessage(body=sample_message)
    msg.validate()

    # Delete a required property and watch it fail
    del msg.body['msg']['from']
    msg.validate()

Note that message schema can be composed of other message schema, and
validation of fields can be much more detailed than just a simple type check.
Consult the `JSON Schema`_ documentation for complete details.

.. _JSON Schema: http://json-schema.org/
