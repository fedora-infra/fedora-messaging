JSON schemas
============

.. highlight:: python

Message bodies are JSON objects, that adhere to a schema. Message schemas live
in their own Python package, so they can be installed on the producer and on
the consumer.

In Fedora Messaging, we follow the `JSON Schema`_ standard, and use the
`jsonschema`_ library.

.. _JSON Schema: http://json-schema.org/
.. _jsonschema: https://python-jsonschema.readthedocs.io/


Creating the schema package
---------------------------

Copy the ``docs/sample_schema_package/`` directory from the
``fedora-messaging`` git clone to your app directory.

Edit the ``setup.py`` file to change the package metadata. Rename the
``mailman_schema`` directory to something relevant to your app, like
``yourapp_message_schemas``. There is no naming convention at the moment.
Edit the ``README`` file too.


Writing the schema
------------------

JSON objects are converted to dictionaries in Python. Writing a JSON schema
with the `jsonschema`_ library means writing a Python dictionary that will
describe the message's JSON object body. Read up on the `jsonschema`_ library
documentation if you have questions about the format.

Open the ``schema.py`` file, it contains an example schema for
Mailman-originating messages on the bus. The schema is a Python class
containing an important dictionary attribute: ``body_schema``. This is where
the JSON schema lives.

For clarity, edit the ``setup.py`` file and in the entry points list change the
``mailman.messageV1`` name to something more relevant to your app, like
``yourapp.my_messageV1``. The entry point name needs to be unique to your
application, so it's best to prefix it with your package or application name.

Schema format
~~~~~~~~~~~~~
This dictionary describes the possible keys and types in the JSON object being
validated, using the following reserved keys:

- ``id`` (or ``$id``): an URI identifing this schema. Change the last part of
  the example URL to use your app's name.
- ``$schema``: an URI describing the validator to use, you can leave that one
  as it is. It is only present at the root of the dictionary.
- ``description``: a fulltext description of the key.
- ``type``: the value type for this key. You can choose among:
  - ``null``: equivalent to ``None``
  - ``boolean``: equivalent to ``True`` or ``False``
  - ``object``: a Python dictionary
  - ``array``: a Python list
  - ``number``: an ``int`` or a ``float``
  - ``string``: a Python string
- ``properties``: a dictionary describing the possible keys contained in the
  JSON object, where keys are possible key names, and values are JSON schemas.
  Those schemas can also have ``properties`` keys to describe all the possible
  nested keys.
- ``required``: a list of keys that must be present in the JSON object.
- ``format``: a format validation type. You can choose among:
  - hostname
  - ipv4
  - ipv6
  - email
  - uri (requires the ``rfc3987`` package)
  - date
  - time
  - date-time (requires the ``strict-rfc3339`` package)
  - regex
  - color (requires the ``webcolors`` package)

For information on creating JSON schemas to validate your data, there is a good
introduction to JSON Schema fundamentals underway at `Understanding JSON
Schema`_.

.. _`Understanding JSON Schema`: https://spacetelescope.github.io/understanding-json-schema/

Example
~~~~~~~
Now edit the ``body_schema`` key to use the following schema::

    {
        'id': 'http://fedoraproject.org/message-schema/fedora-messaging-tutorial#',
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'description': 'Schema for the Fedora Messaging tutorial',
        'type': 'object',
        'properties': {
            'package': {
                'type': 'object',
                'properties': {
                    'name': {
                        'type': 'string',
                        'description': 'The name of the package',
                    },
                    'version': {'type': 'string'},
                }
                'required': ['name'],
            },
            'owner': {
                'description': 'The owner of the package',
                'type': 'string',
            },
        },
        'required': ['package', 'owner'],
    }

Human readable representation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The schema class also contains a few methods to extract relevant information
from the message, or to create a human-readable representation.

Change the ``__str__()`` method to use the expected items from the message body. For example::

    return '{owner} did something to the {package} package'.format(
        owner=self.body['owner'], package=self.body['package']['name'])

Also edit the ``summary`` property to return something relevant.


Severity
~~~~~~~~

Messages can also have a severity level. This is used by consumers to determine
the importance of a message to an end user. The possibly severity levels are
defined in the :ref:`message-severity` API documentation.

You should set a reasonable default for your messages.


Testing it
----------

JSON schemas can also be unit-tested. Check out the ``tests/test_schema.py``
file and write the unit tests that are appropriate for the message schema and
the methods you just wrote. Use the example tests for inspiration.


Using it
--------

To use your new JSON schema, its Python distribution must be available on the
system. Run ``python setup.py develop`` in the schema directory to install it.

Now you can use the ``yourapp_message_schemas.schema.Message`` class (or
however you named the package) to construct your message instances and call
:py:func:`fedora_messaging.api.publish <pub-api>` on them. Edit the
``publish.py`` script to read::

    #!/usr/bin/env python3

    from fedora_messaging.api import publish
    from fedora_messaging.config import conf
    from yourapp_message_schema.schema import Message

    conf.setup_logging()
    message = Message(
        topic="tutorial.topic",
        body={
            "owner": "fedorauser",
            "package": {
                "name": "foobar",
                "version": "1.0",
            }
        }
    )
    publish(message)

Start a consumer, and send the message. Try to comment out the "owner" key and
see what happens when you try to send a message that is not valid according to
the schema.


Updating it
-----------

Message formats can change over time, and the schema must change to reflect
that. When that happens, you need to copy the old class to a new class in the
schemas package, make the changes you need to do, and import the new one in
your publisher. You must also add a new entry in the ``entry_points`` argument
in the schema package's ``setup.py`` file. The name of the entry point is
currently unused, only the class path matters.

However, be warned that messages published with the new class may be dropped by
the receivers if they don't have the new schema available locally.  Therefore,
you should publish the schema package with the new schema, update it on all the
receivers, restart them, and then start using the new version in the publishers.

You should keep the old schema versions in the schemas package for a reasonable
amount of time, long enough to make sure all receivers are up-to-date. To avoid
clutter, we recommend you use a separate module per schema version
(``yourapp_message_schemas.v1:Message``,
``yourapp_message_schemas.v2:Message``, etc)

Now create a new version and use it in the ``publish.py`` script. Send a
message before restarting the ``consume.py`` script to see what happens when a
message with an unknown schema is received. Now restart the ``consume.py``
script and re-send the message.
