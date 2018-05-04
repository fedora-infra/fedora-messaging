"""
This module defines the base class of message objects and keeps a registry of
known message implementations. This registry is populated from Python entry
points. The registry keys are Python paths and the values are sub-classes of
:class:`Message`.

When publishing, the API will add a header with the schema used so the consumer
can locate the correct schema.

To implement your own message schema, simply create a class that inherits the
:class:`Message` class, and add an entry point in your Python package under the
"fedora.messages" group. For example, an entry point for the :class:`Message`
schema would be::

    entry_points = {
        'fedora.messages': ['base.message=fedora_messaging.message:Message']
    }

Since every client needs to have the message schema installed, you should define
this class in a small Python package of its own. Note that at this time, the
entry point name is unused.
"""

import json
import logging

import jsonschema
import pkg_resources


_log = logging.getLogger(__name__)

# Maps regular expressions to message classes
_class_registry = {}

# Used to load the registry automatically on first use
_registry_loaded = False


def get_class(schema_name):
    """
    Retrieve the message class associated with the schema name.

    If no match is found, the default schema is returned and a warning is logged.

    Args:
        schema_name (six.text_type): The name of the :class:`Message` sub-class;
            this is typically the Python path.

    Returns:
        Message: A sub-class of :class:`Message` to create the message from.
    """
    global _registry_loaded
    if not _registry_loaded:
        load_message_classes()
        _registry_loaded = True

    try:
        return _class_registry[schema_name]
    except KeyError:
        _log.warning('The schema "%s" is not in the schema registry! Either install '
                     'the package with its schema definition or define a schema. '
                     'Falling back to the default schema...', schema_name)
        return _class_registry[_schema_name(Message)]


def load_message_classes():
    """Load the 'fedora.messages' entry points and register the message classes."""
    for message in pkg_resources.iter_entry_points('fedora.messages'):
        cls = message.load()
        _log.info("Registering the '%r' class as a Fedora Message", cls)
        _class_registry[_schema_name(cls)] = cls


def _schema_name(cls):
    """
    Get the Python path of a class, used to identify the message schema.

    Args:
        cls (object): The class (not the instance of the class).

    Returns:
        str: The path in the format "<module>:<class_name>".
    """
    return '{}:{}'.format(cls.__module__, cls.__name__)


class Message(object):
    """
    Messages are simply JSON-encoded objects. This allows message authors to
    define a schema and implement Python methods to abstract the raw message
    from the user. This allows the schema to change and evolve without breaking
    the user-facing API.

    A message class includes a topic. This topic is used by message consumers
    to filter what messages they receive. Topics should be a string of words
    separated by '.' characters, with a length limit of 255 bytes. Because of
    this limit, it is best to avoid non-ASCII characters as well as
    variable-length components where the total size of the topic would exceed
    255 bytes.

    Attributes:
        topic (six.text_type): The message topic as a unicode string.
        headers_schema (dict): A `JSON schema <http://json-schema.org/>`_ to be used with
            :func:`jsonschema.validate` to validate the message headers.
        body_schema (dict): A `JSON schema <http://json-schema.org/>`_ to be used with
            :func:`jsonschema.validate` to validate the message headers.

    Args:
        headers (dict): A set of message headers. Consult the headers schema for
            expected keys and values.
        body (dict): The message body. Consult the body schema for expected keys
            and values.
        topic (six.text_type): The message topic as a unicode string. If this is
            not provided, the default topic for the class is used.
    """

    topic = ''
    headers_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'description': 'Schema for message headers',
        'type': 'object',
    }
    body_schema = {
        '$schema': 'http://json-schema.org/draft-04/schema#',
        'description': 'Schema for message body',
        'type': 'object',
    }
    schema_version = 1

    def __init__(self, body=None, headers=None, topic=None):
        self.headers = headers or {}
        self.body = body or {}
        if topic:
            self.topic = topic

    def __str__(self):
        """
        A human-readable representation of this message.

        This should provide a detailed representation of the message, much like the body
        of an email.

        The default implementation is to format the raw message topic, headers, and body.
        Applications use this to present messages to users.
        """
        return 'Topic: {t}\n\nHeaders: {h}\n\nBody: {b}'.format(
            t=self.topic,
            h=json.dumps(self.headers, sort_keys=True, indent=4),
            b=json.dumps(self.body, sort_keys=True, indent=4)
        )

    def __repr__(self):
        """
        Provide a printable representation of the object that can be passed to func:`eval`.
        """
        return "{}(body={}, headers={}, topic={})".format(
            self.__class__.__name__, repr(self.body), repr(self.headers), repr(self.topic))

    def __eq__(self, other):
        """
        Two messages of the same class with the same topic, headers, and body are equal.

        Args:
            other (object): The object to check for equality.

        Returns:
            bool: True if the messages are equal.
        """
        return (isinstance(other, self.__class__) and self.topic == other.topic and
                self.body == other.body and self.headers == other.headers)

    def summary(self):
        """
        A short, human-readable representation of this message.

        This should provide a short summary of the message, much like the subject line
        of an email.

        The default implementation is to simply return the message topic.
        """
        return self.topic

    def validate(self):
        """
        Validate the headers and body with the message schema, if any.

        Raises:
            jsonschema.ValidationError: If either the message headers or the message body
                are invalid.
            jsonschema.SchemaError: If either the message header schema or the message body
                schema are invalid.
        """
        _log.debug('Validating message headers "%r" with schema "%r", version %d',
                   self.headers, self.headers_schema, self.schema_version)
        jsonschema.validate(self.headers, self.headers_schema)
        _log.debug('Validating message body "%r" with schema "%r", version %d',
                   self.headers, self.headers_schema, self.schema_version)
        jsonschema.validate(self.body, self.body_schema)
