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

import datetime
import json
import logging
import uuid

import jsonschema
import pika
import pkg_resources

from .exceptions import ValidationError


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


def get_message(routing_key, properties, body):
    """
    Construct a Message instance given the routing key, the properties and the
    body received from the AMQP broker.

    Args:
        routing_key (str): The AMQP routing key (will become the message topic)
        properties (pika.BasicProperties): the AMQP properties
        body (bytes): The encoded message body
    """
    if properties.headers is None:
        _log.error('Message (body=%r) arrived without headers. '
                   'A publisher is misbehaving!', body)
        properties.headers = {}

    try:
        MessageClass = get_class(properties.headers['fedora_messaging_schema'])
    except KeyError:
        _log.error('Message (headers=%r, body=%r) arrived without a schema header.'
                   ' A publisher is misbehaving!', properties.headers, body)
        MessageClass = Message

    if properties.content_encoding is None:
        _log.error('Message arrived without a content encoding')
        properties.content_encoding = 'utf-8'
    try:
        body = body.decode(properties.content_encoding)
    except UnicodeDecodeError as e:
        _log.error('Unable to decode message body %r with %s content encoding',
                   body, properties.content_encoding)
        raise ValidationError(e)

    try:
        body = json.loads(body)
    except ValueError as e:
        _log.error('Failed to load message body %r, %r', body, e)
        raise ValidationError(e)

    message = MessageClass(
        body=body, topic=routing_key, properties=properties)
    try:
        message.validate()
        _log.debug('Successfully validated message %r', message)
    except jsonschema.exceptions.ValidationError as e:
        _log.error('Message validation of %r failed: %r', message, e)
        raise ValidationError(e)
    return message


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
        id (six.text_type): The message id as a unicode string.
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
        properties (pika.BasicProperties): The AMQP properties. If this is not
            provided, they will be generated.
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

    def __init__(self, body=None, headers=None, topic=None, properties=None):
        self._body = body or {}
        if topic:
            self.topic = topic
        headers = headers or {}
        self._properties = properties or self._build_properties(headers)

    def _build_properties(self, headers):
        # Consumers use this to determine what schema to use and if they're out
        # of date.
        headers['fedora_messaging_schema'] = _schema_name(self.__class__)
        now = datetime.datetime.now().replace(microsecond=0)
        headers['sent-at'] = now.isoformat()
        # message_id = "{}.{}".format(now.year, uuid.uuid4())
        message_id = str(uuid.uuid4())
        return pika.BasicProperties(
            content_type='application/json', content_encoding='utf-8', delivery_mode=2,
            headers=headers, message_id=message_id,
        )

    @property
    def _headers(self):
        """
        The message headers dictionary.

        .. note: If there's a reason users want to use this interface, it can
              be made public. Please file a bug if you feel you need this.
        """
        return self._properties.headers

    @_headers.setter
    def _headers_setter(self, value):
        self._properties.headers = value

    @property
    def id(self):
        return self._properties.message_id

    @id.setter
    def id_setter(self, value):
        self._properties.message_id = value

    @property
    def _encoded_routing_key(self):
        """The encoded routing key used to publish the message on the broker."""
        return self.topic.encode('utf-8')

    @property
    def _encoded_body(self):
        """The encoded body used to publish the message."""
        return json.dumps(self._body).encode('utf-8')

    def __str__(self):
        """
        A human-readable representation of this message.

        This should provide a detailed representation of the message, much like the body
        of an email.

        The default implementation is to format the raw message id, topic, headers, and
        body. Applications use this to present messages to users.
        """
        return 'Id: {i}\nTopic: {t}\nHeaders: {h}\nBody: {b}'.format(
            i=self.id,
            t=self.topic,
            h=json.dumps(self._headers, sort_keys=True, indent=4),
            b=json.dumps(self._body, sort_keys=True, indent=4)
        )

    def __repr__(self):
        """
        Provide a printable representation of the object that can be passed to func:`eval`.
        """
        return "{}(id={}, topic={}, body={})".format(
            self.__class__.__name__, repr(self.id), repr(self.topic), repr(self._body))

    def __eq__(self, other):
        """
        Two messages of the same class with the same topic, headers, and body are equal.

        Args:
            other (object): The object to check for equality.

        Returns:
            bool: True if the messages are equal.
        """
        return (isinstance(other, self.__class__) and self.topic == other.topic and
                self._body == other._body and self._headers == other._headers)

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

        .. warning:: This method should not be overridden by sub-classes.

        Raises:
            jsonschema.ValidationError: If either the message headers or the message body
                are invalid.
            jsonschema.SchemaError: If either the message header schema or the message body
                schema are invalid.
        """
        _log.debug('Validating message headers "%r" with schema "%r"',
                   self._headers, self.headers_schema)
        jsonschema.validate(self._headers, self.headers_schema)
        _log.debug('Validating message body "%r" with schema "%r"',
                   self._body, self.body_schema)
        jsonschema.validate(self._body, self.body_schema)
