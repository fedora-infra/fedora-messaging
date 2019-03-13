"""
This module defines the base class of message objects and keeps a registry of
known message implementations. This registry is populated from Python entry
points in the "fedora.messages" group.

To implement your own message schema, simply create a class that inherits the
:class:`Message` class, and add an entry point in your Python package under the
"fedora.messages" group. For example, an entry point for the :class:`Message`
schema would be::

    entry_points = {
        'fedora.messages': [
            'base.message=fedora_messaging.message:Message'
        ]
    }

The entry point name must be unique to your application and is used to map
messages to your message class, so it's best to prefix it with your application
name (e.g. ``bodhi.new_update_messageV1``). When publishing, the Fedora
Messaging library will add a header with the entry point name of the class used
so the consumer can locate the correct schema.

Since every client needs to have the message schema installed, you should define
this class in a small Python package of its own.
"""

import datetime
import json
import logging
import warnings
import uuid

import jsonschema
import pika
import pkg_resources
import pytz

from . import config
from .exceptions import ValidationError


#: Indicates the message is for debugging or is otherwise very low priority. Users
#: will not be notified unless they've explicitly requested DEBUG level messages.
DEBUG = 10

#: Indicates the message is informational. End users will not receive notifications
#: for these messages by default. For example, automated tests passed for their
#: package.
INFO = 20

#: Indicates a problem or an otherwise important problem. Users are notified of
#: these messages when they pertain to packages they are associated with by default.
#: For example, one or more automated tests failed against their package.
WARNING = 30

#: Indicates a critically important message that users should act upon as soon as
#: possible. For example, their package no longer builds.
ERROR = 40

#: A tuple of all valid severity levels
SEVERITIES = (DEBUG, INFO, WARNING, ERROR)


_log = logging.getLogger(__name__)

# Maps string names of message types to classes and back
_schema_name_to_class = {}
_class_to_schema_name = {}

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

    try:
        return _schema_name_to_class[schema_name]
    except KeyError:
        _log.warning(
            'The schema "%s" is not in the schema registry! Either install '
            "the package with its schema definition or define a schema. "
            "Falling back to the default schema...",
            schema_name,
        )
        return Message


def get_name(cls):
    """
    Retrieve the schema name associated with a message class.

    Returns:
        str: The schema name.

    Raises:
        TypeError: If the message class isn't registered. Check your entry point
            for correctness.
    """
    global _registry_loaded
    if not _registry_loaded:
        load_message_classes()

    try:
        return _class_to_schema_name[cls]
    except KeyError:
        raise TypeError(
            "The class {} is not in the message registry, which indicates it is"
            ' not in the current list of entry points for "fedora_messaging".'
            " Please check that the class has been added to your package's"
            " entry points.".format(repr(cls))
        )


def load_message_classes():
    """Load the 'fedora.messages' entry points and register the message classes."""
    for message in pkg_resources.iter_entry_points("fedora.messages"):
        cls = message.load()
        _log.info(
            "Registering the '%s' key as the '%r' class in the Message "
            "class registry",
            message.name,
            cls,
        )
        _schema_name_to_class[message.name] = cls
        _class_to_schema_name[cls] = message.name
    global _registry_loaded
    _registry_loaded = True


def get_message(routing_key, properties, body):
    """
    Construct a Message instance given the routing key, the properties and the
    body received from the AMQP broker.

    Args:
        routing_key (str): The AMQP routing key (will become the message topic)
        properties (pika.BasicProperties): the AMQP properties
        body (bytes): The encoded message body

    Raises:
        ValidationError: If Message validation failed or message body
            docoding/loading is impossible.
    """
    if properties.headers is None:
        _log.error(
            "Message (body=%r) arrived without headers. " "A publisher is misbehaving!",
            body,
        )
        properties.headers = {}

    try:
        MessageClass = get_class(properties.headers["fedora_messaging_schema"])
    except KeyError:
        _log.error(
            "Message (headers=%r, body=%r) arrived without a schema header."
            " A publisher is misbehaving!",
            properties.headers,
            body,
        )
        MessageClass = Message

    try:
        severity = properties.headers["fedora_messaging_severity"]
    except KeyError:
        _log.error(
            "Message (headers=%r, body=%r) arrived without a severity."
            " A publisher is misbehaving! Defaulting to INFO.",
            properties.headers,
            body,
        )
        severity = INFO

    if properties.content_encoding is None:
        _log.error("Message arrived without a content encoding")
        properties.content_encoding = "utf-8"
    try:
        body = body.decode(properties.content_encoding)
    except UnicodeDecodeError as e:
        _log.error(
            "Unable to decode message body %r with %s content encoding",
            body,
            properties.content_encoding,
        )
        raise ValidationError(e)

    try:
        body = json.loads(body)
    except ValueError as e:
        _log.error("Failed to load message body %r, %r", body, e)
        raise ValidationError(e)

    message = MessageClass(
        body=body, topic=routing_key, properties=properties, severity=severity
    )
    try:
        message.validate()
        _log.debug("Successfully validated message %r", message)
    except jsonschema.exceptions.ValidationError as e:
        _log.error("Message validation of %r failed: %r", message, e)
        raise ValidationError(e)
    return message


def dumps(messages):
    """
    Serialize messages to a JSON formatted str

    Args:
        messages (list): The list of messages to serialize. Each message in
            the messages is subclass of Messge.

    Returns:
        str: Serialized messages.

    Raises:
        TypeError: If at least one message is not instance of Message class or subclass.
    """
    serialized_messages = []
    try:
        for message in messages:
            message_dict = message._dump()
            serialized_messages.append(message_dict)
    except AttributeError:
        _log.error("Improper object for messages serialization.")
        raise TypeError("Message have to be instance of Message class or subclass.")

    return json.dumps(serialized_messages, sort_keys=True)


def loads(serialized_messages):
    """
    Deserialize messages from a JSON formatted str

    Args:
        serialized_messages (JSON str):

    Returns:
        list: Deserialized message objects.

    Raises:
        ValidationError: If deserialized message validation failed.
        KeyError: If serialized_messages aren't properly serialized.
        ValueError: If serialized_messages is not valid JSON
    """
    try:
        messages_dicts = json.loads(serialized_messages)
    except ValueError:
        _log.error("Loading serialized messages failed.")
        raise

    messages = []
    for message_dict in messages_dicts:
        try:
            headers = message_dict["headers"]
        except KeyError:
            _log.error("Message saved without headers.")
            raise

        try:
            MessageClass = get_class(headers["fedora_messaging_schema"])
        except KeyError:
            _log.error("Message (headers=%r) saved without a schema header.", headers)
            raise

        try:
            body = message_dict["body"]
        except KeyError:
            _log.error("Message saved without body.")
            raise

        try:
            id = message_dict["id"]
        except KeyError:
            _log.error("Message saved without id.")
            raise

        try:
            queue = message_dict["queue"]
        except KeyError:
            _log.warning("Message saved without queue.")
            queue = None

        try:
            topic = message_dict["topic"]
        except KeyError:
            _log.error("Message saved without topic.")
            raise

        try:
            severity = headers["fedora_messaging_severity"]
        except KeyError:
            _log.error("Message saved without a severity.")
            raise

        message = MessageClass(
            body=body, topic=topic, headers=headers, severity=severity
        )
        try:
            message.validate()
            _log.debug("Successfully validated message %r", message)
        except jsonschema.exceptions.ValidationError as e:
            _log.error("Message validation of %r failed: %r", message, e)
            raise ValidationError(e)

        message.queue = queue
        message.id = id
        messages.append(message)

    return messages


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
        severity (int): An integer that indicates the severity of the message. This is
            used to determine what messages to notify end users about and should be
            :data:`DEBUG`, :data:`INFO`, :data:`WARNING`, or :data:`ERROR`.
        queue (str): The name of the queue this message arrived through.

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

    severity = INFO
    topic = ""
    headers_schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "Schema for message headers",
        "type": "object",
        "properties": {
            "fedora_messaging_severity": {
                "type": "number",
                "enum": [DEBUG, INFO, WARNING, ERROR],
            },
            "fedora_messaging_schema": {"type": "string"},
            "sent-at": {"type": "string"},
        },
    }
    body_schema = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "Schema for message body",
        "type": "object",
    }

    def __init__(
        self, body=None, headers=None, topic=None, properties=None, severity=None
    ):
        self.body = body or {}

        if topic:
            # Default is "" on base class
            self.topic = topic

        headers = headers or {}
        if severity:
            self.severity = severity
        self._properties = properties or self._build_properties(headers)
        self.queue = None

    def _build_properties(self, headers):
        # Consumers use this to determine what schema to use and if they're out
        # of date.
        headers["fedora_messaging_schema"] = get_name(self.__class__)
        now = datetime.datetime.utcnow().replace(microsecond=0, tzinfo=pytz.utc)
        headers["sent-at"] = now.isoformat()
        headers["fedora_messaging_severity"] = self.severity
        headers.update(self._filter_headers())
        message_id = str(uuid.uuid4())
        return pika.BasicProperties(
            content_type="application/json",
            content_encoding="utf-8",
            delivery_mode=2,
            headers=headers,
            message_id=message_id,
        )

    def _filter_headers(self):
        """
        Add headers designed for filtering messages based on objects.

        Returns:
            dict: Filter-related headers to be combined with the existing headers
        """
        headers = {}
        for user in self.usernames:
            headers["fedora_messaging_user_{}".format(user)] = True
        for package in self.packages:
            headers["fedora_messaging_rpm_{}".format(package)] = True
        for container in self.containers:
            headers["fedora_messaging_container_{}".format(container)] = True
        for module in self.modules:
            headers["fedora_messaging_module_{}".format(module)] = True
        for flatpak in self.flatpaks:
            headers["fedora_messaging_flatpak_{}".format(flatpak)] = True
        return headers

    @property
    def _headers(self):
        """
        The message headers dictionary.

        .. note: If there's a reason users want to use this interface, it can
              be made public. Please file a bug if you feel you need this.
        """
        return self._properties.headers

    @_headers.setter
    def _headers(self, value):
        self._properties.headers = value

    @property
    def id(self):
        return self._properties.message_id

    @id.setter
    def id(self, value):
        self._properties.message_id = value

    @property
    def _encoded_routing_key(self):
        """The encoded routing key used to publish the message on the broker."""
        topic = self.topic
        if config.conf["topic_prefix"]:
            topic = ".".join((config.conf["topic_prefix"].rstrip("."), topic))
        return topic.encode("utf-8")

    @property
    def _encoded_body(self):
        """The encoded body used to publish the message."""
        return json.dumps(self.body).encode("utf-8")

    def __repr__(self):
        """
        Provide a printable representation of the object that can be passed to func:`eval`.
        """
        return "{}(id={}, topic={}, body={})".format(
            self.__class__.__name__, repr(self.id), repr(self.topic), repr(self.body)
        )

    def __eq__(self, other):
        """
        Two messages of the same class with the same topic, headers, and body are equal.

        The "sent-at" header is excluded from the equality check as this is set
        automatically and is dependent on when the object is created.

        Args:
            other (object): The object to check for equality.

        Returns:
            bool: True if the messages are equal.
        """
        if not isinstance(other, self.__class__):
            return False

        headers = self._headers.copy()
        other_headers = other._headers.copy()
        try:
            del headers["sent-at"]
        except KeyError:
            pass
        try:
            del other_headers["sent-at"]
        except KeyError:
            pass

        return (
            self.topic == other.topic
            and self.body == other.body
            and headers == other_headers
        )

    def validate(self):
        """
        Validate the headers and body with the message schema, if any.

        In addition to the user-provided schema, all messages are checked against
        the base schema which requires certain message headers and the that body
        be a JSON object.

        .. warning:: This method should not be overridden by sub-classes.

        Raises:
            jsonschema.ValidationError: If either the message headers or the message body
                are invalid.
            jsonschema.SchemaError: If either the message header schema or the message body
                schema are invalid.
        """
        for schema in (self.headers_schema, Message.headers_schema):
            _log.debug(
                'Validating message headers "%r" with schema "%r"',
                self._headers,
                schema,
            )
            jsonschema.validate(self._headers, schema)
        for schema in (self.body_schema, Message.body_schema):
            _log.debug(
                'Validating message body "%r" with schema "%r"', self.body, schema
            )
            jsonschema.validate(self.body, schema)

    @property
    def summary(self):
        """
        A short, human-readable representation of this message.

        This should provide a short summary of the message, much like the subject line
        of an email.

        The default implementation is to simply return the message topic.
        """
        return self.topic

    def __str__(self):
        """
        A human-readable representation of this message.

        This should provide a detailed representation of the message, much like the body
        of an email.

        The default implementation is to format the raw message id, topic, headers, and
        body. Applications use this to present messages to users.
        """
        return "Id: {i}\nTopic: {t}\nHeaders: {h}\nBody: {b}".format(
            i=self.id,
            t=self.topic,
            h=json.dumps(
                self._headers, sort_keys=True, indent=4, separators=(",", ": ")
            ),
            b=json.dumps(self.body, sort_keys=True, indent=4, separators=(",", ": ")),
        )

    @property
    def url(self):
        """
        An URL to the action that caused this message to be emitted.

        Returns:
            str or None: A relevant URL.
        """
        return None

    @property
    def app_icon(self):
        """An URL to the icon of the application that generated the message.

        Returns:
            str or None: The URL to the app's icon.
        """
        return None

    @property
    def agent_avatar(self):
        """An URL to the avatar of the user who caused the action.

        Returns:
            str or None: The URL to the user's avatar.
        """
        return None

    @property
    def usernames(self):
        """List of users affected by the action that generated this message.

        Returns:
            list(str): A list of affected usernames.
        """
        return []

    @property
    def packages(self):
        """List of packages affected by the action that generated this message.

        Returns:
            list(str): A list of affected package names.
        """
        return []

    @property
    def containers(self):
        """List of containers affected by the action that generated this message.

        Returns:
            list(str): A list of affected container names.
        """
        return []

    @property
    def modules(self):
        """List of modules affected by the action that generated this message.

        Returns:
            list(str): A list of affected module names.
        """
        return []

    @property
    def flatpaks(self):
        """List of flatpaks affected by the action that generated this message.

        Returns:
            list(str): A list of affected flatpaks names.
        """
        return []

    def _dump(self):
        """
        Dump message attributes.

        Returns:
            dict: A dictionary of message attributes.
        """
        return {
            "topic": self.topic,
            "headers": self._headers,
            "id": self.id,
            "body": self.body,
            "queue": self.queue,
        }

    @property
    def _body(self):
        warnings.warn(
            "The '_body' property has been renamed to 'body'.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.body

    @_body.setter
    def _body(self, value):
        warnings.warn(
            "The '_body' property has been renamed to 'body'.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.body = value
