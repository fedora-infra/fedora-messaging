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


class Message(object):
    """
    Messages are simply JSON-encoded objects. This allows message authors to
    define a schema and implement Python methods to abstract the raw message
    from the user. This allows the schema to change and evolve without breaking
    the user-facing API.

    There are a number of properties that are intended to be overridden by
    users.  These fields are used to sort messages for notifications or are
    used to create human-readable versions of the messages. Properties that are
    intended for this purpose are noted in their attribute documentation below.

    Args:
        headers (dict): A set of message headers. Consult the headers schema for
            expected keys and values.
        body (dict): The message body. Consult the body schema for expected keys
            and values. This dictionary must be JSON-serializable by the default
            serializer.
        topic (six.text_type): The message topic as a unicode string. If this is
            not provided, the default topic for the class is used. See the
            attribute documentation below for details.
        properties (pika.BasicProperties): The AMQP properties. If this is not
            provided, they will be generated. Most users should not need to provide
            this, but it can be useful in testing scenarios.
        severity (int): An integer that indicates the severity of the message. This is
            used to determine what messages to notify end users about and should be
            :data:`DEBUG`, :data:`INFO`, :data:`WARNING`, or :data:`ERROR`. The
            default is :data:`INFO`, and can be set as a class attribute or on
            an instance-by-instance basis.

    Attributes:
        id (six.text_type): The message id as a unicode string. This attribute is
            automatically generated and set by the library and users should only
            set it themselves in testing scenarios.
        topic (six.text_type): The message topic as a unicode string. The topic
            is used by message consumers to filter what messages they receive.
            Topics should be a string of words separated by '.' characters,
            with a length limit of 255 bytes. Because of this byte limit, it is
            best to avoid non-ASCII character. Topics should start general and
            get more specific each word. For example: "bodhi.update.kernel" is
            a possible topic. "bodhi" identifies the application, "update"
            identifies the message, and "kernel" identifies the package in the
            update. This can be set at a class level or on a instance level.
            Dynamic, specific topics that allow for fine-grain filtering are
            preferred.
        headers_schema (dict): A `JSON schema <http://json-schema.org/>`_ to be used with
            :func:`jsonschema.validate` to validate the message headers. For
            most users, the default definition should suffice.
        body_schema (dict): A `JSON schema <http://json-schema.org/>`_ to be used with
            :func:`jsonschema.validate` to validate the message body. The body_schema
            is retrieved on a message instance so it is not required to be a
            class attribute, although this is a convenient approach. Users are
            also free to write the JSON schema as a file and load the file from
            the filesystem or network if they prefer.
        body (dict): The message body as a Python dictionary. This is validated by
            the body schema before publishing and before consuming.
        severity (int): An integer that indicates the severity of the message. This is
            used to determine what messages to notify end users about and should be
            :data:`DEBUG`, :data:`INFO`, :data:`WARNING`, or :data:`ERROR`. The
            default is :data:`INFO`, and can be set as a class attribute or on
            an instance-by-instance basis.
        queue (str): The name of the queue this message arrived through. This
            attribute is set automatically by the library and users should never
            set it themselves.
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

        .. note:: Sub-classes should override this method. It is used to create
            the subject of email notifications, IRC notification, and by other
            tools to display messages to humans in short form.

        The default implementation is to simply return the message topic.
        """
        return self.topic

    def __str__(self):
        """
        A human-readable representation of this message.

        This should provide a detailed, long-form representation of the
        message. The default implementation is to format the raw message id,
        topic, headers, and body.

        .. note:: Sub-classes should override this method. It is used to create
            the body of email notifications and by other tools to display messages
            to humans.
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

        .. note:: Sub-classes should override this method if there is a URL
            associated with message.

        Returns:
            str or None: A relevant URL.
        """
        return None

    @property
    def app_icon(self):
        """An URL to the icon of the application that generated the message.

        .. note:: Sub-classes should override this method if their application
            has an icon and they wish that image to appear in applications that
            consume messages.

        Returns:
            str or None: The URL to the app's icon.
        """
        return None

    @property
    def agent_avatar(self):
        """An URL to the avatar of the user who caused the action.

        .. note:: Sub-classes should override this method if the message was
            triggered by a particular user.

        Returns:
            str or None: The URL to the user's avatar.
        """
        return None

    @property
    def usernames(self):
        """List of users affected by the action that generated this message.

        .. note:: Sub-classes should override this method if the message pertains
            to a user or users. The data returned from this property is used to
            filter notifications.

        Returns:
            list(str): A list of affected usernames.
        """
        return []

    @property
    def packages(self):
        """List of RPM packages affected by the action that generated this message.

        .. note:: Sub-classes should override this method if the message pertains
            to one or more RPM packages. The data returned from this property
            is used to filter notifications.

        Returns:
            list(str): A list of affected package names.
        """
        return []

    @property
    def containers(self):
        """List of containers affected by the action that generated this message.

        .. note:: Sub-classes should override this method if the message pertains
            to one or more container images. The data returned from this property
            is used to filter notifications.

        Returns:
            list(str): A list of affected container names.
        """
        return []

    @property
    def modules(self):
        """List of modules affected by the action that generated this message.

        .. note:: Sub-classes should override this method if the message pertains
            to one or more modules. The data returned from this property is
            used to filter notifications.

        Returns:
            list(str): A list of affected module names.
        """
        return []

    @property
    def flatpaks(self):
        """List of flatpaks affected by the action that generated this message.

        .. note:: Sub-classes should override this method if the message pertains
            to one or more flatpaks. The data returned from this property is
            used to filter notifications.

        Returns:
            list(str): A list of affected flatpaks names.
        """
        return []

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


#: The schema for each JSON object produced by :func:`dumps`, consumed by
#: :func:`loads`, and expected by CLI commands like "fedora-messaging publish".
SERIALIZED_MESSAGE_SCHEMA = {
    "$schema": "http://json-schema.org/draft-04/schema#",
    "description": "Schema for the JSON object used to represent messages in a file",
    "type": "object",
    "properties": {
        "topic": {"type": "string", "description": "The message topic"},
        "headers": {
            "type": "object",
            "properties": Message.headers_schema["properties"],
            "description": "The message headers",
        },
        "id": {"type": "string", "description": "The message's UUID."},
        "body": {"type": "object", "description": "The message body."},
        "queue": {
            "type": "string",
            "description": "The queue the message arrived on, if any.",
        },
    },
    "required": ["topic", "headers", "id", "body", "queue"],
}


def dumps(messages):
    """
    Serialize messages to a file format acceptable for :func:`loads` or for the
    publish CLI command. The format is a string where each line is a JSON
    object that conforms to the :data:`SERIALIZED_MESSAGE_SCHEMA` format.

    Args:
        messages (list or Message): The messages to serialize. Each message in
            the messages is subclass of Message.

    Returns:
        str: Serialized messages.

    Raises:
        ValidationError: If one of the messages provided doesn't conform to its
            schema.
    """
    if isinstance(messages, Message):
        messages = [messages]

    serialized_messages = []
    for message in messages:
        try:
            message.validate()
        except (jsonschema.exceptions.ValidationError, AttributeError) as e:
            raise ValidationError(e)
        m = {
            "topic": message.topic,
            "headers": message._headers,
            "id": message.id,
            "body": message.body,
            "queue": message.queue,
        }
        serialized_messages.append(json.dumps(m, ensure_ascii=False, sort_keys=True))

    return "\n".join(serialized_messages) + "\n"


def loads(serialized_messages):
    """
    Deserialize messages from a file format produced by :func:`dumps`.  The
    format is a string where each line is a JSON object that conforms to the
    :data:`SERIALIZED_MESSAGE_SCHEMA` format.

    Args:
        serialized_messages (str): A string made up of a JSON object per line.

    Returns:
        list: Deserialized message objects.

    Raises:
        ValidationError: If the string isn't formatted properly or message
            doesn't pass the message schema validation
    """
    messages = []
    for serialized_message in serialized_messages.splitlines():
        try:
            message_dict = json.loads(serialized_message)
        except ValueError as e:
            raise ValidationError(e)
        try:
            jsonschema.validate(message_dict, SERIALIZED_MESSAGE_SCHEMA)
        except jsonschema.exceptions.ValidationError as e:
            raise ValidationError(e)
        MessageClass = get_class(message_dict["headers"]["fedora_messaging_schema"])
        message = MessageClass(
            body=message_dict["body"],
            topic=message_dict["topic"],
            headers=message_dict["headers"],
            severity=message_dict["headers"]["fedora_messaging_severity"],
        )
        message.queue = message_dict["queue"]
        message.id = message_dict["id"]
        messages.append(message)

    return messages
