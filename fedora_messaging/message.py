# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

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
import re
import uuid
from collections.abc import Mapping
from importlib.metadata import distribution as get_distribution
from importlib.metadata import entry_points, PackageNotFoundError
from typing import Any, cast, ClassVar, Optional, TYPE_CHECKING, Union

import jsonschema
import jsonschema.exceptions
import pika

from . import config
from .exceptions import ValidationError
from .schema_utils import user_avatar_url


if TYPE_CHECKING:
    from typing_extensions import TypeAlias


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
_schema_name_to_class: dict[str, type["Message"]] = {}
_class_to_schema_name: dict[type["Message"], str] = {}
_schema_name_to_package: dict[str, str] = {}

# Used to load the registry automatically on first use
_registry_loaded = False


HeaderType: "TypeAlias" = dict[str, Union[str, int, bool]]


def get_class(schema_name: str) -> type["Message"]:
    """
    Retrieve the message class associated with the schema name.

    If no match is found, the default schema is returned and a warning is logged.

    Args:
        schema_name: The name of the :class:`Message` sub-class;
            this is typically the Python path.

    Returns:
        A sub-class of :class:`Message` to create the message from.
    """

    return _get_class_from_headers(dict(fedora_messaging_schema=schema_name))


def _get_class_from_headers(headers: HeaderType) -> type["Message"]:
    global _registry_loaded
    if not _registry_loaded:
        load_message_classes()

    schema_name = cast(str, headers["fedora_messaging_schema"])
    try:
        return _schema_name_to_class[schema_name]
    except KeyError:
        schema_package = headers.get("fedora_messaging_schema_package")
        if schema_package:
            package_text = f"You can install the missing schema from package {schema_package!r}"
        else:
            package_text = (
                "Either install the package with its schema definition or define a schema"
            )

        _log.warning(
            'The schema "%s" is not in the schema registry! %s. '
            "Falling back to the default schema...",
            schema_name,
            package_text,
        )
        return Message


def get_name(cls: type["Message"]) -> str:
    """
    Retrieve the schema name associated with a message class.

    Returns:
        The schema name.

    Raises:
        TypeError: If the message class isn't registered. Check your entry point
            for correctness.
    """
    global _registry_loaded
    if not _registry_loaded:
        load_message_classes()

    try:
        return _class_to_schema_name[cls]
    except KeyError as e:
        raise TypeError(
            f"The class {cls!r} is not in the message registry, which indicates it is"
            ' not in the current list of entry points for "fedora_messaging".'
            " Please check that the class has been added to your package's entry points."
        ) from e


def _get_distribution_from_module(module: str) -> Union[str, None]:
    if not module:
        return None
    module_parts = module.split(".")
    try:
        distribution = get_distribution(".".join(module_parts))
        try:
            distribution_name = distribution.name
        except AttributeError:  # pragma: no cover
            # COMPAT: Python <= 3.9
            distribution_name = distribution.metadata["Name"]
    except PackageNotFoundError:
        return _get_distribution_from_module(".".join(module_parts[:-1]))
    # Normalize the name: PEP 503 plus dashes as underscores.
    return re.sub(r"[-_.]+", "-", distribution_name).lower().replace("-", "_")


def load_message_classes() -> None:
    """Load the 'fedora.messages' entry points and register the message classes."""
    try:
        eps = entry_points(group="fedora.messages")
    except TypeError:  # pragma: no cover
        # Python < 3.10
        # https://docs.python.org/3.10/library/importlib.metadata.html#entry-points
        eps = entry_points().get("fedora.messages", [])  # type: ignore
    for message in eps:
        cls = message.load()
        _log.debug(
            "Registering the '%s' key as the '%r' class in the Message " "class registry",
            message.name,
            cls,
        )
        _schema_name_to_class[message.name] = cls
        _class_to_schema_name[cls] = message.name
        module = message.module
        distribution = _get_distribution_from_module(module)
        if distribution is None:
            continue
        _schema_name_to_package[message.name] = distribution
    global _registry_loaded
    _registry_loaded = True


def get_message(routing_key: str, properties: pika.BasicProperties, body: bytes) -> "Message":
    """
    Construct a Message instance given the routing key, the properties and the
    body received from the AMQP broker.

    Args:
        routing_key: The AMQP routing key (will become the message topic)
        properties: the AMQP properties
        body: The encoded message body

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
        MessageClass = _get_class_from_headers(cast(HeaderType, properties.headers))
    except KeyError:
        _log.error(
            "Message (headers=%r, body=%r) arrived without a schema header."
            " A publisher is misbehaving!",
            properties.headers,
            body,
        )
        MessageClass = Message

    try:
        severity = cast(int, properties.headers["fedora_messaging_severity"])
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
        body_str = body.decode(properties.content_encoding)
    except UnicodeDecodeError as e:
        _log.error(
            "Unable to decode message body %r with %s content encoding",
            body,
            properties.content_encoding,
        )
        raise ValidationError(e) from e

    try:
        body_dict = json.loads(body_str)
    except ValueError as e:
        _log.error("Failed to load message body %r, %r", body_str, e)
        raise ValidationError(e) from e

    message = MessageClass(
        body=body_dict, topic=routing_key, properties=properties, severity=severity
    )
    try:
        message.validate()
        _log.debug("Successfully validated message %r", message)
    except jsonschema.exceptions.ValidationError as e:
        _log.error("Message validation of %r failed: %r", message, e)
        raise ValidationError(e) from e

    if MessageClass.deprecated:
        _log.warning(
            "A message with a deprecated schema (%s.%s) has been received on topic %r. "
            "You should check the emitting application's documentation to upgrade to "
            "the newer schema version.",
            MessageClass.__module__,
            MessageClass.__name__,
            message.topic,
        )
    return message


class Message:
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
        headers: A set of message headers. Consult the headers schema for
            expected keys and values.
        body: The message body. Consult the body schema for expected keys
            and values. This dictionary must be JSON-serializable by the default
            serializer.
        topic: The message topic as a unicode string. If this is
            not provided, the default topic for the class is used. See the
            attribute documentation below for details.
        properties: The AMQP properties. If this is not
            provided, they will be generated. Most users should not need to provide
            this, but it can be useful in testing scenarios.
        severity: An integer that indicates the severity of the message. This is
            used to determine what messages to notify end users about and should be
            :data:`DEBUG`, :data:`INFO`, :data:`WARNING`, or :data:`ERROR`. The
            default is :data:`INFO`, and can be set as a class attribute or on
            an instance-by-instance basis.

    Attributes:
        topic (str): The message topic as a unicode string. The topic
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
        headers_schema (dict[str, Any]): A `JSON schema <http://json-schema.org/>`_ to be used with
            :func:`jsonschema.validate` to validate the message headers. For
            most users, the default definition should suffice.
        body_schema (dict[str, Any]): A `JSON schema <http://json-schema.org/>`_ to be used with
            :func:`jsonschema.validate` to validate the message body. The body_schema
            is retrieved on a message instance so it is not required to be a
            class attribute, although this is a convenient approach. Users are
            also free to write the JSON schema as a file and load the file from
            the filesystem or network if they prefer.
        body (dict[str, Any]): The message body as a Python dictionary. This is validated by
            the body schema before publishing and before consuming.
        severity (int): An integer that indicates the severity of the message. This is
            used to determine what messages to notify end users about and should be
            :data:`DEBUG`, :data:`INFO`, :data:`WARNING`, or :data:`ERROR`. The
            default is :data:`INFO`, and can be set as a class attribute or on
            an instance-by-instance basis.
        queue (str): The name of the queue this message arrived through. This
            attribute is set automatically by the library and users should never
            set it themselves.
        deprecated (bool): Whether this message schema has been deprecated by a more
            recent version. Emits a warning when a message of this class is received,
            to let consumers know that they should plan to upgrade. Defaults to
            ``False``.
    """

    severity: int = INFO
    topic: str = ""
    headers_schema: ClassVar[dict[str, Any]] = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "Schema for message headers",
        "type": "object",
        "properties": {
            "fedora_messaging_severity": {
                "type": "number",
                "enum": [DEBUG, INFO, WARNING, ERROR],
            },
            "fedora_messaging_schema": {"type": "string"},
            "fedora_messaging_schema_package": {"type": "string"},
            "sent-at": {"type": "string"},
        },
    }
    body_schema: ClassVar[dict[str, Any]] = {
        "$schema": "http://json-schema.org/draft-04/schema#",
        "description": "Schema for message body",
        "type": "object",
    }
    deprecated: bool = False

    def __init__(
        self,
        body: Optional[Mapping[str, Any]] = None,
        headers: Optional[HeaderType] = None,
        topic: Optional[str] = None,
        properties: Optional[pika.BasicProperties] = None,
        severity: Optional[int] = None,
    ):
        self.body = body or {}

        if topic:
            # Default is "" on base class
            self.topic = topic

        headers = headers or {}
        if severity:
            self.severity = severity
        self._properties = properties or self._build_properties(headers)
        self.queue: Optional[str] = None

    def _build_properties(self, headers: HeaderType) -> pika.BasicProperties:
        # Consumers use this to determine what schema to use and if they're out
        # of date.
        headers = headers.copy()
        headers["fedora_messaging_schema"] = schema_name = get_name(self.__class__)
        schema_package = _schema_name_to_package.get(schema_name)
        if schema_package:
            headers["fedora_messaging_schema_package"] = schema_package
        now = datetime.datetime.now(tz=datetime.timezone.utc).replace(microsecond=0)
        headers["sent-at"] = now.isoformat()
        headers["fedora_messaging_severity"] = self.severity
        # Mirror the priority in the headers for debugging purposes
        headers["priority"] = config.conf["publish_priority"] or 0
        headers.update(self._filter_headers())
        message_id = str(uuid.uuid4())
        return pika.BasicProperties(
            content_type="application/json",
            content_encoding="utf-8",
            delivery_mode=2,
            headers=headers,
            message_id=message_id,
            priority=config.conf["publish_priority"],
        )

    def _filter_headers(self) -> HeaderType:
        """
        Add headers designed for filtering messages based on objects.

        Returns:
            dict: Filter-related headers to be combined with the existing headers
        """
        headers: HeaderType = {}
        properties = [
            ("user", "usernames"),
            ("group", "groups"),
            ("rpm", "packages"),
            ("container", "containers"),
            ("module", "modules"),
            ("flatpak", "flatpaks"),
        ]
        for header_name, prop_name in properties:
            try:
                items = getattr(self, prop_name)
            except Exception:
                # The message is probably invalid, don't add the header
                _log.warning("Header %s not found in message", prop_name)
                continue
            for item in items:
                headers[f"fedora_messaging_{header_name}_{item}"] = True
        return headers

    @property
    def _headers(self) -> HeaderType:
        """
        The message headers dictionary.

        .. note: If there's a reason users want to use this interface, it can
              be made public. Please file a bug if you feel you need this.
        """
        return cast(Union[HeaderType, None], self._properties.headers) or {}

    @_headers.setter
    def _headers(self, value: HeaderType) -> None:
        self._properties.headers = value

    @property
    def id(self) -> str:
        """The message id as a unicode string.

        This attribute is automatically generated and set by the library and users should only set
        it themselves in testing scenarios.
        """
        return cast(str, self._properties.message_id)

    @id.setter
    def id(self, value: str) -> None:
        self._properties.message_id = value

    @property
    def priority(self) -> int:
        """The priority for the message, if the destination queue supports it.

        Defaults to zero (lowest priority).

        This value is taken into account in queues that have the ``x-max-priority``
        argument set. Most queues in Fedora don't support priorities, in which case
        the value will be ignored.

        Larger numbers indicate higher priority, you can read more about it in
        `RabbitMQ's documentation on priority`_.

        .. _RabbitMQ's documentation on priority:  https://www.rabbitmq.com/priority.html
        """
        return self._properties.priority or 0

    @priority.setter
    def priority(self, value: Union[int, None]) -> None:
        value = value or 0  # convert None to 0
        self._properties.priority = value
        # Mirror the priority in the headers for debugging purposes
        self._headers["priority"] = value

    @property
    def _encoded_routing_key(self) -> str:
        """The encoded routing key used to publish the message on the broker."""
        topic = self.topic
        if config.conf["topic_prefix"]:
            topic = ".".join((config.conf["topic_prefix"].rstrip("."), topic))
        return topic

    @property
    def _encoded_body(self) -> bytes:
        """The encoded body used to publish the message."""
        return json.dumps(self.body).encode("utf-8")

    def __repr__(self) -> str:
        """
        Provide a printable representation of the object that can be passed to func:`eval`.
        """
        return (
            f"{self.__class__.__name__}(id={self.id!r}, topic={self.topic!r}, body={self.body!r})"
        )

    def __eq__(self, other: object) -> bool:
        """
        Two messages of the same class with the same topic, headers, and body are equal.

        The "sent-at" header is excluded from the equality check as this is set
        automatically and is dependent on when the object is created.

        Args:
            other: The object to check for equality.

        Returns:
            True if the messages are equal.
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

        return self.topic == other.topic and self.body == other.body and headers == other_headers

    def validate(self) -> None:
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
            _log.debug('Validating message body "%r" with schema "%r"', self.body, schema)
            jsonschema.validate(self.body, schema)

    @property
    def summary(self) -> str:
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

    def __str__(self) -> str:
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
            h=json.dumps(self._headers, sort_keys=True, indent=4, separators=(",", ": ")),
            b=json.dumps(self.body, sort_keys=True, indent=4, separators=(",", ": ")),
        )

    @property
    def url(self) -> Union[str, None]:
        """
        An URL to the action that caused this message to be emitted.

        .. note:: Sub-classes should override this method if there is a URL
            associated with message.

        Returns:
            A relevant URL.
        """
        return None

    @property
    def app_name(self) -> Union[str, None]:
        """The name of the application that generated the message.

        .. note:: Sub-classes should override this method.

        Returns:
            The name of the application.
        """
        return None

    @property
    def app_icon(self) -> Union[str, None]:
        """An URL to the icon of the application that generated the message.

        .. note:: Sub-classes should override this method if their application
            has an icon and they wish that image to appear in applications that
            consume messages.

        Returns:
            The URL to the app's icon.
        """
        return None

    @property
    def agent_name(self) -> Union[str, None]:
        """The username of the user who caused the action.

        .. note:: Sub-classes should override this method if the message was
            triggered by a particular user.

        Returns:
            The agent's username.
        """
        return None

    @property
    def agent_avatar(self) -> Union[str, None]:
        """An URL to the avatar of the user who caused the action.

        .. note:: Sub-classes should override this method if the default
            Libravatar and OpenID-based URL generator is not appropriate.

        Returns:
            The URL to the user's avatar.
        """
        return user_avatar_url(self.agent_name) if self.agent_name is not None else None

    @property
    def usernames(self) -> list[str]:
        """List of users affected by the action that generated this message.

        .. note:: Sub-classes should override this method if the message pertains
            to a user or users. The data returned from this property is used to
            filter notifications.

        Returns:
            A list of affected usernames.
        """
        return []

    @property
    def groups(self) -> list[str]:
        """List of groups affected by the action that generated this message.

        .. note:: Sub-classes should override this method if the message pertains
            to a group or groups. The data returned from this property is used to
            filter notifications.

        Returns:
            A list of affected groups.
        """
        return []

    @property
    def packages(self) -> list[str]:
        """List of RPM packages affected by the action that generated this message.

        .. note:: Sub-classes should override this method if the message pertains
            to one or more RPM packages. The data returned from this property
            is used to filter notifications.

        Returns:
            A list of affected package names.
        """
        return []

    @property
    def containers(self) -> list[str]:
        """List of containers affected by the action that generated this message.

        .. note:: Sub-classes should override this method if the message pertains
            to one or more container images. The data returned from this property
            is used to filter notifications.

        Returns:
            A list of affected container names.
        """
        return []

    @property
    def modules(self) -> list[str]:
        """List of modules affected by the action that generated this message.

        .. note:: Sub-classes should override this method if the message pertains
            to one or more modules. The data returned from this property is
            used to filter notifications.

        Returns:
            A list of affected module names.
        """
        return []

    @property
    def flatpaks(self) -> list[str]:
        """List of flatpaks affected by the action that generated this message.

        .. note:: Sub-classes should override this method if the message pertains
            to one or more flatpaks. The data returned from this property is
            used to filter notifications.

        Returns:
            A list of affected flatpaks names.
        """
        return []


#: The schema for each JSON object produced by :func:`dumps`, consumed by
#: :func:`loads`, and expected by CLI commands like "fedora-messaging publish".
SERIALIZED_MESSAGE_SCHEMA: Mapping[str, Any] = {
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
            "type": ["string", "null"],
            "description": "The queue the message arrived on, if any.",
        },
        "priority": {
            "type": ["integer", "null"],
            "description": "The priority that the message has been sent with.",
        },
    },
    "required": ["topic", "body"],
}


def load_message(message_dict: Mapping[str, Any]) -> Message:
    """Load a message from a dictionary serialization.

    The dictionary must conform to the :data:`SERIALIZED_MESSAGE_SCHEMA` format.

    Args:
        message_dict: The dictionary representing the message.

    Raises:
        ValidationError: If the dictionary does not pass the serialization schema.

    Returns:
        The deserialized message object, as an instance of :class:`Message`
            or one of its subclasses.
    """
    try:
        jsonschema.validate(message_dict, SERIALIZED_MESSAGE_SCHEMA)
    except jsonschema.exceptions.ValidationError as e:
        raise ValidationError(e) from e
    try:
        MessageClass = _get_class_from_headers(message_dict.get("headers", {}))
    except KeyError:
        # No "fedora_messaging_schema" header
        MessageClass = Message
    message = MessageClass(
        body=message_dict["body"],
        topic=message_dict["topic"],
        headers=message_dict.get("headers"),
        severity=message_dict.get("headers", {}).get("fedora_messaging_severity"),
    )
    # Restore the sent-at header
    try:
        message._headers["sent-at"] = message_dict["headers"]["sent-at"]
    except KeyError:
        pass
    if "id" in message_dict:
        message.id = message_dict["id"]
    message.queue = message_dict.get("queue")
    message.priority = message_dict.get("priority")
    return message


def dumps(messages: Union[Message, list[Message]]) -> str:
    """
    Serialize messages to a file format acceptable for :func:`loads` or for the
    publish CLI command. The format is a string where each line is a JSON
    object that conforms to the :data:`SERIALIZED_MESSAGE_SCHEMA` format.

    Args:
        messages: The messages to serialize. Each message in
            the messages is subclass of Message.

    Returns:
        Serialized messages.

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
            raise ValidationError(e) from e
        m = {
            "topic": message.topic,
            "headers": message._headers,
            "id": message.id,
            "body": message.body,
            "queue": message.queue,
            "priority": message.priority,
        }
        serialized_messages.append(json.dumps(m, ensure_ascii=False, sort_keys=True))

    return "\n".join(serialized_messages) + "\n"


def loads(serialized_messages: str) -> list[Message]:
    """
    Deserialize messages from a file format produced by :func:`dumps`.  The
    format is a string where each line is a JSON object that conforms to the
    :data:`SERIALIZED_MESSAGE_SCHEMA` format.

    Args:
        serialized_messages: A string made up of a JSON object per line.

    Returns:
        Deserialized message objects.

    Raises:
        ValidationError: If the string isn't formatted properly or message
            doesn't pass the message schema validation
    """
    messages = []
    for serialized_message in serialized_messages.splitlines():
        try:
            message_dict = json.loads(serialized_message)
        except ValueError as e:
            raise ValidationError(e) from e
        message = load_message(message_dict)
        messages.append(message)

    return messages
