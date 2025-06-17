# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

"""
fedora-messaging can be configured with the
``/etc/fedora-messaging/config.toml`` file or by setting the
``FEDORA_MESSAGING_CONF`` environment variable to the path of the configuration
file.

Each configuration option has a default value.

.. contents:: Table of Configuration Options
    :local:

A complete example TOML configuration:

.. literalinclude:: ../../config.toml.example


Generic Options
===============

These options apply to both consumers and publishers.

.. _conf-amqp-url:

amqp_url
--------
The AMQP broker to connect to. This URL should be in the format described by
the :class:`pika.connection.URLParameters` documentation. This defaults to
``'amqp://?connection_attempts=3&retry_delay=5``.

.. note:: When using the Twisted consumer API, which the CLI does by default,
          any connection-related setting won't apply as Twisted manages the
          TCP/TLS connection.


.. _conf-passive-declares:

passive_declares
----------------
A boolean to specify if queues and exchanges should be declared passively (i.e
checked, but not actually created on the server). Defaults to ``False``.


.. _conf-tls:

tls
---
A dictionary of the TLS settings to use when connecting to the AMQP broker. The
default is::

    {
        'ca_cert': '/etc/pki/tls/certs/ca-bundle.crt',
        'keyfile': None,
        'certfile': None,
    }

The value of ``ca_cert`` should be the path to a bundle of CA certificates used
to validate the certificate presented by the server. The 'keyfile' and
'certfile' values should be to the client key and client certificate to use
when authenticating with the broker.

.. note:: The broker URL must use the ``amqps`` scheme. It is also possible to
          provide these setting via the ``amqp_url`` setting using a URL-encoded
          JSON object. This setting is provided as a convenient way to avoid that.


.. _conf-client-properties:

client_properties
-----------------
A dictionary that describes the client to the AMQP broker. This makes it easy
to identify the application using a connection. The dictionary can contain
arbitrary string keys and values. The default is::

    {
        'app': 'Unknown',
        'product': 'Fedora Messaging with Pika',
        'information': 'https://fedora-messaging.readthedocs.io/en/stable/',
        'version': 'fedora_messaging-<version> with pika-<version>',
    }

Apps should set the ``app`` along with any additional keys they feel will help
administrators when debugging application connections. At a minimum, the recommended
fields are:

* ``app_url``: The value of this key should be a URL to the upstream project for
  the client.
* ``app_contacts_email``: One or more emails of maintainers to contact with
  questions (if, for example, a client is misbehaving, or a service disruption
  is about to occur).

Do not use the ``product``, ``information``, and ``version`` keys as these will
be set automatically.


.. _conf-exchanges:

exchanges
---------
A dictionary of exchanges that should be present in the broker. Each key should
be an exchange name, and the value should be a dictionary with the exchange's
configuration.  Options are:

* ``type`` - the type of exchange to create.

* ``durable`` - whether or not the exchange should survive a broker restart.

* ``auto_delete`` - whether or not the exchange should be deleted once no queues
  are bound to it.

* ``arguments`` - dictionary of arbitrary keyword arguments for the exchange,
  which depends on the broker in use and its extensions.

For example::

    {
        'my_exchange': {
            'type': 'fanout',
            'durable': True,
            'auto_delete': False,
            'arguments': {},
        },
    }

The default is to ensure the 'amq.topic' topic exchange exists which should be
sufficient for most use cases.

.. _conf-log-config:

log_config
----------
A dictionary describing the logging configuration to use, in a format accepted
by :func:`logging.config.dictConfig`.

.. note:: Logging is only configured for consumers, not for producers.


Publisher Options
=================

The following configuration options are publisher-related.

.. _conf-publish-exchange:

publish_exchange
----------------
A string that identifies the exchange to publish to. The default is
``amq.topic``.


.. _conf-topic-prefix:

topic_prefix
------------
A string that will be prepended to topics on sent messages.
This is useful to migrate from fedmsg, but should not be used otherwise.
The default is an empty string.

.. _conf-publish-priority:

publish_priority
----------------
A number that will be set as the priority for the messages. The range of
possible priorities depends on the ``x-max-priority`` argument of the
destination queue, as described in `RabbitMQ's priority documentation`_.
The default is ``None``, which RabbitMQ will interpret as zero.

.. _RabbitMQ's priority documentation: https://www.rabbitmq.com/priority.html

.. _sub-config:

Consumer Options
================

The following configuration options are consumer-related.

.. _conf-queues:

queues
------
A dictionary of queues that should be present in the broker. Each key should be
a queue name, and the value should be a dictionary with the queue's configuration.
Options are:

* ``durable`` - whether or not the queue should survive a broker restart. This is
  set to ``False`` for the default queue.

* ``auto_delete`` - whether or not the queue should be deleted once the
  consumer disconnects. This is set to ``True`` for the default queue.

* ``exclusive`` - whether or not the queue is exclusive to the current
  connection. This is set to ``False`` for the default queue.

* ``arguments`` - dictionary of arbitrary keyword arguments for the queue, which
  depends on the broker in use and its extensions. This is set to ``{}`` for the
  default queue

For example::

    {
        'my_queue': {
            'durable': True,
            'auto_delete': True,
            'exclusive': False,
            'arguments': {},
        },
    }


.. _conf-bindings:

bindings
--------
A list of dictionaries that define queue bindings to exchanges that consumers
will subscribe to. The ``queue`` key is the queue's name. The ``exchange`` key
should be the exchange name and the ``routing_keys`` key should be a list of
routing keys. For example::

    [
        {
            'queue': 'my_queue',
            'exchange': 'amq.topic',
            'routing_keys': ['topic1', 'topic2.#'],
        },
    ]

This would create two bindings for the ``my_queue`` queue, both to the
``amq.topic`` exchange. Consumers will consume from both queues.

.. _conf-callback:

callback
--------
The Python path of the callback. This should be in the format
``<module>:<object>``. For example, if the callback was called "my_callback"
and was located in the "my_module" module of the "my_package" package, the path
would be defined as ``my_package.my_module:my_callback``. The default is None.

Consult the :ref:`consumers` documentation for details on implementing a
callback.

.. _conf-consumer-config:

consumer_config
---------------
A dictionary for the consumer to use as configuration. The consumer should
access this key in its callback for any configuration it needs. Defaults to
an empty dictionary. If, for example, this dictionary contains the
``print_messages`` key, the callback can access this configuration with::

    from fedora_messaging import config

    def callback(message):
        if config.conf["consumer_config"]["print_messages"]:
            print(message)


.. _conf-qos:

qos
---
The quality of service settings to use for consumers. This setting is a
dictionary with two keys. ``prefetch_count`` specifies the number of messages
to pre-fetch from the server. Pre-fetching messages improves performance by
reducing the amount of back-and-forth between client and server. The downside
is if the consumer encounters an unexpected problem, messages won't be returned
to the queue and sent to a different consumer until the consumer times out.
``prefetch_size`` limits the size of pre-fetched messages (in bytes), with 0
meaning there is no limit. The default settings are::

    {
        'prefetch_count': 10,
        'prefetch_size': 0,
    }

.. _conf-monitoring:

monitoring
----------
The options for the embedded HTTP server dedicated to monitoring the service.
This is where you can configure the address and the port to be listened on.
If the section is empty, monitoring will be disabled.
The default value for ``address`` is an empty string, which means that the
service will listen on all interfaces. There is no default value for
``port``, you will have to choose a port.
"""


import copy
import logging
import logging.config
import os
from collections.abc import Coroutine, Mapping, Sequence
from importlib.metadata import version
from typing import (
    Any,
    Callable,
    Optional,
    TYPE_CHECKING,
    TypedDict,
    Union,
)


try:
    import tomllib
except ImportError:  # pragma: no cover
    import tomli as tomllib  # type: ignore

from . import exceptions


if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    from .message import Message

_log = logging.getLogger(__name__)

_fedora_version = version("fedora_messaging")
_pika_version = version("pika")


#: Possible values for a message callback.
CallbackType: "TypeAlias" = Union[
    Callable[["Message"], None], Callable[["Message"], Coroutine[Any, Any, None]]
]


class _BaseBinding(TypedDict):
    exchange: str


class BaseBinding(_BaseBinding, total=False):
    queue: str


class BindingConfig(BaseBinding):
    """Configuration for a binding on the broker.

    See: https://www.rabbitmq.com/docs/exchanges#what-are-bindings
    """

    routing_keys: Sequence[str]


#: Possible configuration formats for a binding on the broker.
BindingsType: "TypeAlias" = Union[BindingConfig, list[BindingConfig]]


class _QueueConfig(TypedDict):
    durable: bool
    auto_delete: bool
    exclusive: bool
    arguments: Mapping[str, Any]


class QueueConfig(_QueueConfig, total=False):
    """The configuration for a queue as expected by RabbitMQ.

    See: https://www.rabbitmq.com/docs/queues#properties
    """

    passive: bool


class NamedQueueConfig(QueueConfig):
    """The full definition of a non-anonymous queue as expected by RabbitMQ."""

    queue: str


class BaseExchangeConfig(TypedDict):
    durable: bool
    auto_delete: bool
    arguments: Union[Mapping[str, Any], None]


class _ExchangeDefinition(BaseExchangeConfig):
    exchange: str
    exchange_type: str


class ExchangeDefinition(_ExchangeDefinition, total=False):
    passive: bool


class ExchangeConfig(BaseExchangeConfig):
    """The configuration for an exchange on the broker.

    See: https://www.rabbitmq.com/docs/exchanges#properties
    """

    type: str


class ClientProperties(TypedDict):
    app: str
    product: str
    information: str
    version: str


class QOSConfig(TypedDict):
    prefetch_size: int
    prefetch_count: int


class TLSConfig(TypedDict):
    ca_cert: Optional[str]
    certfile: Optional[str]
    keyfile: Optional[str]


class ConfigType(TypedDict):
    amqp_url: str
    client_properties: ClientProperties
    publish_exchange: str
    topic_prefix: str
    publish_priority: Optional[int]
    passive_declares: bool
    exchanges: dict[str, ExchangeConfig]
    queues: dict[str, QueueConfig]
    bindings: list[BindingConfig]
    qos: QOSConfig
    callback: Optional[str]
    monitoring: dict[str, Any]
    consumer_config: dict[str, Any]
    tls: TLSConfig
    log_config: dict[str, Any]


# By default, use a server-generated queue name
_default_queue_name: str = ""

#: The default configuration settings for fedora-messaging. This should not be
#: modified and should be copied with :func:`copy.deepcopy`.
DEFAULTS: ConfigType = {
    "amqp_url": "amqp://?connection_attempts=3&retry_delay=5",
    #: The default client properties reported to the AMQP broker in the "start-ok"
    #: method of the connection negotiation. This allows the broker administrators
    #: to easily identify what a connection is being used for and the client's
    #: capabilities.
    "client_properties": {
        "app": "Unknown",
        "product": "Fedora Messaging with Pika",
        "information": "https://fedora-messaging.readthedocs.io/en/stable/",
        "version": f"fedora_messaging-{_fedora_version} with pika-{_pika_version}",
    },
    "publish_exchange": "amq.topic",
    "topic_prefix": "",
    "publish_priority": None,
    "passive_declares": False,
    "exchanges": {
        "amq.topic": {
            "type": "topic",
            "durable": True,
            "auto_delete": False,
            "arguments": {},
        }
    },
    "queues": {
        _default_queue_name: {
            "durable": False,
            "auto_delete": True,
            "exclusive": True,
            "arguments": {},
        }
    },
    "bindings": [{"queue": _default_queue_name, "exchange": "amq.topic", "routing_keys": ["#"]}],
    "qos": {"prefetch_size": 0, "prefetch_count": 10},
    "callback": None,
    "monitoring": {},
    "consumer_config": {},
    "tls": {"ca_cert": None, "certfile": None, "keyfile": None},
    "log_config": {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"simple": {"format": "[%(name)s %(levelname)s] %(message)s"}},
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "simple",
                "stream": "ext://sys.stdout",
            }
        },
        "loggers": {
            "fedora_messaging": {
                "level": "INFO",
                "propagate": False,
                "handlers": ["console"],
            }
        },
        # The root logger configuration; this is a catch-all configuration
        # that applies to all log messages not handled by a different logger
        "root": {"level": "WARNING", "handlers": ["console"]},
    },
}


def validate_bindings(bindings: BindingsType) -> None:
    """
    Validate the bindings configuration.

    Raises:
        exceptions.ConfigurationException: If the configuration provided is of an
            invalid format.
    """
    if not isinstance(bindings, (list, tuple)):
        raise exceptions.ConfigurationException(
            f"bindings must be a list or tuple of dictionaries, but was a {type(bindings)}"
        )

    for binding in bindings:
        missing_keys = []
        for key in ("exchange", "routing_keys"):
            if key not in binding:
                missing_keys.append(key)
        if missing_keys:
            raise exceptions.ConfigurationException(
                "a binding is missing the following keys from its settings "
                f"value: {missing_keys}"
            )

        if not isinstance(binding["routing_keys"], (list, tuple)):
            raise exceptions.ConfigurationException(
                "routing_keys must be a list or tuple, but was a {}".format(
                    type(binding["routing_keys"])
                )
            )


def validate_queues(queues: dict[str, QueueConfig]) -> None:
    """
    Validate the queues configuration.

    Raises:
        exceptions.ConfigurationException: If the configuration provided is of an
            invalid format.
    """
    if not isinstance(queues, dict):
        raise exceptions.ConfigurationException(
            "'queues' must be a dictionary mapping queue names to settings."
        )

    for queue, settings in queues.items():
        if not isinstance(settings, dict):
            raise exceptions.ConfigurationException(
                f"the {queue} queue in the 'queues' setting has a value of type {type(settings)}, "
                "but it should be a dictionary of settings."
            )
        missing_keys = []
        for key in ("durable", "auto_delete", "exclusive", "arguments"):
            if key not in settings:
                missing_keys.append(key)
        if missing_keys:
            raise exceptions.ConfigurationException(
                f"the {queue} queue is missing the following keys from its settings "
                f"value: {missing_keys}"
            )


def validate_client_properties(props: dict[str, str]) -> None:
    """
    Validate the client properties setting.

    This will add the "version", "information", and "product" keys if they are
    missing. All other keys are application-specific.

    Raises:
        exceptions.ConfigurationException: If any of the basic keys are overridden.
    """
    for key in ("version", "information", "product"):
        # Nested dictionaries are not merged so key can be missing
        if key not in props:
            props[key] = DEFAULTS["client_properties"][key]
        # Don't let users override these as they identify this library in AMQP
        if props[key] != DEFAULTS["client_properties"][key]:
            raise exceptions.ConfigurationException(
                f'"{key}" is a reserved keyword in client_properties'
            )


def validate_monitoring(monitoring_conf: dict[str, Any]) -> None:
    """
    Validate the monitoring setting.

    This will add the "address" and "port" keys if they are missing.
    """
    if not monitoring_conf:
        return  # If empty, monitoring will be disabled.
    if "port" not in monitoring_conf:
        raise exceptions.ConfigurationException(
            "The port must be defined in [monitoring] to activate it"
        )
    if "address" not in monitoring_conf:
        monitoring_conf["address"] = ""


class LazyConfig(dict[str, Any]):
    """This class lazy-loads the configuration file."""

    loaded = False

    def __getitem__(self, *args: Any, **kw: Any) -> Any:
        if not self.loaded:
            self.load_config()
        return super().__getitem__(*args, **kw)

    def get(self, *args: Any, **kw: Any) -> Any:
        if not self.loaded:
            self.load_config()
        return super().get(*args, **kw)

    def pop(self, *args: Any, **kw: Any) -> None:
        raise exceptions.ConfigurationException("Configuration keys cannot be removed!")

    def copy(self, *args: Any, **kw: Any) -> dict[str, Any]:
        if not self.loaded:
            self.load_config()
        return super().copy(*args, **kw)

    def update(self, *args: Any, **kw: Any) -> None:
        if not self.loaded:
            self.load_config()
        return super().update(*args, **kw)

    def setup_logging(self) -> None:
        if not self.loaded:
            self.load_config()
        logging.config.dictConfig(self["log_config"])

    def _validate(self) -> None:
        """
        Perform checks on the configuration to assert its validity

        Raises:
            ConfigurationException: If the configuration is invalid.
        """
        for key in self:
            if key not in DEFAULTS:
                raise exceptions.ConfigurationException(
                    f"Unknown configuration key {key!r}! Valid configuration keys are"
                    f" {list(DEFAULTS.keys())}"
                )

        validate_queues(self["queues"])
        validate_bindings(self["bindings"])
        validate_client_properties(self["client_properties"])
        validate_monitoring(self["monitoring"])

    def load_config(self, config_path: Optional[str] = None) -> "LazyConfig":
        """
        Load application configuration from a file and merge it with the default
        configuration.

        If the ``FEDORA_MESSAGING_CONF`` environment variable is set to a
        filesystem path, the configuration will be loaded from that location.
        Otherwise, the path defaults to ``/etc/fedora-messaging/config.toml``.
        """
        self.loaded = True
        config = copy.deepcopy(DEFAULTS)

        if config_path is None:
            if "FEDORA_MESSAGING_CONF" in os.environ:
                config_path = os.environ["FEDORA_MESSAGING_CONF"]
            else:
                config_path = "/etc/fedora-messaging/config.toml"

        if os.path.exists(config_path):
            _log.info(f"Loading configuration from {config_path}")
            with open(config_path, "rb") as fd:
                try:
                    file_config = tomllib.load(fd)
                    for key in file_config:
                        config[key.lower()] = file_config[key]  # type: ignore[literal-required]
                except tomllib.TOMLDecodeError as e:
                    msg = f"Failed to parse {config_path}: {e}"
                    raise exceptions.ConfigurationException(msg) from e
        else:
            _log.info(f"The configuration file, {config_path}, does not exist.")

        self.update(config)
        self._validate()
        return self


#: The configuration dictionary used by fedora-messaging and consumers.
conf = LazyConfig()
