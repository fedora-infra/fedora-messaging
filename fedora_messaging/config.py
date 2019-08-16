# This file is part of fedora_messaging.
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
"""
fedora-messaging can be configured with the
``/etc/fedora-messaging/config.toml`` file or by setting the
``FEDORA_MESSAGING_CONF`` environment variable to the path of the configuration
file.

Each configuration option has a default value.

.. contents:: Table of Configuration Options
    :local:

A complete example TOML configuration:

.. literalinclude:: ../config.toml.example


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
"""
from __future__ import unicode_literals

import copy
import logging
import logging.config
import os
import uuid

import pkg_resources
import toml

from . import exceptions


_log = logging.getLogger(__name__)

_fedora_version = pkg_resources.get_distribution("fedora_messaging").version
_pika_version = pkg_resources.get_distribution("pika").version

# A default, auto-deleted queue for consumers
_default_queue_name = str(uuid.uuid4())

#: The default configuration settings for fedora-messaging. This should not be
#: modified and should be copied with :func:`copy.deepcopy`.
DEFAULTS = dict(
    amqp_url="amqp://?connection_attempts=3&retry_delay=5",
    #: The default client properties reported to the AMQP broker in the "start-ok"
    #: method of the connection negotiation. This allows the broker administrators
    #: to easily identify what a connection is being used for and the client's
    #: capabilities.
    client_properties={
        "app": "Unknown",
        "product": "Fedora Messaging with Pika",
        "information": "https://fedora-messaging.readthedocs.io/en/stable/",
        "version": "fedora_messaging-{} with pika-{}".format(
            _fedora_version, _pika_version
        ),
    },
    publish_exchange="amq.topic",
    topic_prefix="",
    passive_declares=False,
    exchanges={
        "amq.topic": {
            "type": "topic",
            "durable": True,
            "auto_delete": False,
            "arguments": {},
        }
    },
    queues={
        _default_queue_name: {
            "durable": False,
            "auto_delete": True,
            "exclusive": False,
            "arguments": {},
        }
    },
    bindings=[
        {"queue": _default_queue_name, "exchange": "amq.topic", "routing_keys": ["#"]}
    ],
    qos={"prefetch_size": 0, "prefetch_count": 10},
    callback=None,
    consumer_config={},
    tls={"ca_cert": None, "certfile": None, "keyfile": None},
    log_config={
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
)


def validate_bindings(bindings):
    """
    Validate the bindings configuration.

    Raises:
        exceptions.ConfigurationException: If the configuration provided is of an
            invalid format.
    """
    if not isinstance(bindings, (list, tuple)):
        raise exceptions.ConfigurationException(
            "bindings must be a list or tuple of dictionaries, but was a {}".format(
                type(bindings)
            )
        )

    for binding in bindings:
        missing_keys = []
        for key in ("queue", "exchange", "routing_keys"):
            if key not in binding:
                missing_keys.append(key)
        if missing_keys:
            raise exceptions.ConfigurationException(
                "a binding is missing the following keys from its settings "
                "value: {}".format(missing_keys)
            )

        if not isinstance(binding["routing_keys"], (list, tuple)):
            raise exceptions.ConfigurationException(
                "routing_keys must be a list or tuple, but was a {}".format(
                    type(binding["routing_keys"])
                )
            )


def validate_queues(queues):
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
                "the {} queue in the 'queues' setting has a value of type {}, but it "
                "should be a dictionary of settings.".format(queue, type(settings))
            )
        missing_keys = []
        for key in ("durable", "auto_delete", "exclusive", "arguments"):
            if key not in settings:
                missing_keys.append(key)
        if missing_keys:
            raise exceptions.ConfigurationException(
                "the {} queue is missing the following keys from its settings "
                "value: {}".format(queue, missing_keys)
            )


def validate_client_properties(props):
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
                '"{}" is a reserved keyword in client_properties'.format(key)
            )


class LazyConfig(dict):
    """This class lazy-loads the configuration file."""

    loaded = False

    def __getitem__(self, *args, **kw):
        if not self.loaded:
            self.load_config()
        return super(LazyConfig, self).__getitem__(*args, **kw)

    def get(self, *args, **kw):
        if not self.loaded:
            self.load_config()
        return super(LazyConfig, self).get(*args, **kw)

    def pop(self, *args, **kw):
        raise exceptions.ConfigurationException("Configuration keys cannot be removed!")

    def copy(self, *args, **kw):
        if not self.loaded:
            self.load_config()
        return super(LazyConfig, self).copy(*args, **kw)

    def update(self, *args, **kw):
        if not self.loaded:
            self.load_config()
        return super(LazyConfig, self).update(*args, **kw)

    def setup_logging(self):
        if not self.loaded:
            self.load_config()
        logging.config.dictConfig(self["log_config"])

    def _validate(self):
        """
        Perform checks on the configuration to assert its validity

        Raises:
            ConfigurationException: If the configuration is invalid.
        """
        for key in self:
            if key not in DEFAULTS:
                raise exceptions.ConfigurationException(
                    'Unknown configuration key "{}"! Valid configuration keys are'
                    " {}".format(key, list(DEFAULTS.keys()))
                )

        validate_queues(self["queues"])
        validate_bindings(self["bindings"])
        validate_client_properties(self["client_properties"])

    def load_config(self, config_path=None):
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
            _log.info("Loading configuration from {}".format(config_path))
            with open(config_path) as fd:
                try:
                    file_config = toml.load(fd)
                    for key in file_config:
                        config[key.lower()] = file_config[key]
                except toml.TomlDecodeError as e:
                    msg = "Failed to parse {}: error at line {}, column {}: {}".format(
                        config_path, e.lineno, e.colno, e.msg
                    )
                    raise exceptions.ConfigurationException(msg)
        else:
            _log.info("The configuration file, {}, does not exist.".format(config_path))

        self.update(config)
        self._validate()
        return self


#: The configuration dictionary used by fedora-messaging and consumers.
conf = LazyConfig()
