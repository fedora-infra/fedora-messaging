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

Generic Options
===============

These options apply to both consumers and publishers.

.. _conf-amqp-url:

amqp_url
--------
The AMQP broker to connect to. This URL should be in the format described by
the :class:`pika.connection.URLParameters` documentation. This defaults to
``amqp://``.

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

It's recommended that applications only change the ``app`` key in the default
set of keys.

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


Publisher Options
=================

The following configuration options are publisher-related.

.. _conf-publish-exchange:

publish_exchange
----------------
A string that identifies the exchange to publish to. The default is
``amq.topic``.


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

* ``durable`` - whether or not the queue should survive a broker restart.

* ``auto_delete`` - whether or not the queue should be deleted once the
  consumer disconnects.

* ``exclusive`` - whether or not the queue is exclusive to the current
  connection.

* ``arguments`` - dictionary of arbitrary keyword arguments for the queue, which
  depends on the broker in use and its extensions.

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
            'queue': 'name',
            'exchange': 'amq.topic',
            'routing_keys': ['topic1', 'topic2.#'],
        },
    ]

This would create two bindings for the ``my_queue`` queue, both to the
``amq.topic`` exchange. Consumers will consume from both queues.

callback
--------
The Python path of the callback. This should be in the format
``<module>:<object>``. For example, if the callback was called "my_callback"
and was located in the "my_module" module of the "my_package" package, the path
would be defined as ``my_package.my_module:my_callback``. The default is None.

Consult the :ref:`consumers` documentation for details on implementing a
callback.

consumer_config
---------------
A dictionary for the consumer to use as configuration. The consumer should
access this key in its callback for any configuration it needs. Defaults to
an empty dictionary.

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

import logging
import logging.config
import os
import sys
import uuid

import pkg_resources
import pytoml


_log = logging.getLogger(__name__)

_fedora_version = pkg_resources.get_distribution('fedora_messaging').version
_pika_version = pkg_resources.get_distribution('pika').version

# A default, auto-deleted queue for consumers
_default_queue_name = str(uuid.uuid4())

#: A dictionary of application configuration defaults.
DEFAULTS = dict(
    amqp_url='amqp://',
    #: The default client properties reported to the AMQP broker in the "start-ok"
    #: method of the connection negotiation. This allows the broker administrators
    #: to easily identify what a connection is being used for and the client's
    #: capabilities.
    client_properties={
        'app': 'Unknown',
        'product': 'Fedora Messaging with Pika',
        'information': 'https://fedora-messaging.readthedocs.io/en/stable/',
        'version': 'fedora_messaging-{} with pika-{}'.format(_fedora_version, _pika_version),
    },
    publish_exchange='amq.topic',
    exchanges={
        'amq.topic': {
            'type': 'topic',
            'durable': True,
            'auto_delete': False,
            'arguments': {},
        },
    },
    queues={
        _default_queue_name: {
            'durable': False,
            'auto_delete': True,
            'exclusive': False,
            'arguments': {},
        },
    },
    bindings=[
        {
            'queue': _default_queue_name,
            'exchange': 'amq.topic',
            'routing_keys': ['#'],
        },
    ],
    qos={
        'prefetch_size': 0,
        'prefetch_count': 10,
    },
    callback=None,
    consumer_config={},
    log_config={
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'simple': {
                'format': '[%(name)s %(levelname)s] %(message)s',
            },
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'simple',
                'stream': 'ext://sys.stdout',
            }
        },
        'loggers': {
            'fedora_messaging': {
                'level': 'INFO',
                'propagate': False,
                'handlers': ['console'],
            },
        },
        # The root logger configuration; this is a catch-all configuration
        # that applies to all log messages not handled by a different logger
        'root': {
            'level': 'WARNING',
            'handlers': ['console'],
        },
    },
)

# Start with a basic logging configuration, which will be replaced by any user-
# specified logging configuration when the configuration is loaded.
logging.config.dictConfig(DEFAULTS['log_config'])


def load(filename=None):
    """
    Load application configuration from a file and merge it with the default
    configuration.

    If the ``FEDORA_MESSAGING_CONF`` environment variable is set to a
    filesystem path, the configuration will be loaded from that location.
    Otherwise, the path defaults to ``/etc/fedora-messaging/config.toml``.
    """
    config = DEFAULTS.copy()

    if filename:
        config_path = filename
    elif 'FEDORA_MESSAGING_CONF' in os.environ:
        config_path = os.environ['FEDORA_MESSAGING_CONF']
    else:
        config_path = '/etc/fedora-messaging/config.toml'

    if os.path.exists(config_path):
        _log.info('Loading configuration from {}'.format(config_path))
        with open(config_path) as fd:
            try:
                file_config = pytoml.loads(fd.read())
                for key in file_config:
                    config[key.lower()] = file_config[key]
            except pytoml.core.TomlError as e:
                _log.error('Failed to parse {}: {}'.format(config_path, str(e)))
                sys.exit(1)
    else:
        _log.info('The configuration file, {}, does not exist.'.format(config_path))

    return config


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
        if not self.loaded:
            self.load_config()
        return super(LazyConfig, self).pop(*args, **kw)

    def copy(self, *args, **kw):
        if not self.loaded:
            self.load_config()
        return super(LazyConfig, self).copy(*args, **kw)

    def update(self, *args, **kw):
        if not self.loaded:
            self.load_config()
        return super(LazyConfig, self).update(*args, **kw)

    def load_config(self, filename=None):
        self.loaded = True
        self.update(load(filename=filename))
        logging.config.dictConfig(self['log_config'])
        return self


#: The application configuration dictionary.
conf = LazyConfig()
