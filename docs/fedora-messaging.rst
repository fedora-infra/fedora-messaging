
.. _fm-cli:

================
fedora-messaging
================

Synopsis
========

``fedora-messaging`` COMMAND [OPTIONS] [ARGS]...


Description
===========

``fedora-messaging`` can be used to work with AMQP message brokers using the
``fedora-messaging`` library to start message consumers.


Options
=======

``--help``

    Show help text and exit.

``--conf``

    Path to a valid configuration file to use in place of the configuration in
    ``/etc/fedora-messaging/config.toml``.


Commands
========

There are three sub-commands, ``consume``, ``publish`` and ``record``, described
in detail in their own sections below.

``fedora-messaging consume [OPTIONS]``

    Starts a consumer process with a user-provided callback function to execute
    when a message arrives.

``fedora-messaging publish [OPTIONS] FILE``

    Loads serialized messages from a file and publishes them to the specified
    exchange.

``fedora-messaging record [OPTIONS] FILE``

    Records messages arrived from AMQP queue and saves them to file with specified
    name.


consume
-------

All options below correspond to settings in the configuration file. However,
not all available configuration keys can be overridden with options, so it is
recommended that for complex setups and production environments you use the
configuration file and no options on the command line.

``--app-name``

    The name of the application, used by the AMQP client to identify itself to
    the broker. This is purely for administrator convenience to determine what
    applications are connected and own particular resources.

    This option is equivalent to the ``app`` setting in the ``client_properties``
    section of the configuration file.

``--callback``

    The Python path to the callable object to execute when a message arrives.
    The Python path should be in the format ``module.path:object_in_module``
    and should point to either a function or a class. Consult the API
    documentation for the interface required for these objects.

    This option is equivalent to the ``callback`` setting in the configuration
    file.

``--routing-key``

    The AMQP routing key to use with the queue. This controls what messages are
    delivered to the consumer. Can be specified multiple times; any message
    that matches at least one will be placed in the message queue.

    Setting this option is equivalent to setting the ``routing_keys`` setting
    in *all* ``bindings`` entries in the configuration file.

``--queue-name``

    The name of the message queue in AMQP. Can contain ASCII letters, digits,
    hyphen, underscore, period, or colon. If one is not specified, a unique
    name will be created for you.

    Setting this option is equivalent to setting the ``queue`` setting in *all*
    ``bindings`` entries and creating a ``queue.<queue-name>`` section in the
    configuration file.

``--exchange``

    The name of the exchange to bind the queue to. Can contain ASCII letters,
    digits, hyphen, underscore, period, or colon.

    Setting this option is equivalent to setting the ``exchange`` setting
    in *all* ``bindings`` entries in the configuration file.


publish
-------

The publish command expects the message or messages provided in ``FILE`` to be
JSON objects with each message separated by a newline character. The JSON
object is described by the following JSON schema::

  {
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

They can be produced from ``Message`` objects by the
:func:`fedora_messaging.api.dumps` API. ``stdin`` can be used instead of a file
by providing ``-`` as an argument::

    $ fedora-messaging publish -

Options
~~~~~~~

``--exchange``

    The name of the exchange to publish to. Can contain ASCII letters,
    digits, hyphen, underscore, period, or colon.


record
------

``--limit``

    The maximum number of messages to record.

``--app-name``

    The name of the application, used by the AMQP client to identify itself to
    the broker. This is purely for administrator convenience to determine what
    applications are connected and own particular resources. If not specified,
    the default is ``recorder``.

    This option is equivalent to the ``app`` setting in the ``client_properties``
    section of the configuration file.

``--routing-key``

    The AMQP routing key to use with the queue. This controls what messages are
    delivered to the consumer. Can be specified multiple times; any message
    that matches at least one will be placed in the message queue.

    Setting this option is equivalent to setting the ``routing_keys`` setting
    in *all* ``bindings`` entries in the configuration file.

``--queue-name``

    The name of the message queue in AMQP. Can contain ASCII letters, digits,
    hyphen, underscore, period, or colon. If one is not specified, a unique
    name will be created for you.

    Setting this option is equivalent to setting the ``queue`` setting in *all*
    ``bindings`` entries and creating a ``queue.<queue-name>`` section in the
    configuration file.

``--exchange``

    The name of the exchange to bind the queue to. Can contain ASCII letters,
    digits, hyphen, underscore, period, or colon.

    Setting this option is equivalent to setting the ``exchange`` setting
    in *all* ``bindings`` entries in the configuration file.


Exit codes
==========

consume
-------
The ``consume`` command can exit for a number of reasons:

``0``

    The consumer intentionally halted by raising a ``HaltConsumer`` exception.

``2``

    The argument or option provided is invalid.

``10``

    The consumer was unable to declare an exchange, queue, or binding in the
    message broker. This occurs with the user does not have permission on the
    broker to create the object *or* the object already exists, but does not
    have the attributes the consumer expects (e.g. the consumer expects it to
    be a durable queue, but it is transient).

``11``

    The consumer encounters an unexpected error while registering the consumer
    with the broker. This is a bug in fedora-messaging and should be reported.

``12``

    The consumer is canceled by the message broker.  The consumer is typically
    canceled when the queue it is subscribed to is deleted on the broker, but
    other exceptional cases could result in this. The broker administrators
    should be consulted in this case.

``13``

    An unexpected general exception is raised by your consumer callback.

Additionally, consumer callbacks can cause the command to exit with a custom
exit code. Consult the consumer's documentation to see what error codes it uses.

publish
-------

``0``

    The messages were all successfully published.

``1``

    A general, unexpected exception occurred and the message was not successfully
    published.

``121``

    The message broker rejected the message, likely due to resource constraints.

``111``

    A connection to the broker could not be established.


Signals
=======

consume
-------

The ``consume`` command handles the SIGTERM and SIGINT signals by allowing any
consumers which are currently processing a message to finish, acknowledging the
message to the message broker, and then shutting down. Repeated SIGTERM or
SIGINT signals are ignored. To halt immediately, send the SIGKILL signal;
messages that are partially processed will be re-delivered when the consumer
restarts.


Systemd service
===============

The ``consume`` subcommand can be started as a system service, and Fedora
Messaging provides a dynamic systemd service file.

First, create a valid Fedora Messaging configuration file in
``/etc/fedora-messaging/foo.toml``, with the ``callback`` parameter pointing to
your consuming function or class. Remember that you can use the
``consumer_config`` section for your own configuration.

Enable and start the service in systemd with the following commands::

  systemctl enable fm-consumer@foo.service
  systemctl start fm-consumer@foo.service

The service name after the ``@`` and before the ``.service`` must match your
filename in ``/etc/fedora-messaging`` (without the ``.toml`` suffix).


Help
====

If you find bugs in fedora-messaging or its man page, please file a bug report
or a pull request::

    https://github.com/fedora-infra/fedora-messaging

Or, if you prefer, send an email to infrastructure@fedoraproject.org with bug
reports or patches.

fedora-messaging's documentation is available online::

    https://fedora-messaging.readthedocs.io/

.. _pika: http://pika.readthedocs.io/en/
