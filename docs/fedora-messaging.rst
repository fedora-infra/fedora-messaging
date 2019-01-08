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

There is a single sub-command, ``consume``, described in detail in its ow
section below.

``fedora-messaging consume [OPTIONS]``

    Starts a consumer process with a user-provided callback function to execute
    when a message arrives.


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
    digits, hyphen, underscore, period, or colon. If one is not specified, the
    default is the ``amq.topic`` exchange.

    Setting this option is equivalent to setting the ``exchange`` setting
    in *all* ``bindings`` entries in the configuration file.


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
