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

``--app-name``

    The name of the application, used by the AMQP client to identify itself to
    the broker. This is purely for administrator convenience to determine what
    applications are connected and own particular resources.

``--callback``

    The Python path to the callable object to execute when a message arrives.
    The Python path should be in the format ``module.path:object_in_module``
    and should point to either a function or a class. Consult the API
    documentation for the interface required for these objects.

``--routing-key``

    The AMQP routing key to use with the queue. This controls what messages are
    delivered to the consumer. Can be specified multiple times; any message
    that matches at least one will be placed in the message queue.

``--queue-name``

    The name of the message queue in AMQP. Can contain ASCII letters, digits,
    hyphen, underscore, period, or colon. If one is not specified, a unique
    name will be created for you.

``--exchange``

    The name of the exchange to bind the queue to. Can contain ASCII letters,
    digits, hyphen, underscore, period, or colon. If one is not specified, the
    default is the ``amq.topic`` exchange.


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
