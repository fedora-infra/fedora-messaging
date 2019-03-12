Installation
============

.. highlight:: shell

Installing the library
----------------------

Create a Python virtual environment::

    mkdir fedora-messaging-tutorial
    cd fedora-messaging-tutorial
    mkvirtualenv -p python3 -a `pwd` fedora-messaging-tutorial
    workon fedora-messaging-tutorial

Install the library and its dependencies::

    pip install fedora-messaging
    # Alternatively, install it directly from the git repository
    git clone https://github.com/fedora-infra/fedora-messaging.git
    cd fedora-messaging
    pip install -e .

Make sure it is available and working::

    fedora-messaging --help


Setting up RabbitMQ
-------------------

Install RabbitMQ and start it::

    dnf install rabbitmq-server
    systemctl start rabbitmq-server

RabbitMQ has a web admin interface that you can access at:
http://localhost:15672/. The username is ``guest`` and the password is
``guest``. This interface lets you change the configuration, send messages and
read the messages in the queues. Keep it open in a browser tab, we'll need it
later.

If your project uses containers, consult the `RabbitMQ documentation`_ about containers.

.. _RabbitMQ documentation: https://www.rabbitmq.com/download.html#docker

Configuration
-------------

An example of the library configuration file is provided in the
``config.toml.example`` file. Copy that file to
``/etc/fedora-messaging/config.toml`` to make it available system-wide.
Alternatively, you can copy it to ``config.toml`` anywhere and set the
``FEDORA_MESSAGING_CONF`` environement variable to that file's path.

Refer to `the documentation`_ for a complete description of the configuration
options.

.. _the documentation: http://fedora-messaging.readthedocs.io/en/latest/configuration.html

Comment out the ``callback`` and ``bindings`` options, and all the
``[exchanges.custom_exchange]`` and ``[queues.my_queue]`` sections.

In the ``[client_properties]`` section, change the ``app`` value to ``Fedora
Messaging tutorial``.

