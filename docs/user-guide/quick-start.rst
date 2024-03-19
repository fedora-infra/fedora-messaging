
.. _quick-start:

===========
Quick Start
===========

This is a quick-start guide that covers a few common use-cases and contains
pointers to more in-depth documentation for the curious.


.. _local-broker:

Local Broker
============

To publish and consume messages locally can be a useful way to learn about the
library, and is also helpful during development of your application or service.

To install the message broker on Fedora::

    $ sudo dnf install rabbitmq-server

RabbitMQ is also available in EPEL7, although it is quite old and the library
is not regularly tested against it. You can also install the broker from
RabbitMQ directly if you are not using Fedora.

Next, it's recommended that you enable the management interface::

    $ sudo rabbitmq-plugins enable rabbitmq_management

This provides an HTTP interface and API, available at http://localhost:15672/
by default. The "guest" user with the password "guest" is created by default.

Finally, start the broker::

    $ sudo systemctl start rabbitmq-server

You should now be able to consume messages with the following Python script::

    from fedora_messaging import api, config

    config.conf.setup_logging()
    api.consume(lambda message: print(message))

To learn more about consuming messages, check out the :ref:`consumers`
documentation.

You can publish messages with::

    from fedora_messaging import api, config

    config.conf.setup_logging()
    api.publish(api.Message(topic="hello", body={"Hello": "world!"}))

To learn more about publishing messages, check out the :ref:`publishing`
documentation.


.. _fedora-broker:

Fedora's Public Broker
======================

Fedora's message broker has a publicly accessible virtual host located at
``amqps://rabbitmq.fedoraproject.org/%2Fpublic_pubsub``. This virtual host
mirrors all messages published to the restricted ``/pubsub`` virtual host and
allows anyone to consume messages being published by the various Fedora
services.

These public queues have some restrictions applied to them. Firstly, they are
limited to about 50 megabytes in size, so if your application cannot handle the
message throughput messages will be automatically discarded once you hit this
limit. Secondly, queues that are set to be durable (in other words, not
exclusive or auto-deleted) are automatically deleted if they have no consumers
after approximately an hour.

If you need more robust guarantees about message delivery, or if you need to
publish messages into Fedora's message broker, contact the Fedora
Infrastructure team about getting access to the private virtual host.


Getting Connected
-----------------

The public virtual host still requires users to authenticate when connecting,
so a public user has been created and its private key and x509 certificate are
distributed with fedora-messaging.

If fedora-messaging was installed via RPM, they should be in
``/etc/fedora-messaging/`` along with a configuration file called
``fedora.toml``. If it's been installed via pip, it's easiest to get the
`key`_, `certificate`_, and the `CA certificate`_ from the upstream git
repository and start with the following configuration file:

.. literalinclude:: ../../configs/fedora.toml

Assuming the ``/etc/fedora-messaging/fedora.toml``,
``/etc/fedora-messaging/cacert.pem``, ``/etc/fedora-messaging/fedora-key.pem``,
and ``/etc/fedora-messaging/fedora-cert.pem`` files exist, the following
command will create a configuration file called ``my_config.toml`` with a
unique queue name for your consumer::

    $ sed -e "s/[0-9a-f]\{8\}-[0-9a-f]\{4\}-[0-9a-f]\{4\}-[0-9a-f]\{4\}-[0-9a-f]\{12\}/$(uuidgen)/g" \
        /etc/fedora-messaging/fedora.toml > my_config.toml

.. warning:: Do not skip the step above. This is important because if there are
    multiple consumers on a queue the broker delivers messages to them in a
    round-robin fashion. In other words, you'll only get some of the messages being
    sent.

Run a quick test to make sure you can connect to the broker. The configuration file
comes with an example consumer which simply prints the message to standard output::

    $ fedora-messaging --conf my_config.toml consume

Alternatively, you can start a Python shell and use the API::

    $ FEDORA_MESSAGING_CONF=my_config.toml python
    >>> from fedora_messaging import api, config
    >>> config.conf.setup_logging()
    >>> api.consume(lambda message: print(message))

If all goes well, you'll see a log entry similar to::

    Successfully registered AMQP consumer Consumer(queue=af0f78d2-159e-4279-b404-7b8c1b4649cc, callback=<function printer at 0x7f9a59e077b8>)

This will be followed by the messages being sent inside Fedora's
Infrastructure.  All that's left to do is change the callback in the
configuration to use your consumer :ref:`conf-callback` and adjusting the
routing keys in your :ref:`conf-bindings` to receive only the messages your
consumer is interested in.

.. _key: https://raw.githubusercontent.com/fedora-infra/fedora-messaging/stable/configs/fedora-key.pem
.. _certificate: https://raw.githubusercontent.com/fedora-infra/fedora-messaging/stable/configs/fedora-cert.pem
.. _CA certificate: https://raw.githubusercontent.com/fedora-infra/fedora-messaging/stable/configs/cacert.pem


.. _fedora-restricted-broker:

Fedora's Restricted Broker
==========================

Connecting the Fedora's private virtual host requires working with the Fedora
infrastructure team. The current process and configuration for this is
documented in the `infrastructure team's development guide
<https://docs.fedoraproject.org/en-US/infra/developer_guide/messaging/>`_.
