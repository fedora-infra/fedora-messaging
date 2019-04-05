
.. _fedora-broker:

=======================
Fedora's Message Broker
=======================

Once you've got an application that publishes or consumes messages, you need to
get it connected to Fedora's message broker. The message broker is located at
``amqps://rabbitmq.fedoraproject.org/``.


External Connections
====================

Fedora allows anyone on the Internet to create queues and consume from them in
the public ``/public_pubsub`` AMQP virtual host. This virtual host mirrors all
messages published to the restricted ``/pubsub`` virtual host.

These public queues have some restrictions applied to them. Firstly, they are
limited to about 50 megabytes in size, so if your application cannot handle the
message throughput messages will be automatically discarded once you hit this
limit. Secondly, queues that are set to be durable (in other words, not
exclusive or auto-deleted) are automatically deleted after approximately an
hour.

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

.. literalinclude:: ../configs/fedora.toml

Assuming the ``/etc/fedora-messaging/fedora.toml``,
``/etc/fedora-messaging/cacert.pem``, ``/etc/fedora-messaging/fedora-key.pem``,
and ``/etc/fedora-messaging/fedora-cert.pem`` files exist, the following
command will create a configuration file called ``my_config.toml`` with a
unique queue name for your consumer::

    $ sed -e "s/[0-9a-f]\{8\}-[0-9a-f]\{4\}-[0-9a-f]\{4\}-[0-9a-f]\{4\}-[0-9a-f]\{12\}/$(uuidgen)/g" \
        /etc/fedora-messaging/fedora.toml > my_config.toml

Run a quick test to make sure you can connect to the broker. The configuration file
comes with an example consumer which simply prints the message to standard output::

    $ fedora-messaging --conf my_config.toml consume

If all goes well, you'll see a log entry similar to::

    Successfully registered AMQP consumer Consumer(queue=af0f78d2-159e-4279-b404-7b8c1b4649cc, callback=<function printer at 0x7f9a59e077b8>)

This will be followed by the messages being sent inside Fedora's
Infrastructure.  All that's left to do is change the callback in the
configuration to use your consumer :ref:`conf-callback` and adjusting the
routing keys in your :ref:`conf-bindings` to receive only the messages your
consumer is interested in.


.. _key: https://raw.githubusercontent.com/fedora-infra/fedora-messaging/master/configs/fedora-key.pem
.. _certificate: https://raw.githubusercontent.com/fedora-infra/fedora-messaging/master/configs/fedora-cert.pem
.. _CA certificate: https://raw.githubusercontent.com/fedora-infra/fedora-messaging/master/configs/cacert.pem
