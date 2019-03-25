
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


.. _key: https://raw.githubusercontent.com/fedora-infra/fedora-messaging/master/configs/fedora-key.pem
.. _certificate: https://raw.githubusercontent.com/fedora-infra/fedora-messaging/master/configs/fedora-cert.pem
.. _CA certificate: https://raw.githubusercontent.com/fedora-infra/fedora-messaging/master/configs/cacert.pem
