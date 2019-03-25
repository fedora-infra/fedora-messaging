
================
Fedora Messaging
================

This package provides tools and APIs to make using Fedora's messaging
infrastructure easier. These include a framework for declaring message schemas,
a set of synchronous APIs to publish messages to AMQP brokers, a set of
asynchronous APIs to consume messages, and services to easily run consumers.

This library is designed to be a replacement for the `PyZMQ`_-backed `fedmsg`_
library.


User Guide
==========

.. toctree::
   :maxdepth: 2

   installation
   configuration
   publishing
   consuming
   messages
   testing
   fedora-broker
   changelog


Command Line Interface Manuals
------------------------------

.. toctree::
   :maxdepth: 2

   fedora-messaging


Tutorial
========

.. toctree::
   :maxdepth: 2

   tutorial/index


API Documentation
=================

.. toctree::
   :maxdepth: 2

   api
   wire-format


Contributor Guide
=================

.. toctree::
   :maxdepth: 2

   contributing


.. _fedmsg: https://github.com/fedora-infra/fedmsg/
.. _PyZMQ: https://pyzmq.readthedocs.io/
