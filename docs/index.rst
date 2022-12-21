
================
Fedora Messaging
================

This package provides tools and APIs to make using Fedora's messaging
infrastructure easier. These include a framework for declaring message schemas,
a set of synchronous APIs to publish messages to AMQP brokers, a set of
asynchronous APIs to consume messages, and services to easily run consumers.

This library is designed to be a replacement for the `PyZMQ`_-backed `fedmsg`_
library.


.. toctree::
   :maxdepth: 2
   :caption: User Guide

   user-guide/installation
   user-guide/quick-start
   user-guide/configuration
   user-guide/publishing
   user-guide/messages
   user-guide/consuming
   user-guide/schemas
   user-guide/testing
   user-guide/cli


.. toctree::
   :maxdepth: 2
   :caption: Tutorial

   tutorial/installation
   tutorial/usage
   tutorial/schemas
   tutorial/exceptions
   tutorial/conversion


.. toctree::
   :maxdepth: 2
   :caption: API Documentation

   api/api
   api/wire-format


.. toctree::
   :maxdepth: 2
   :caption: Contributing

   contributing


.. toctree::
   :maxdepth: 1
   :caption: Release Notes

   changelog


.. _fedmsg: https://github.com/fedora-infra/fedmsg/
.. _PyZMQ: https://pyzmq.readthedocs.io/
