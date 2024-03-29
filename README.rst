Fedora Messaging
================

.. image:: https://img.shields.io/pypi/v/fedora-messaging.svg
    :target: https://pypi.org/project/fedora-messaging/

.. image:: https://img.shields.io/pypi/pyversions/fedora-messaging.svg
    :target: https://pypi.org/project/fedora-messaging/

.. image:: https://readthedocs.org/projects/fedora-messaging/badge/?version=stable
    :alt: Documentation Status
    :target: https://fedora-messaging.readthedocs.io/en/stable/?badge=stable

.. image:: https://github.com/fedora-infra/fedora-messaging/actions/workflows/main.yml/badge.svg?branch=develop
    :alt: Tests Status
    :target: https://github.com/fedora-infra/fedora-messaging/actions/workflows/main.yml?query=branch%3Adevelop

.. image:: https://codecov.io/gh/fedora-infra/fedora-messaging/branch/master/graph/badge.svg
  :target: https://codecov.io/gh/fedora-infra/fedora-messaging

This package provides tools and APIs to make using Fedora's messaging
infrastructure easier. These include a framework for declaring message schemas,
a set of synchronous APIs to publish messages to AMQP brokers, a set of
asynchronous APIs to consume messages, and services to easily run consumers.

This library is designed to be a replacement for the `PyZMQ`_-backed `fedmsg`_
library in Fedora Infrastructure.

To get started, check out our `user guide`_.

Looking to contribute? We appreciate it! Check out the `contributor guide`_.


.. _`user guide`: https://fedora-messaging.readthedocs.io/en/latest/#user-guide
.. _`contributor guide`: https://fedora-messaging.readthedocs.io/en/latest/contributing.html
.. _`PyZMQ`: https://pyzmq.readthedocs.io/
.. _`fedmsg`: https://github.com/fedora-infra/fedmsg/
