===================
Developer Interface
===================

This documentation covers the public interfaces fedora_messaging provides.

.. note:: Documented interfaces follow `Semantic Versioning 2.0.0`_. Any interface
          not documented here may change at any time without warning.


.. _pub-api:

Publishing
==========

.. automodule:: fedora_messaging.api
   :members: publish

.. _sub-api:


Subscribing
===========

.. automodule:: fedora_messaging.api
   :members: consume


.. _message-api:

Message Schemas
===============

.. automodule:: fedora_messaging.message
   :members: Message, get_class


.. _semantic versioning 2.0.0: http://semver.org/
