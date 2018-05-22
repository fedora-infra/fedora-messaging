===================
Developer Interface
===================

This documentation covers the public interfaces fedora_messaging provides.

.. note:: Documented interfaces follow `Semantic Versioning 2.0.0`_. Any interface
          not documented here may change at any time without warning.


.. _pub-api:

Publishing
==========

.. autofunction:: fedora_messaging.api.publish

.. _sub-api:


Subscribing
===========

.. autofunction:: fedora_messaging.api.consume


.. _message-api:

Message Schemas
===============

.. automodule:: fedora_messaging.message
   :members: Message, get_class


Exceptions
==========

.. automodule:: fedora_messaging.exceptions
   :members:


.. _semantic versioning 2.0.0: http://semver.org/
