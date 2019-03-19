===================
Developer Interface
===================

This documentation covers the public interfaces fedora_messaging provides.

.. note:: Documented interfaces follow `Semantic Versioning 2.0.0`_. Any interface
          not documented here may change at any time without warning.

.. _semantic versioning 2.0.0: http://semver.org/


.. _pub-api:

Publishing
==========

.. autofunction:: fedora_messaging.api.publish


.. _sub-api:

Subscribing
===========

.. autofunction:: fedora_messaging.api.twisted_consume

.. autoclass:: fedora_messaging.api.Consumer
   :members:

.. autofunction:: fedora_messaging.api.consume


.. _signal-api:

Signals
=======

.. automodule:: fedora_messaging.signals

.. autodata:: fedora_messaging.api.pre_publish_signal
.. autodata:: fedora_messaging.api.publish_signal
.. autodata:: fedora_messaging.api.publish_failed_signal


.. _message-api:

Message Schemas
===============

.. automodule:: fedora_messaging.message
   :members: Message, get_class

.. _message-severity:

Message Severity
----------------

Each message can have a severity associated with it. The severity is used by
applications like the notification service to determine what messages to send
to users. The severity can be set at the class level, or on a message-by-message
basis. The following are valid severity levels:

.. autodata:: fedora_messaging.message.DEBUG
.. autodata:: fedora_messaging.message.INFO
.. autodata:: fedora_messaging.message.WARNING
.. autodata:: fedora_messaging.message.ERROR

.. _exceptions-api:


Utilities
---------

.. automodule:: fedora_messaging.schema_utils
   :members:


Exceptions
==========

.. automodule:: fedora_messaging.exceptions
   :members:


.. _twisted-api:

Twisted
=======

In addition to the synchronous API, a Twisted API is provided for applications
that need an asynchronous API. This API requires Twisted 16.1.0 or greater.

.. note:: This API is deprecated, please use :class:`fedora_messaging.api.twisted_consume`

Protocol
--------

.. automodule:: fedora_messaging.twisted.protocol
   :members: FedoraMessagingProtocol, Consumer

Factory
-------

.. automodule:: fedora_messaging.twisted.factory
   :members:

Service
-------

.. automodule:: fedora_messaging.twisted.service
   :members:
