.. _messages:

===============
Message Schemas
===============

Before you release your application, you should create a subclass of
:class:`fedora_messaging.message.Message`, define a schema, define a default
severity, and implement some methods.

Schema
======

Defining a message schema is important for several reasons.

First and foremost, if will help you (the developer) ensure you don't
accidentally change your message's format. When messages are being generated
from, say, a database object, it's easy to make a schema change to the database
and unintentionally alter your message, which breaks consumers of your message.
Without a schema, you might not catch this until you deploy your application
and consumers start crashing. With a schema, you'll get an error as you
develop!

Secondly, it allows you to change your message format in a controlled fashion
by versioning your schema. You can then choose to implement methods one way or
another based on the version of the schema used by a message. For details on how
to deprecate and upgrade message schemas, see :ref:`schema-upgrade`.

Message schema are defined using `JSON Schema`_. The complete API can be found
in the :ref:`message-api` API documentation.


.. _header-schema:

Header Schema
-------------

The default header schema declares that the header field must be a JSON object
with several expected keys. You can leave the schema as-is when you define your
own message, or you can refine it. The base schema will always be enforced in
addition to your custom schema.


.. _body-schema:

Body Schema
-----------

The default body schema simply declares that the header field must be a JSON
object.


Example Schema
--------------

.. include:: ../sample_schema_package/mailman_messages/messages.py
   :literal:

Note that message schema can be composed of other message schema, and
validation of fields can be much more detailed than just a simple type check.
Consult the `JSON Schema`_ documentation for complete details.


Message Conventions
===================

Schema are Immutable
--------------------

Message schema should be treated as immutable. Once defined, they should not be
altered. Instead, define a new schema class, mark the old one as deprecated,
and remove it after an appropriate transition period.

Provide Accessors
-----------------

The JSON schema ensures the message sent "on the wire" conforms to a particular
format. Messages should provide Python properties to access the deserialized
JSON object. This Python API should maintain backwards compatibility between
schema. This shields consumers from changes in schema.

Useful Accessors
~~~~~~~~~~~~~~~~

All available accessors are described in the :ref:`message-api` API documentation ;
here is a list of those we recommend implementing to allow users to get
notifications for your messages:

* :py:meth:`~fedora_messaging.message.Message.__str__`:
  A human-readable representation of this message. This can be a multi-line string
  that forms the body of an email notification.
* :py:attr:`~fedora_messaging.message.Message.summary`:
  A short, single-line, human-readable summary of the message, much like the subject
  line of an email.
* :py:attr:`~fedora_messaging.message.Message.agent_name`:
  The username of the user who caused the action.
* :py:attr:`~fedora_messaging.message.Message.app_name`:
  The name of the application that generated the message. This can be implemented as
  a class attribute or as a property.
* :py:attr:`~fedora_messaging.message.Message.app_icon`:
  A URL to the icon of the application that generated the message. This can be
  implemented as a class attribute or as a property.
* :py:attr:`~fedora_messaging.message.Message.packages`:
  A list of RPM packages affected by the action that generated this message, if any.
* :py:attr:`~fedora_messaging.message.Message.flatpaks`:
  A list of flatpaks affected by the action that generated this message, if any.
* :py:attr:`~fedora_messaging.message.Message.modules`:
  A list of modules affected by the action that generated this message, if any.
* :py:attr:`~fedora_messaging.message.Message.containers`:
  A list of containers affected by the action that generated this message, if any.
* :py:attr:`~fedora_messaging.message.Message.usernames`:
  A list of usernames affected by the action that generated this message.
  This may contain the ``agent_name``.
* :py:attr:`~fedora_messaging.message.Message.groups`:
  A list of group names affected by the action that generated this message.
* :py:attr:`~fedora_messaging.message.Message.url`:
  A URL to the action that caused this message to be emitted, if any.
* :py:attr:`~fedora_messaging.message.Message.severity`:
  An integer that indicates the severity of the message. This is used to determine
  what messages to notify end users about and should be
  :py:data:`~fedora_messaging.message.DEBUG`,
  :py:data:`~fedora_messaging.message.INFO`,
  :py:data:`~fedora_messaging.message.WARNING`,
  or :py:data:`~fedora_messaging.message.ERROR`.
  The default is :py:data:`~fedora_messaging.message.INFO`, and can be set
  as a class attribute or on an instance-by-instance basis.


Packaging
=========

Finally, you must distribute your schema to clients. It is recommended that you
maintain your message schema in your application's git repository in a separate
Python package. The package name should be ``<your-app-name>-messages``.

A complete sample schema package can be found in `the fedora-messaging
repository`_. This includes unit tests, the schema classes, and a setup.py. You
can adapt this boilerplate with the following steps:

* Change the package name from ``mailman_messages`` to ``<your-app-name>_messages``
  in ``setup.py``.

* Rename the ``mailman_messages`` directory to ``<your-app-name>_messages``.

* Add your schema classes to ``messages.py`` and tests to
  ``tests/test_messages.py``.

* Update the ``README`` file.

* Build the distribution with ``python setup.py sdist bdist_wheel``.

* Upload the sdist and wheel to PyPI with ``twine``.

* Submit an RPM package for it to Fedora and EPEL.

If you prefer `CookieCutter`_, there is a `template repository`_ that you can use with the
command::

    cookiecutter gh:fedora-infra/cookiecutter-message-schemas

It will ask you for the application name and some other variables, and will create the package
structure for you.

.. _JSON Schema: http://json-schema.org/
.. _the fedora-messaging repository: https://github.com/fedora-infra/fedora-messaging/tree/master/docs/sample_schema_package/
.. _CookieCutter: https://cookiecutter.readthedocs.io
.. _template repository: https://github.com/fedora-infra/cookiecutter-message-schemas



.. _schema-upgrade:

Upgrade and deprecation
=======================

Message schema classes should not be modified in a backwards-incompatible fashion. To facilitate the
evolution of schemas, we recommend including the schema version in the topic itself, such as
``myservice.myevent.v1``.

When a backwards-incompatible change is required, create a new class with the topic ending in
``.v2``, set the :py:attr:`Message.deprecated` attribute to ``True`` on the old class, and send both
versions for a reasonable period of time. Note that you need to add the new class to the schema
package's entry points as well.

We leave the duration to the developer's appreciation, since it depends on how many different
consumers they expect to have, whether they are only inside the Fedora infrastructure or outside
too, etc. This duration can range from weeks to months, possibly a year. At the time of this
writing, Fedora's message bus is very far from being overwhelmed by messages, so you don't need to
worry about that.

Proceeding this way ensures that consumers subscribing to ``.v1`` will not break when ``.v2``
arrives, and can choose to subscribe to the ``.v2`` topic when they are ready to handle the new
format. They will get a warning in their logs when they receive deprecated messages, prompting them
to upgrade.

When you add the new version, please upgrade the major version number of your schema
package, and communicate clearly that the old version is deprecated, including for how long you have
decided to send both versions.
