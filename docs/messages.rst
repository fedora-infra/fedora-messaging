.. _messages:

========
Messages
========

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
another based on the version of the schema used by a message.

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

.. include:: sample_schema_package/mailman_messages/messages.py
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
