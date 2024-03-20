=============
Release Notes
=============

.. towncrier release notes start

3.5.0 (2024-03-20)
==================

Features
--------

* Add a replay command
  (`#332 <https://github.com/fedora-infra/fedora-messaging/issues/332>`_)
* Add support Python 3.11 and 3.12, drop support for Python 3.6 and 3.7
* Better protection against invalid bodies breaking the headers generation and the instanciation of a message
* Testing framework: make the sent messages available in the context manager

Documentation Improvements
--------------------------

* Add SECURITY.md for project security policy
  (`PR#314 <https://github.com/fedora-infra/fedora-messaging/pull/314>`_)
* Add fedora-messaging-git-hook-messages to the known schema packages

Development Changes
-------------------

* Make the tests use the pytest fixtures and assert system
  (`#961b82d <https://github.com/fedora-infra/fedora-messaging/issues/961b82d>`_)
* Make fedora-messaging use poetry
  (`#294 <https://github.com/fedora-infra/fedora-messaging/issues/294>`_)
* Add some generic pre-commit checks
* Don't distribute the tests in the wheel

Contributors
------------

Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Aurélien Bompard
* Thibaut Batale
* Khaled Achech
* Lenka Segura
* Ryan Lerch

3.4.1 (2023-05-26)
==================

Bug Fixes
---------

* Fix CI
  (`0f2e39c <https://github.com/fedora-infra/fedora-messaging/commit/0f2e39c>`_)


3.4.0 (2023-05-26)
==================

Features
--------

* Mirror the message priority in the headers
  (`eba336b <https://github.com/fedora-infra/fedora-messaging/commit/eba336b>`_)


3.3.0 (2023-03-31)
==================

Features
--------

* Add support for asyncio-based callbacks in the consumer. As a consequence,
  the Twisted reactor used by the CLI is now ``asyncio``.
  (`PR#282 <https://github.com/fedora-infra/fedora-messaging/pull/282>`_)

Documentation Improvements
--------------------------

* Improve documentation layout, and add some documentation on the message
  schemas.
  (`1fa8998
  <https://github.com/fedora-infra/fedora-messaging/commit/1fa8998>`_)
* Add koji-fedoramessaging-messages to the list of known schemas.
  (`ef12fa2
  <https://github.com/fedora-infra/fedora-messaging/commit/ef12fa2>`_)

Development Changes
-------------------

* Update pre-commit linters.
  (`0efdde1
  <https://github.com/fedora-infra/fedora-messaging/commit/0efdde1>`_)


3.2.0 (2022-10-17)
==================

Features
--------

* Add a priority property to messages, and a default priority in the
  configuration
  (`PR#275 <https://github.com/fedora-infra/fedora-messaging/pull/275>`_)
* Add a message schema attribute and some documentation to help deprecate and
  upgrade message schemas
  (`#227 <https://github.com/fedora-infra/fedora-messaging/issues/227>`_)

Other Changes
-------------

* Use tomllib from the standard library on Python 3.11 and above,
  fallback to tomli otherwise.
  (`PR#274 <https://github.com/fedora-infra/fedora-messaging/pull/274>`_)

Contributors
------------

Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Akashdeep Dhar
* Aurélien Bompard
* Erol Keskin
* Miro Hrončok
* Stephen Coady


3.1.0 (2022-09-13)
==================

Features
--------

* Add the ``app_name`` and ``agent_name`` properties to message schemas
  (`PR#272 <https://github.com/fedora-infra/fedora-messaging/pull/272>`_)
* Added "groups" property to message schemas. This property can be used if an
  event affects a list of groups.
  (`#252 <https://github.com/fedora-infra/fedora-messaging/issues/252>`_)


3.0.2 (2022-05-19)
==================

Development Changes
-------------------

* Fix CI in Github actions
  (`6257100 <https://github.com/fedora-infra/fedora-messaging/commit/6257100>`_)
* Update pre-commit checkers
  (`1d35a5d <https://github.com/fedora-infra/fedora-messaging/commit/1d35a5d>`_)
* Fix Packit configuration
  (`d2ea85f <https://github.com/fedora-infra/fedora-messaging/commit/d2ea85f>`_)

Contributors
------------
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Akashdeep Dhar
* Aurélien Bompard


3.0.1 (2022-05-12)
==================

Development Changes
-------------------

* Add packit configuration allowing us to have automatic downstream RPM builds
  (`#259 <https://github.com/fedora-infra/fedora-messaging/issues/259>`_)
* Don't build universal wheels since we don't run on Python 2 anymore
  (`e8c5f4c <https://github.com/fedora-infra/fedora-messaging/commit/e8c5f4c>`_)


Documentation Improvements
--------------------------

* Add some schema packages to the docs
  (`03e7f42 <https://github.com/fedora-infra/fedora-messaging/commit/03e7f42>`_)
* Change the example email addresses
  (`1555742 <https://github.com/fedora-infra/fedora-messaging/commit/1555742>`_)


Contributors
------------
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Akashdeep Dhar
* Aurélien Bompard


3.0.0 (2021-12-14)
==================

API Changes
-----------

* Queues created by the CLI are now non-durable, auto-deleted and exclusive, as
  server-named queues are.
  (`PR#239 <https://github.com/fedora-infra/fedora-messaging/pull/239>`_)

* It is no longer necessary to declare a queue in the configuration file: a
  server-named queue will be created. Configured bindings which do not specify
  a queue name will be applied to the server-named queue.
  (`PR#239 <https://github.com/fedora-infra/fedora-messaging/pull/239>`_)

* Drop support for Python 2
  (`PR#246 <https://github.com/fedora-infra/fedora-messaging/pull/246>`_)

* Drop the Twisted classes that had been flagged as deprecated.
  Drop the deprecated ``Message._body`` property.
  Refactor the consuming code into the ``Consumer`` class.
  (`PR#249 <https://github.com/fedora-infra/fedora-messaging/pull/249>`_)


Features
--------

* Support anonymous (server-named) queues.
  (`PR#239 <https://github.com/fedora-infra/fedora-messaging/pull/239>`_)

* Support Python 3.10
  (`PR#250 <https://github.com/fedora-infra/fedora-messaging/pull/250>`_)

* Raise ``PublishForbidden`` exception immediately if publishing to `virtual host <https://www.rabbitmq.com/access-control.html>`_ is denied rather than waiting until timeout occurs.
  (`#203 <https://github.com/fedora-infra/fedora-messaging/issues/203>`_)


Bug Fixes
---------

* Fixed validation exception of queue field on serialized schemas.
  (`#240 <https://github.com/fedora-infra/fedora-messaging/issues/240>`_)


Documentation Improvements
--------------------------

* Improve release notes process documentation.
  (`PR#238 <https://github.com/fedora-infra/fedora-messaging/pull/238>`_)

* Build a list of available topics in the documentation from known schema packages
  (`PR#242 <https://github.com/fedora-infra/fedora-messaging/pull/242>`_)


Development Changes
-------------------

* Start using pre-commit for linters and formatters
  (`732c7fb <https://github.com/fedora-infra/fedora-messaging/commit/732c7fb>`_)


Contributors
------------
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Aurélien Bompard
* David Jimenez
* Michal Konečný
* Onur Ozkan


2.1.0 (2021-05-12)
==================

Features
--------

* Improve the testing module to check message topics and bodies separately,
  and to use the rewritten assert that pytest provides
  (`PR#230 <https://github.com/fedora-infra/fedora-messaging/pull/230>`_)

* Handle `topic authorization <https://www.rabbitmq.com/access-control.html#topic-authorisation>`_
  by raising a ``PublishForbidden`` exception instead of being stuck in a retry loop
  (`PR#235 <https://github.com/fedora-infra/fedora-messaging/pull/235>`_)

* Test on Python 3.8 and 3.9
  (`PR#237 <https://github.com/fedora-infra/fedora-messaging/pull/237>`_)


Bug Fixes
---------

* Require setuptools, as ``pkg_resources`` is used
  (`PR#233 <https://github.com/fedora-infra/fedora-messaging/pull/233>`_)


Development Changes
-------------------

* Update test fixture keys to 4096 bits
  (`PR#232 <https://github.com/fedora-infra/fedora-messaging/pull/232>`_)

* Use Github Actions for CI
  (`PR#234 <https://github.com/fedora-infra/fedora-messaging/pull/234>`_)


Contributors
------------
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Aurélien Bompard
* Jeremy Cline
* Miro Hrončok
* Pierre-Yves Chibon


2.0.2 (2020-08-04)
==================

Bug Fixes
---------

* Set the QoS on the channel that is created for the consumer
  (`#223 <https://github.com/fedora-infra/fedora-messaging/issues/223>`_)


Documentation Improvements
--------------------------

* When running ``fedora-messaging consume``, the callback module should
  not contain a call to ``api.consume()`` or it will block.
  (`df4055f <https://github.com/fedora-infra/fedora-messaging/commit/df4055f>`_)

* Update the schema docs
  (`PR#219 <https://github.com/fedora-infra/fedora-messaging/pull/219>`_)

* Fix quickstart cert file links
  (`PR#222 <https://github.com/fedora-infra/fedora-messaging/pull/222>`_)

* Fix the docs about exceptions being wrapped by HaltConsumer
  (`#215 <https://github.com/fedora-infra/fedora-messaging/issues/215>`_)


Other Changes
-------------

* Only try to restart fm-consumer@ services every 60 seconds
  (`PR#214 <https://github.com/fedora-infra/fedora-messaging/pull/214>`_)


2.0.1 (2020-01-02)
==================

Bug Fixes
---------

* Fix handling of new connections after a publish timeout
  (`#212 <https://github.com/fedora-infra/fedora-messaging/issues/212>`_)


2.0.0 (2019-12-03)
==================

Dependency Changes
------------------

* Drop official Python 3.4 and 3.5 support
* Bump the pika requirement to 1.0.1+
* New dependency: `Crochet <https://crochet.readthedocs.io/en/stable/>`_


API Changes
-----------

* Move all APIs to use the Twisted-managed connection. There are a few minor
  changes here which slightly change the APIs:

  1. Publishing now raises a PublishTimeout when the timeout is reached
     (30 seconds by default).
  2. Previously, the Twisted consume API did not validate arguments like
     the synchronous version did, so it now raises a ValueError on invalid
     arguments instead of crashing in some undefined way.
  3. Calling publish from the Twisted reactor thread now raises an
     exception instead of blocking the reactor thread.
  4. Consumer exceptions are not re-raised as ``HaltConsumer`` exceptions
     anymore, the original exception bubbles up and has to be handled by the
     application.


Features
--------

* The ``fedora-messaging`` cli now has 2 new sub-commands: ``publish`` and ``record``.
  (`PR#43 <https://github.com/fedora-infra/fedora-messaging/pull/43>`_)
* Log the failure traceback on connection ready failures.


Bug Fixes
---------

* Fix an issue where reconnection to the server would fail.
  (`#208 <https://github.com/fedora-infra/fedora-messaging/issues/208>`_)
* Don't declare exchanges when consuming.
  (`#171 <https://github.com/fedora-infra/fedora-messaging/issues/171>`_)
* Fix Twisted legacy logging (it does not accept format parameters).
* Handle ConnectionLost errors in the v2 Factory.


Development Changes
-------------------

* Many Twisted-related tests were added.
* Include tests for sample schema package.
* Update the dumps and loads functions for a new message format.


Documentation Improvements
--------------------------

* Document that logging is only set up for consumers.
* Update the six intersphinx URL to fix the docs build.
* Add the "conf" and "DEFAULTS" variables to the API documentation.
* Update example config: extra properties, logging.
* Document a quick way to setup logging.
* Document the sent-at header in messages.
* Create a quick-start guide.
* Clarify queues are only deleted if unused.
* Wire-format: improve message properties documentation.
* Note the addition client properties in the config docs.


Contributors
------------

Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Aurélien Bompard
* Adam Williamson
* dvejmz
* Jeremy Cline
* Randy Barlow
* Shraddha Agrawal
* Sebastian Wojciechowski


1.7.2 (2019-08-02)
==================

Bug Fixes
---------

* Fix variable substitution in log messages.
  (`PR#200 <https://github.com/fedora-infra/fedora-messaging/pull/200>`_)
* Add MANIFEST.in and include tests for sample schema package.
  (`PR#197 <https://github.com/fedora-infra/fedora-messaging/pull/197>`_)


Documentation Improvements
--------------------------

* Document the sent-at header in messages.
  (`PR#199 <https://github.com/fedora-infra/fedora-messaging/pull/199>`_)
* Create a quick-start guide.
  (`PR#196 <https://github.com/fedora-infra/fedora-messaging/pull/196>`_)


Contributors
------------
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Adam Williamson
* Aurélien Bompard
* Jeremy Cline
* Shraddha Agrawal


v1.7.1 (2019-06-24)
===================

Bug Fixes
---------

* Don't declare exchanges when consuming using the synchronous
  :func:`fedora_messaging.api.consume` API, which was causing consuming to fail
  from the Fedora broker
  (`PR#191 <https://github.com/fedora-infra/fedora-messaging/pull/191>`_)

Contributors
------------
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Randy Barlow
* Aurélien Bompard
* Jeremy Cline
* Adam Williamson


Documentation Improvements
--------------------------

* Document some additional app properties and add a note about setting up logging
  in the fedora.toml and stg.fedora.toml configuration files
  (`PR#188 <https://github.com/fedora-infra/fedora-messaging/pull/188>`_)

* Document how to setup logging in the consuming snippets so any problems are
  logged to stdout
  (`PR#192 <https://github.com/fedora-infra/fedora-messaging/pull/192>`_)

* Document that logging is only set up for consumers
  (`#181 <https://github.com/fedora-infra/fedora-messaging/issues/181>`_)

* Document the :data:`fedora_messaging.config.conf` and
  :data:`fedora_messaging.config.DEFAULTS` variables in the API documentation
  (`#182 <https://github.com/fedora-infra/fedora-messaging/issues/182>`_)


v1.7.0 (2019-05-21)
===================

Features
--------

* "fedora-messaging consume" now accepts a "--callback-file" argument which will
  load a callback function from an arbitrary Python file. Previously, it was
  required that the callback be in the Python path
  (`#159 <https://github.com/fedora-infra/fedora-messaging/issues/159>`_).


Bug Fixes
---------

* Fix a bug where publishes that failed due to certain connection errors were not
  retried
  (`#175 <https://github.com/fedora-infra/fedora-messaging/issues/175>`_).

* Fix a bug where AMQP protocol errors did not reset the connection used for
  publishing messages. This would result in publishes always failing with a
  ConnectionError
  (`#178 <https://github.com/fedora-infra/fedora-messaging/pull/178>`_).


Documentation Improvements
--------------------------

* Document the ``body`` attribute on the ``Message`` class
  (`#164 <https://github.com/fedora-infra/fedora-messaging/issues/164>`_).

* Clearly document what properties message schema classes should override
  (`#166 <https://github.com/fedora-infra/fedora-messaging/issues/166>`_).

* Re-organize the consumer documentation to make the consuming API clearer
  (`#168 <https://github.com/fedora-infra/fedora-messaging/issues/168>`_).


Contributors
------------
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Randy Barlow
* Aurélien Bompard
* Jeremy Cline
* Dusty Mabe


v1.6.1 (2019-04-17)
===================

Bug Fixes
---------

* Fix a bug in publishing where if the broker closed the connection, the client
  would not properly dispose of the connection object and publishing would fail
  forever (`PR#157 <https://github.com/fedora-infra/fedora-messaging/pull/157>`_).

* Fix a bug in the :func:`fedora_messaging.api.twisted_consume` function where
  if the user did not have permissions to read from the specified queue which
  had already been declared, the Deferred that was returned never fired. It now
  errors back with a :class:`fedora_messaging.exceptions.PermissionException`
  (`PR#160 <https://github.com/fedora-infra/fedora-messaging/pull/160>`_).


Development Changes
-------------------

* Stop pinning pytest to 4.0 or less as the incompatibility with pytest-twisted
  has been resolved
  (`PR#158 <https://github.com/fedora-infra/fedora-messaging/pull/158>`_).


Other Changes
-------------

* Include commands to connect to the Fedora broker in the documentation
  (`PR#154 <https://github.com/fedora-infra/fedora-messaging/pull/154>`_).


Contributors
------------
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Aurélien Bompard
* Jeremy Cline


v1.6.0 (2019-04-04)
===================

Dependency Changes
------------------

* Twisted is no longer an optional dependency: fedora-messaging requires Twisted
  12.2 or greater.

Features
--------

* A new API, :func:`fedora_messaging.api.twisted_consume`, has been added to
  support consuming using the popular async framework Twisted. The
  fedora-messaging command-line interface has been switched to use this API. As
  a result, Twisted 12.2+ is now a dependency of fedora-messsaging. Users of
  this new API are not affected by `Issue #130
  <https://github.com/fedora-infra/fedora-messaging/issues/130>`_ (`PR#139
  <https://github.com/fedora-infra/fedora-messaging/pull/139>`_).

Bug Fixes
---------

* Only prepend the topic_prefix on outgoing messages. Previously, the topic
  prefix was incorrectly applied to incoming messages (`#143
  <https://github.com/fedora-infra/fedora-messaging/issues/143>`_).

Documentation
-------------

* Add a note to the tutorial on how to instal the library and RabbitMQ in
  containers (`PR#141
  <https://github.com/fedora-infra/fedora-messaging/pull/141>`_).

* Document how to access the Fedora message broker from outside the Fedora
  infrastructure VPN. Users of fedmsg can now migrate to fedora-messaging for
  consumers outside Fedora's infrastructure. Consult the new documentation at
  :ref:`fedora-broker` for details (`PR#149
  <https://github.com/fedora-infra/fedora-messaging/pull/149>`_).

Contributors
------------
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Aurélien Bompard
* Jeremy Cline
* Shraddha Agrawal


v1.5.0 (2019-02-28)
===================

Dependency Changes
------------------

* Replace the dependency on ``pytoml`` with ``toml``
  (`#132 <https://github.com/fedora-infra/fedora-messaging/issues/132>`_).


Features
--------

* Support passive declarations for locked-down brokers
  (`#136 <https://github.com/fedora-infra/fedora-messaging/issues/136>`_).


Bug Fixes
---------

* Fix a bug in the sample schema pachage
  (`#135 <https://github.com/fedora-infra/fedora-messaging/issues/135>`_).


Development Changes
-------------------

* Switch to Mergify v2
  (`#129 <https://github.com/fedora-infra/fedora-messaging/pull/129>`_).


Contributors
------------
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Aurélien Bompard
* Jeremy Cline
* Michal Konečný
* Shraddha Agrawal


v1.4.0 (2019-02-07)
===================

Features
--------

* The ``topic_prefix`` configuration value has been added to automatically add
  a prefix to the topic of all outgoing messages.
  (`#121 <https://github.com/fedora-infra/fedora-messaging/issues/121>`_)

* Support for Pika 0.13.
  (`#126 <https://github.com/fedora-infra/fedora-messaging/issues/126>`_)

* Add a systemd service file for consumers.


Development Changes
-------------------

* Use Bandit for security checking.


Contributors
------------
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Aurélien Bompard


v1.3.0 (2019-01-24)
===================

API Changes
-----------

* The :py:attr:`Message._body` attribute is renamed to ``body``, and is now part of the public API.
  (`PR#119 <https://github.com/fedora-infra/fedora-messaging/pull/119>`_)


Contributors
------------
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Aurélien Bompard
* Jeremy Cline


v1.2.0 (2019-01-21)
===================

Features
--------

* The :func:`fedora_messaging.api.consume` API now accepts a "queues" keyword
  which specifies the queues to declare and consume from, and the
  "fedora-messaging" CLI makes use of this
  (`PR#107 <https://github.com/fedora-infra/fedora-messaging/pull/107>`_)

* Utilities were added in the :py:mod:`schema_utils` module to help write the
  Python API of your message schemas
  (`PR#108 <https://github.com/fedora-infra/fedora-messaging/pull/108>`_)

* No long require "--exchange", "--queue-name", and "--routing-key" to all be
  specified when using "fedora-messaging consume". If one is not supplied, a
  default is chosen. These defaults are documented in the command's manual page
  (`PR#117 <https://github.com/fedora-infra/fedora-messaging/pull/117>`_)


Bug Fixes
---------

* Fix the "consumer" setting in config.toml.example to point to a real Python path
  (`PR#104 <https://github.com/fedora-infra/fedora-messaging/pull/104>`_)

* fedora-messaging consume now actually uses the --queue-name and --routing-key
  parameter provided to it, and --routing-key can now be specified multiple times
  as was documented
  (`PR#105 <https://github.com/fedora-infra/fedora-messaging/pull/105>`_)

* Fix the equality check on :class:`fedora_messaging.message.Message` objects to
  exclude the 'sent-at' header
  (`PR#109 <https://github.com/fedora-infra/fedora-messaging/pull/109>`_)

* Documentation for consumers indicated any callable object was acceptable to use
  as a callback as long as it accepted a single positional argument (the
  message). However, the implementation required that the callable be a function
  or a class, which it then instantiated. This has been fixed and you may now use
  any callable object, such as a method or an instance of a class that implements
  ``__call__``
  (`PR#110 <https://github.com/fedora-infra/fedora-messaging/pull/110>`_)

* Fix an issue where the fedora-messaging CLI would only log if a configuration
  file was explicitly supplied
  (`PR#113 <https://github.com/fedora-infra/fedora-messaging/pull/113>`_)


Contributors
------------
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Aurélien Bompard
* Jeremy Cline
* Sebastian Wojciechowski
* Tomas Tomecek


v1.1.0 (2018-11-13)
===================

Features
--------

* Initial work on a serialization format for
  :class:`fedora_messaging.message.Message` and APIs for loading and storing
  messages. This is intended to make it easy to record and replay messages for
  testing purposes.
  (`#84 <https://github.com/fedora-infra/fedora-messaging/issues/84>`_)

* Add a module, :mod:`fedora_messaging.testing`, to add useful test helpers.
  Check out the module documentation for details!
  (`#100 <https://github.com/fedora-infra/fedora-messaging/issues/100>`_)


Contributors
------------
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Jeremy Cline
* Sebastian Wojciechowski


v1.0.1 (2018-10-10)
===================

Bug Fixes
---------

* Fix a compatibility issue in Twisted between pika 0.12 and 1.0.
  (`#97 <https://github.com/fedora-infra/fedora-messaging/issues/97>`_)


v1.0.0 (2018-10-10)
===================

API Changes
-----------

* The unused ``exchange`` parameter from the PublisherSession was removed
  (`PR#56 <https://github.com/fedora-infra/fedora-messaging/pull/56>`_)

* The ``setupRead`` API in the Twisted protocol has been removed and replaced with
  ``consume`` and ``cancel`` APIs which allow for multiple consumers with multiple
  callbacks
  (`PR#72 <https://github.com/fedora-infra/fedora-messaging/pull/72>`_)

* The name of the entry point is now used to identify the message type
  (`PR#89 <https://github.com/fedora-infra/fedora-messaging/pull/89>`_)


Features
--------

* Ensure proper TLS client cert checking with ``service_identity``
  (`PR#51 <https://github.com/fedora-infra/fedora-messaging/pull/51>`_)

* Support Python 3.7
  (`PR#53 <https://github.com/fedora-infra/fedora-messaging/pull/53>`_)

* Compatibility with `Click <https://click.palletsprojects.com/>`_ 7.x
  (`PR#86 <https://github.com/fedora-infra/fedora-messaging/pull/86>`_)

* The complete set of valid severity levels is now available at
  :data:`fedora_messaging.api.SEVERITIES`
  (`PR#60 <https://github.com/fedora-infra/fedora-messaging/pull/60>`_)

* A ``queue`` attribute is present on received messages with the name of the
  queue it arrived on
  (`PR#65 <https://github.com/fedora-infra/fedora-messaging/pull/65>`_)

* The wire format of fedora-messaging is now documented
  (`PR#88 <https://github.com/fedora-infra/fedora-messaging/pull/88>`_)


Development Changes
-------------------

* Use `towncrier <https://github.com/hawkowl/towncrier>`_ to generate the release notes
  (`PR#67 <https://github.com/fedora-infra/fedora-messaging/pull/67>`_)

* Check that our dependencies have Free licenses
  (`PR#68 <https://github.com/fedora-infra/fedora-messaging/pull/68>`_)

* Test coverage is now at 97%.


Other Changes
-------------

* The library is available in Fedora as ``fedora-messaging``.


Contributors
------------
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Aurélien Bompard
* Jeremy Cline
* Michal Konečný
* Sebastian Wojciechowski


v1.0.0b1
========

API Changes
-----------

* :data:`fedora_messaging.message.Message.summary` is now a property rather than
  a method (`#25 <https://github.com/fedora-infra/fedora-messaging/pull/25>`_).

* The non-functional ``--amqp-url`` parameter has been removed from the CLI
  (`#49 <https://github.com/fedora-infra/fedora-messaging/pull/49>`_).


Features
--------

* Configuration parsing failures now produce point to the line and column of
  the parsing error (`#21
  <https://github.com/fedora-infra/fedora-messaging/pull/21>`_).

* :class:`fedora_messaging.message.Message` now come with a set of standard accessors
  (`#32 <https://github.com/fedora-infra/fedora-messaging/pull/32>`_).

* Consumers can now specify whether a message should be re-queued when halting
  (`#44 <https://github.com/fedora-infra/fedora-messaging/pull/44>`_).

* An example consumer that prints to standard output now ships with
  fedora-messaging. It can be used by running ``fedora-messaging consume
  --callback="fedora_messaging.example:printer"``
  (`#40 <https://github.com/fedora-infra/fedora-messaging/pull/40>`_).

* :class:`fedora_messaging.message.Message` now have a ``severity`` associated with them
  (`#48 <https://github.com/fedora-infra/fedora-messaging/pull/48>`_).

Bug Fixes
---------

* Fix an issue where invalid or missing configuration files resulted in a
  traceback rather than a formatted error message from the CLI (`#21
  <https://github.com/fedora-infra/fedora-messaging/pull/21>`_).

* Client authentication with x509 now works with both the synchronous API and
  the Twisted API (
  `#29 <https://github.com/fedora-infra/fedora-messaging/pull/29>`_,
  `#35 <https://github.com/fedora-infra/fedora-messaging/pull/35>`_).

* :func:`fedora_messaging.api.publish` no longer raises a
  :class:`pika.exceptions.ChannelClosed` exception. Instead, it raises a
  :class:`fedora_messaging.exceptions.ConnectionException`
  (`#31 <https://github.com/fedora-infra/fedora-messaging/pull/31>`_).

* :func:`fedora_messaging.api.consume` is now documented to raise a :class:`ValueError`
  when the callback isn't callable
  (`#47 <https://github.com/fedora-infra/fedora-messaging/pull/47>`_).


Development Features
--------------------

* The fedora-messaging code base is now compliant with the `Black
  <https://github.com/ambv/black>`_ Python formatter and this is enforced with
  continuous integration.

* Test coverage is moving up and to the right.


Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

* Aurélien Bompard
* Clement Verna
* Ken Dreyer
* Jeremy Cline
* Miroslav Suchý
* Patrick Uiterwijk
* Sebastian Wojciechowski


v1.0.0a1
========

The initial alpha release for fedora-messaging v1.0.0. The API is not expected
to change significantly between this release and the final v1.0.0 release, but
it may do so if serious flaws are discovered in it.
