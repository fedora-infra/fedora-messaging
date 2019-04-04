=============
Release Notes
=============

.. towncrier release notes start

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
