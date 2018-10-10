=============
Release Notes
=============

.. towncrier release notes start

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
