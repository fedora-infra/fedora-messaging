<!--
SPDX-FileCopyrightText: 2024 Red Hat, Inc

SPDX-License-Identifier: GPL-2.0-or-later
-->

# Release Notes

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

This project uses [*towncrier*](https://towncrier.readthedocs.io/) and the changes for the upcoming release can be found in [the `news` directory](https://github.com/fedora-infra/fedora-messaging/tree/develop/news/).

<!-- towncrier release notes start -->

## 3.8.0 (2025-06-17)

### Features

- Add typing to the entire library, and check it with mypy and pyright (PR #477)
- Update the Fedora certificates ([8ce3f93](https://github.com/fedora-infra/fedora-messaging/commit/8ce3f93))
- Make the factory directly available as an attribute of the service ([7374232](https://github.com/fedora-infra/fedora-messaging/commit/7374232))
- Drop support for Python 3.8 and Pika < 1.0
- Support Python 3.13

### Development Changes

- Implement statistics as dataclasses (#4ac5fc5)


## 3.7.1 (2025-04-22)

### Documentation Improvements

- Use Reuse for the license text and license header (#383)
- Update installation.rst (#338)
- Add links to the documentation in the schema list if there's any ([d7f7394](https://github.com/fedora-infra/fedora-messaging/commit/d7f7394))
- Add schema packages:
  - webhook-to-fedora-messaging-messages ([e980001](https://github.com/fedora-infra/fedora-messaging/commit/e980001))
  - mailman3-fedmsg-plugin-schemas ([552f823](https://github.com/fedora-infra/fedora-messaging/commit/552f823))
  - journal-to-fedora-messaging-messages ([f89df00](https://github.com/fedora-infra/fedora-messaging/commit/f89df00))

### Development Changes

- Fix release github action ([59a3f36](https://github.com/fedora-infra/fedora-messaging/commit/59a3f36))

### Other Changes

- Add the new CAs to the trusted CA certs ([6facb16](https://github.com/fedora-infra/fedora-messaging/commit/6facb16))

### Contributors

Many thanks to the contributors of bug reports, pull requests, and pull request reviews for this release:

- Aurélien Bompard
- Olamide Ojo
- nwekealex65


## 3.7.0 (2025-01-03)

### Features

- Indicate which package a schema comes from when missing (#187)
- Add a new `reconsume` CLI command to fetch a message from datagrepper and replay it for the configured consumer (#359)
- Add an embedded HTTP server to monitor the service, see the "Monitoring" section in <project:./user-guide/consuming.rst> (#380)

### Documentation Improvements

- Add the `fedora-image-uploader-messages` schema package

### Development Changes

- Fix the Packit configuration
- Stop using Mergify
- Unit and integration tests improvements

### Other Changes

- Update dependencies

### Contributors

Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Akashdeep Dhar
- Aurélien Bompard
- Jeremy Cline
- Matej Focko


## 3.6.0 (2024-06-13)

### Features

- Add `api.twisted_publish()`, a function to publish messages from within a consumer callback (remember to use `twisted.internet.threads.blockingCallFromThread()` when calling it, or `reactor.callFromThread()`)
- Add a `summary` property to `ValidationError` exceptions

### Bug Fixes

- Add missing dependency on `requests` (PR #365)
- Don't respect the configured `topic_prefix` when replaying messages

### Documentation Improvements

- Add the maubot-fedora-messages schema package (4b3c8d0)

### Development Changes

- Integrate diff-cover in the testing workflow
- Switch to ruff instead of flake8 + isort + bandit

### Other Changes

- Drop the dependency on pytz
- Get rid of `pkg_resources`

### Contributors

Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Vidit Maheshwari
- Aurélien Bompard
- Matej Focko


## 3.5.0 (2024-03-20)

### Features

- Add a replay command (#332)
- Add support Python 3.11 and 3.12, drop support for Python 3.6 and 3.7
- Better protection against invalid bodies breaking the headers generation and the instanciation of a message
- Testing framework: make the sent messages available in the context manager

### Documentation Improvements

- Add SECURITY.md for project security policy
  (PR #314)
- Add fedora-messaging-git-hook-messages to the known schema packages

### Development Changes

- Make the tests use the pytest fixtures and assert system
  ([961b82d](https://github.com/fedora-infra/fedora-messaging/commit/961b82d))
- Make fedora-messaging use poetry (#294)
- Add some generic pre-commit checks
- Don't distribute the tests in the wheel

### Contributors

Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Aurélien Bompard
- Thibaut Batale
- Khaled Achech
- Lenka Segura
- Ryan Lerch

## 3.4.1 (2023-05-26)

### Bug Fixes

- Fix CI
  ([0f2e39c](https://github.com/fedora-infra/fedora-messaging/commit/0f2e39c))


## 3.4.0 (2023-05-26)

### Features

- Mirror the message priority in the headers
  ([eba336b](https://github.com/fedora-infra/fedora-messaging/commit/eba336b))


## 3.3.0 (2023-03-31)

### Features

- Add support for asyncio-based callbacks in the consumer. As a consequence,
  the Twisted reactor used by the CLI is now `asyncio`.
  (PR #282)

### Documentation Improvements

- Improve documentation layout, and add some documentation on the message
  schemas.
  ([1fa8998](https://github.com/fedora-infra/fedora-messaging/commit/1fa8998))
- Add koji-fedoramessaging-messages to the list of known schemas.
  ([ef12fa2](https://github.com/fedora-infra/fedora-messaging/commit/ef12fa2))

### Development Changes

- Update pre-commit linters.
  ([0efdde1](https://github.com/fedora-infra/fedora-messaging/commit/0efdde1))


## 3.2.0 (2022-10-17)

### Features

- Add a priority property to messages, and a default priority in the
  configuration
  (PR #275)
- Add a message schema attribute and some documentation to help deprecate and
  upgrade message schemas
  (#227)

### Other Changes

- Use tomllib from the standard library on Python 3.11 and above,
  fallback to tomli otherwise.
  (PR #274)

### Contributors

Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Akashdeep Dhar
- Aurélien Bompard
- Erol Keskin
- Miro Hrončok
- Stephen Coady


## 3.1.0 (2022-09-13)

### Features

- Add the `app_name` and `agent_name` properties to message schemas
  (PR #272)
- Added "groups" property to message schemas. This property can be used if an
  event affects a list of groups.
  (#252)


## 3.0.2 (2022-05-19)

### Development Changes

- Fix CI in Github actions
  ([6257100](https://github.com/fedora-infra/fedora-messaging/commit/6257100))
- Update pre-commit checkers
  ([1d35a5d](https://github.com/fedora-infra/fedora-messaging/commit/1d35a5d))
- Fix Packit configuration
  ([d2ea85f](https://github.com/fedora-infra/fedora-messaging/commit/d2ea85f))

### Contributors
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Akashdeep Dhar
- Aurélien Bompard


## 3.0.1 (2022-05-12)

### Development Changes

- Add packit configuration allowing us to have automatic downstream RPM builds
  (#259)
- Don't build universal wheels since we don't run on Python 2 anymore
  ([e8c5f4c](https://github.com/fedora-infra/fedora-messaging/commit/e8c5f4c))


### Documentation Improvements

- Add some schema packages to the docs
  ([03e7f42](https://github.com/fedora-infra/fedora-messaging/commit/03e7f42))
- Change the example email addresses
  ([1555742](https://github.com/fedora-infra/fedora-messaging/commit/1555742))


### Contributors
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Akashdeep Dhar
- Aurélien Bompard


## 3.0.0 (2021-12-14)

### API Changes

- Queues created by the CLI are now non-durable, auto-deleted and exclusive, as
  server-named queues are.
  (PR #239)

- It is no longer necessary to declare a queue in the configuration file: a
  server-named queue will be created. Configured bindings which do not specify
  a queue name will be applied to the server-named queue.
  (PR #239)

- Drop support for Python 2
  (PR #246)

- Drop the Twisted classes that had been flagged as deprecated.
  Drop the deprecated `Message._body` property.
  Refactor the consuming code into the `Consumer` class.
  (PR #249)


### Features

- Support anonymous (server-named) queues.
  (PR #239)

- Support Python 3.10
  (PR #250)

- Raise `PublishForbidden` exception immediately if publishing to [virtual host](https://www.rabbitmq.com/access-control.html>) is denied rather than waiting until timeout occurs.
  (#203)


### Bug Fixes

- Fixed validation exception of queue field on serialized schemas.
  (#240)


### Documentation Improvements

- Improve release notes process documentation.
  (PR #238)

- Build a list of available topics in the documentation from known schema packages
  (PR #242)


### Development Changes

- Start using pre-commit for linters and formatters
  ([732c7fb](https://github.com/fedora-infra/fedora-messaging/commit/732c7fb))


### Contributors
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Aurélien Bompard
- David Jimenez
- Michal Konečný
- Onur Ozkan


## 2.1.0 (2021-05-12)

### Features

- Improve the testing module to check message topics and bodies separately,
  and to use the rewritten assert that pytest provides
  (PR #230)

- Handle `topic authorization <https://www.rabbitmq.com/access-control.html#topic-authorisation>`_
  by raising a `PublishForbidden` exception instead of being stuck in a retry loop
  (PR #235)

- Test on Python 3.8 and 3.9
  (PR #237)


### Bug Fixes

- Require setuptools, as `pkg_resources` is used
  (PR #233)


### Development Changes

- Update test fixture keys to 4096 bits
  (PR #232)

- Use Github Actions for CI
  (PR #234)


### Contributors
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Aurélien Bompard
- Jeremy Cline
- Miro Hrončok
- Pierre-Yves Chibon


## 2.0.2 (2020-08-04)

### Bug Fixes

- Set the QoS on the channel that is created for the consumer
  (#223)


### Documentation Improvements

- When running `fedora-messaging consume`, the callback module should
  not contain a call to `api.consume()` or it will block.
  ([df4055f](https://github.com/fedora-infra/fedora-messaging/commit/df4055f))

- Update the schema docs
  (PR #219)

- Fix quickstart cert file links
  (PR #222)

- Fix the docs about exceptions being wrapped by HaltConsumer
  (#215)


### Other Changes

- Only try to restart fm-consumer@ services every 60 seconds
  (PR #214)


## 2.0.1 (2020-01-02)

### Bug Fixes

- Fix handling of new connections after a publish timeout
  (#212)


## 2.0.0 (2019-12-03)

### Dependency Changes

- Drop official Python 3.4 and 3.5 support
- Bump the pika requirement to 1.0.1+
- New dependency: [Crochet](https://crochet.readthedocs.io/en/stable/)


### API Changes

- Move all APIs to use the Twisted-managed connection. There are a few minor
  changes here which slightly change the APIs:

  1. Publishing now raises a PublishTimeout when the timeout is reached
     (30 seconds by default).
  2. Previously, the Twisted consume API did not validate arguments like
     the synchronous version did, so it now raises a ValueError on invalid
     arguments instead of crashing in some undefined way.
  3. Calling publish from the Twisted reactor thread now raises an
     exception instead of blocking the reactor thread.
  4. Consumer exceptions are not re-raised as `HaltConsumer` exceptions
     anymore, the original exception bubbles up and has to be handled by the
     application.


### Features

- The `fedora-messaging` cli now has 2 new sub-commands: `publish` and `record`.
  (PR #43)
- Log the failure traceback on connection ready failures.


### Bug Fixes

- Fix an issue where reconnection to the server would fail.
  (#208)
- Don't declare exchanges when consuming.
  (#171)
- Fix Twisted legacy logging (it does not accept format parameters).
- Handle ConnectionLost errors in the v2 Factory.


### Development Changes

- Many Twisted-related tests were added.
- Include tests for sample schema package.
- Update the dumps and loads functions for a new message format.


### Documentation Improvements

- Document that logging is only set up for consumers.
- Update the six intersphinx URL to fix the docs build.
- Add the "conf" and "DEFAULTS" variables to the API documentation.
- Update example config: extra properties, logging.
- Document a quick way to setup logging.
- Document the sent-at header in messages.
- Create a quick-start guide.
- Clarify queues are only deleted if unused.
- Wire-format: improve message properties documentation.
- Note the addition client properties in the config docs.


### Contributors

Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Aurélien Bompard
- Adam Williamson
- dvejmz
- Jeremy Cline
- Randy Barlow
- Shraddha Agrawal
- Sebastian Wojciechowski


## 1.7.2 (2019-08-02)

### Bug Fixes

- Fix variable substitution in log messages.
  (PR #200)
- Add MANIFEST.in and include tests for sample schema package.
  (PR #197)


### Documentation Improvements

- Document the sent-at header in messages.
  (PR #199)
- Create a quick-start guide.
  (PR #196)


### Contributors
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Adam Williamson
- Aurélien Bompard
- Jeremy Cline
- Shraddha Agrawal


## v1.7.1 (2019-06-24)

### Bug Fixes

- Don't declare exchanges when consuming using the synchronous
  `fedora_messaging.api.consume()` API, which was causing consuming to fail
  from the Fedora broker
  (PR #191)

### Contributors
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Randy Barlow
- Aurélien Bompard
- Jeremy Cline
- Adam Williamson


### Documentation Improvements

- Document some additional app properties and add a note about setting up logging
  in the fedora.toml and stg.fedora.toml configuration files
  (PR #188)

- Document how to setup logging in the consuming snippets so any problems are
  logged to stdout
  (PR #192)

- Document that logging is only set up for consumers
  (#181)

- Document the `fedora_messaging.config.conf` and
  `fedora_messaging.config.DEFAULTS` variables in the API documentation
  (#182)


## v1.7.0 (2019-05-21)

### Features

- "fedora-messaging consume" now accepts a "--callback-file" argument which will
  load a callback function from an arbitrary Python file. Previously, it was
  required that the callback be in the Python path
  (#159).


### Bug Fixes

- Fix a bug where publishes that failed due to certain connection errors were not
  retried
  (#175).

- Fix a bug where AMQP protocol errors did not reset the connection used for
  publishing messages. This would result in publishes always failing with a
  ConnectionError
  (#178).


### Documentation Improvements

- Document the `body` attribute on the `Message` class
  (#164).

- Clearly document what properties message schema classes should override
  (#166).

- Re-organize the consumer documentation to make the consuming API clearer
  (#168).


### Contributors
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Randy Barlow
- Aurélien Bompard
- Jeremy Cline
- Dusty Mabe


## v1.6.1 (2019-04-17)

### Bug Fixes

- Fix a bug in publishing where if the broker closed the connection, the client
  would not properly dispose of the connection object and publishing would fail
  forever (PR #157).

- Fix a bug in the `fedora_messaging.api.twisted_consume()` function where
  if the user did not have permissions to read from the specified queue which
  had already been declared, the Deferred that was returned never fired. It now
  errors back with a `fedora_messaging.exceptions.PermissionException`
  (PR #160).


### Development Changes

- Stop pinning pytest to 4.0 or less as the incompatibility with pytest-twisted
  has been resolved
  (PR #158).


### Other Changes

- Include commands to connect to the Fedora broker in the documentation
  (PR #154).


### Contributors
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Aurélien Bompard
- Jeremy Cline


## v1.6.0 (2019-04-04)

### Dependency Changes

- Twisted is no longer an optional dependency: fedora-messaging requires Twisted
  12.2 or greater.

### Features

- A new API, `fedora_messaging.api.twisted_consume()`, has been added to
  support consuming using the popular async framework Twisted. The
  fedora-messaging command-line interface has been switched to use this API. As
  a result, Twisted 12.2+ is now a dependency of fedora-messsaging. Users of
  this new API are not affected by #130 (PR #139).

### Bug Fixes

- Only prepend the `topic_prefix` on outgoing messages. Previously, the topic
  prefix was incorrectly applied to incoming messages (#143).

### Documentation

- Add a note to the tutorial on how to instal the library and RabbitMQ in
  containers (PR #141).

- Document how to access the Fedora message broker from outside the Fedora
  infrastructure VPN. Users of fedmsg can now migrate to fedora-messaging for
  consumers outside Fedora's infrastructure. Consult the new documentation at
  `fedora-broker` for details (PR #149).

### Contributors
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Aurélien Bompard
- Jeremy Cline
- Shraddha Agrawal


## v1.5.0 (2019-02-28)

### Dependency Changes

- Replace the dependency on `pytoml` with `toml`
  (#132).


### Features

- Support passive declarations for locked-down brokers
  (#136).


### Bug Fixes

- Fix a bug in the sample schema pachage
  (#135).


### Development Changes

- Switch to Mergify v2
  (#129).


### Contributors
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Aurélien Bompard
- Jeremy Cline
- Michal Konečný
- Shraddha Agrawal


## v1.4.0 (2019-02-07)

### Features

- The `topic_prefix` configuration value has been added to automatically add
  a prefix to the topic of all outgoing messages.
  (#121)

- Support for Pika 0.13.
  (#126)

- Add a systemd service file for consumers.


### Development Changes

- Use Bandit for security checking.


### Contributors
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Aurélien Bompard


## v1.3.0 (2019-01-24)

### API Changes

- The `Message._body` attribute is renamed to `body`, and is now part of the public API.
  (PR #119)


### Contributors
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Aurélien Bompard
- Jeremy Cline


## v1.2.0 (2019-01-21)

### Features

- The `fedora_messaging.api.consume()` API now accepts a "queues" keyword
  which specifies the queues to declare and consume from, and the
  "fedora-messaging" CLI makes use of this
  (PR #107)

- Utilities were added in the `schema_utils` module to help write the
  Python API of your message schemas
  (PR #108)

- No long require "--exchange", "--queue-name", and "--routing-key" to all be
  specified when using "fedora-messaging consume". If one is not supplied, a
  default is chosen. These defaults are documented in the command's manual page
  (PR #117)


### Bug Fixes

- Fix the "consumer" setting in config.toml.example to point to a real Python path
  (PR #104)

- fedora-messaging consume now actually uses the --queue-name and --routing-key
  parameter provided to it, and --routing-key can now be specified multiple times
  as was documented
  (PR #105)

- Fix the equality check on `fedora_messaging.message.Message` objects to
  exclude the 'sent-at' header
  (PR #109)

- Documentation for consumers indicated any callable object was acceptable to use
  as a callback as long as it accepted a single positional argument (the
  message). However, the implementation required that the callable be a function
  or a class, which it then instantiated. This has been fixed and you may now use
  any callable object, such as a method or an instance of a class that implements
  `__call__()`
  (PR #110)

- Fix an issue where the fedora-messaging CLI would only log if a configuration
  file was explicitly supplied
  (PR #113)


### Contributors
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Aurélien Bompard
- Jeremy Cline
- Sebastian Wojciechowski
- Tomas Tomecek


## v1.1.0 (2018-11-13)

### Features

- Initial work on a serialization format for
  `fedora_messaging.message.Message` and APIs for loading and storing
  messages. This is intended to make it easy to record and replay messages for
  testing purposes.
  (#84)

- Add a module, `fedora_messaging.testing`, to add useful test helpers.
  Check out the module documentation for details!
  (#100)


### Contributors
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Jeremy Cline
- Sebastian Wojciechowski


## v1.0.1 (2018-10-10)

### Bug Fixes

- Fix a compatibility issue in Twisted between pika 0.12 and 1.0.
  (#97)


## v1.0.0 (2018-10-10)

### API Changes

- The unused `exchange` parameter from the PublisherSession was removed
  (PR #56)

- The `setupRead` API in the Twisted protocol has been removed and replaced with
  `consume` and `cancel` APIs which allow for multiple consumers with multiple
  callbacks
  (PR #72)

- The name of the entry point is now used to identify the message type
  (PR #89)


### Features

- Ensure proper TLS client cert checking with `service_identity`
  (PR #51)

- Support Python 3.7
  (PR #53)

- Compatibility with [Click](https://click.palletsprojects.com/) 7.x
  (PR #86)

- The complete set of valid severity levels is now available at
  `fedora_messaging.api.SEVERITIES`
  (PR #60)

- A `queue` attribute is present on received messages with the name of the
  queue it arrived on
  (PR #65)

- The wire format of fedora-messaging is now documented
  (PR #88)


### Development Changes

- Use [towncrier](https://github.com/hawkowl/towncrier) to generate the release notes
  (PR #67)

- Check that our dependencies have Free licenses
  (PR #68)

- Test coverage is now at 97%.


### Other Changes

- The library is available in Fedora as `fedora-messaging`.


### Contributors
Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Aurélien Bompard
- Jeremy Cline
- Michal Konečný
- Sebastian Wojciechowski


## v1.0.0b1

### API Changes

- `fedora_messaging.message.Message.summary` is now a property rather than
  a method (#25).

- The non-functional `--amqp-url` parameter has been removed from the CLI
  (#49).


### Features

- Configuration parsing failures now produce point to the line and column of
  the parsing error (#21).

- `fedora_messaging.message.Message` now come with a set of standard accessors
  (#32).

- Consumers can now specify whether a message should be re-queued when halting
  (#44).

- An example consumer that prints to standard output now ships with
  fedora-messaging. It can be used by running `fedora-messaging consume
  --callback="fedora_messaging.example:printer"`
  (#40).

- `fedora_messaging.message.Message` now have a `severity` associated with them
  (#48).

### Bug Fixes

- Fix an issue where invalid or missing configuration files resulted in a
  traceback rather than a formatted error message from the CLI (#21).

- Client authentication with x509 now works with both the synchronous API and
  the Twisted API (#29, #35).

- `fedora_messaging.api.publish()` no longer raises a
  `pika.exceptions.ChannelClosed` exception. Instead, it raises a
  `fedora_messaging.exceptions.ConnectionException`
  (#31).

- `fedora_messaging.api.consume()` is now documented to raise a `ValueError`
  when the callback isn't callable
  (#47).


### Development Features

- The fedora-messaging code base is now compliant with the [Black](https://github.com/ambv/black)
  Python formatter and this is enforced with continuous integration.

- Test coverage is moving up and to the right.


Many thanks to the contributors of bug reports, pull requests, and pull request
reviews for this release:

- Aurélien Bompard
- Clement Verna
- Ken Dreyer
- Jeremy Cline
- Miroslav Suchý
- Patrick Uiterwijk
- Sebastian Wojciechowski


## v1.0.0a1

The initial alpha release for fedora-messaging v1.0.0. The API is not expected
to change significantly between this release and the final v1.0.0 release, but
it may do so if serious flaws are discovered in it.
