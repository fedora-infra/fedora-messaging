============
Contributing
============

Thanks for considering contributing to fedora-messaging, we really appreciate it!

Quickstart:

1. Look for an `existing issue
   <https://github.com/fedora-infra/fedora-messaging/issues>`_ about the bug or
   feature you're interested in. If you can't find an existing issue, create a
   `new one <https://github.com/fedora-infra/fedora-messaging/issues/new>`_.

2. Fork the `repository on GitHub
   <https://github.com/fedora-infra/fedora-messaging>`_.

3. Fix the bug or add the feature, and then write one or more tests which show
   the bug is fixed or the feature works.

4. Submit a pull request and wait for a maintainer to review it.

More detailed guidelines to help ensure your submission goes smoothly are
below.

.. note:: If you do not wish to use GitHub, please send patches to
          infrastructure@lists.fedoraproject.org.

Guidelines
==========

Python Support
--------------
fedora-messaging supports Python 2.7 and Python 3.4 or greater. This is
automatically enforced by the continuous integration (CI) suite.


Code Style
----------
We follow the `PEP8 <https://www.python.org/dev/peps/pep-0008/>`_ style guide
for Python. This is automatically enforced by the CI suite.

We are using `Black <https://github.com/ambv/black>` to automatically format
the source code. It is also checked in CI. The Black webpage contains
instructions to configure your editor to run it on the files you edit.


Tests
-----
The test suites can be run using `tox <http://tox.readthedocs.io/>`_ by simply
running ``tox`` from the repository root. All code must have test coverage or
be explicitly marked as not covered using the ``# no-qa`` comment. This should
only be done if there is a good reason to not write tests.

Your pull request should contain tests for your new feature or bug fix. If
you're not certain how to write tests, we will be happy to help you.


Release notes
-------------
To add entries to the release notes, create a file in the ``news`` directory
with the ``source.type`` name format, where ``type`` is one of:

* ``feature``: for new features
* ``bug``: for bug fixes
* ``api``: for API changes
* ``dev``: for development-related changes
* ``author``: for contributor names
* ``other``: for other changes

And where the ``source`` part of the filename is:

* ``42`` when the change is described in issue ``42``
* ``PR42`` when the change has been implemented in pull request ``42``, and
  there is no associated issue
* ``Cabcdef`` when the change has been implemented in changeset ``abcdef``, and
  there is no associated issue or pull request.
* ``username`` for contributors (``author`` extention). It should be the
  username part of their commits' email address.

A preview of the release notes can be generated with ``towncrier --draft``.


Licensing
---------

Your commit messages must include a Signed-off-by tag with your name and e-mail
address, indicating that you agree to the `Developer Certificate of Origin
<https://developercertificate.org/>`_ version 1.1::

	Developer Certificate of Origin
	Version 1.1

	Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
	1 Letterman Drive
	Suite D4700
	San Francisco, CA, 94129

	Everyone is permitted to copy and distribute verbatim copies of this
	license document, but changing it is not allowed.


	Developer's Certificate of Origin 1.1

	By making a contribution to this project, I certify that:

	(a) The contribution was created in whole or in part by me and I
	    have the right to submit it under the open source license
	    indicated in the file; or

	(b) The contribution is based upon previous work that, to the best
	    of my knowledge, is covered under an appropriate open source
	    license and I have the right under that license to submit that
	    work with modifications, whether created in whole or in part
	    by me, under the same open source license (unless I am
	    permitted to submit under a different license), as indicated
	    in the file; or

	(c) The contribution was provided directly to me by some other
	    person who certified (a), (b) or (c) and I have not modified
	    it.

	(d) I understand and agree that this project and the contribution
	    are public and that a record of the contribution (including all
	    personal information I submit with it, including my sign-off) is
	    maintained indefinitely and may be redistributed consistent with
	    this project or the open source license(s) involved.

Use ``git commit -s`` to add the Signed-off-by tag.


Releasing
---------

When cutting a new release, follow these steps:

* update the version in ``fedora_messaging/__init__.py``
* generate the changelog by running ``towncrier``
* change the ``Development Status`` classifier in ``setup.py`` if necessary
* commit the changes
* tag the commit
* push to GitHub
* generate a tarball and push to PyPI with the commands:

::

    python setup.py sdist bdist_wheel
    twine upload -s dist/*
