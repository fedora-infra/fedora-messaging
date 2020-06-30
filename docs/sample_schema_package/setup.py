#!/usr/bin/env python
#
# Copyright (C) 2018  Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
import os

from setuptools import setup, find_packages


here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, "README")) as fd:
    README = fd.read()


setup(
    name="mailman_messages",
    version="1.0.0",
    description="A sample schema package for messages sent by mailman",
    long_description=README,
    url="https://github.com/fedora-infra/fedora-messaging/",
    # Possible options are at https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        "License :: OSI Approved :: GNU General Public License v2 or later (GPLv2+)",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    license="GPLv2+",
    maintainer="Fedora Infrastructure Team",
    maintainer_email="infrastructure@lists.fedoraproject.org",
    platforms=["Fedora", "GNU/Linux"],
    keywords="fedora",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=["fedora_messaging"],
    test_suite="mailman_messages.tests",
    entry_points={
        "fedora.messages": [
            "mailman.messageV1=mailman_messages.messages:MessageV1",
            "mailman.messageV2=mailman_messages.messages:MessageV2",
        ]
    },
)
