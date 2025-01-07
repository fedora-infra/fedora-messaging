#!/usr/bin/env python

# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

import os

from setuptools import find_packages, setup


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
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
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
