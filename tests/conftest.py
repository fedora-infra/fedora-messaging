# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

import asyncio
import os

import crochet
import pytest

from .utils import get_available_port


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config):
    """
    Short-term fix to support Python 3.14.

    Twisted 25.5 does not support Python 3.14, and at the time of this writing neither
    does pytest-twisted. This is because Python removed a number of APIs in asyncio. This
    is a temporary workaround because they plan to remove these APIs too.
    """
    asyncio.set_event_loop(asyncio.new_event_loop())


@pytest.fixture(autouse=True, scope="session")
def crochet_no_setup():
    crochet.no_setup()


@pytest.fixture
def fixtures_dir():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "fixtures/"))


@pytest.fixture
def available_port():
    try:
        import pytest_twisted
    except ImportError:
        pytest.skip("pytest-twisted is missing, skipping tests", allow_module_level=True)

    return pytest_twisted.blockon(get_available_port())
