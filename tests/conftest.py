# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

import os

import crochet
import pytest

from .utils import get_available_port


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
