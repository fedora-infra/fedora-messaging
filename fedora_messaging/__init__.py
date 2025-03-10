# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

import importlib.metadata


try:
    import pytest

    pytest.register_assert_rewrite("fedora_messaging.testing")
except ImportError:
    pass


__version__ = importlib.metadata.version("fedora-messaging")
