# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

import os
from typing import cast

from twisted.internet import defer, interfaces, reactor, task


def sleep(delay: int) -> defer.Deferred[None]:
    # Returns a deferred that calls do-nothing function
    # after `delay` seconds
    return task.deferLater(cast(interfaces.IReactorCore, reactor), delay, lambda: None)


RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
