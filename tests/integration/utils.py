# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

import os

from twisted.internet import reactor, task


def sleep(delay):
    # Returns a deferred that calls do-nothing function
    # after `delay` seconds
    return task.deferLater(reactor, delay, lambda: None)


RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "localhost")
