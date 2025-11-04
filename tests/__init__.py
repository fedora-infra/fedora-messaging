# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

import asyncio

from twisted.internet import asyncioreactor


asyncioreactor.install(asyncio.new_event_loop())
