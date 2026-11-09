# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

from collections.abc import Generator
from random import randrange
from typing import cast

from twisted.internet import defer, error, interfaces, protocol

from fedora_messaging.config import COMPAT_TWISTED_USE_RETURNVALUE


@defer.inlineCallbacks
def get_available_port() -> Generator[defer.Deferred[int]]:
    from twisted.internet import reactor

    dummy_server = protocol.ServerFactory()
    while True:
        port = randrange(1025, 65534)  # noqa: S311
        try:
            twisted_port = cast(interfaces.IReactorCore, reactor).listenTCP(
                port, dummy_server, interface="127.0.0.1"
            )
        except error.CannotListenError:
            continue
        else:
            yield twisted_port.stopListening()
            if COMPAT_TWISTED_USE_RETURNVALUE:
                defer.returnValue(port)
            else:
                return port
