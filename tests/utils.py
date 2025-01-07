# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

from random import randrange

from twisted.internet import defer, error, protocol


@defer.inlineCallbacks
def get_available_port():
    from twisted.internet import reactor

    dummy_server = protocol.ServerFactory()
    while True:
        port = randrange(1025, 65534)  # noqa: S311
        try:
            twisted_port = reactor.listenTCP(port, dummy_server, interface="127.0.0.1")
        except error.CannotListenError:
            continue
        else:
            yield twisted_port.stopListening()
            defer.returnValue(port)
