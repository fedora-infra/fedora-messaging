# This file is part of fedora_messaging.
# Copyright (C) 2018 Red Hat, Inc.
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
