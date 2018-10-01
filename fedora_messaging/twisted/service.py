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
"""
Twisted Service to start and stop the Fedora Messaging Twisted Factory.

This Service makes it easier to build a Twisted application that embeds a
Fedora Messaging component. See the ``verify_missing`` service in
fedmsg-migration-tools for a use case.

See https://twistedmatrix.com/documents/current/core/howto/application.html
"""

from __future__ import absolute_import, unicode_literals

import pika
from twisted.application import service
from twisted.application.internet import TCPClient, SSLClient
from twisted.internet import ssl

from .. import config
from .._session import _configure_tls_parameters
from .factory import FedoraMessagingFactory


class FedoraMessagingService(service.MultiService):
    """
    A Twisted service to connect to the Fedora Messaging broker.

    Args:
        on_message (callable|None): Callback that will be passed each
            incoming messages. If None, no message consuming is setup.
        amqp_url (str): URL to use for the AMQP server.
        bindings (list(dict)): A list of dictionaries that define queue
            bindings to exchanges. This parameter can be used to override the
            bindings declared in the configuration. See the configuration
            documentation for details. Each dictionary should contain the
            following keys: "exchange", "exchange_type", "queue_name",
            "routing_key", "queue_auto_delete", "queue_arguments", and
            "binding_arguments" (which are passed to
            :meth:`pika.channel.Channel.queue_declare` and
            :meth:`pika.channel.Channel.queue_bind` respectively as the
            ``arguments`` key.
    """

    name = "fedora-messaging"
    factoryClass = FedoraMessagingFactory

    def __init__(self, amqp_url=None, exchanges=None, queues=None, bindings=None):
        """Initialize the service."""
        service.MultiService.__init__(self)
        amqp_url = amqp_url or config.conf["amqp_url"]
        self._parameters = pika.URLParameters(amqp_url)
        if amqp_url.startswith("amqps"):
            _configure_tls_parameters(self._parameters)
        if self._parameters.client_properties is None:
            self._parameters.client_properties = config.conf["client_properties"]
        self._exchanges = exchanges or []
        self._queues = queues or []
        self._bindings = bindings or []

    def startService(self):
        self.connect()
        service.MultiService.startService(self)

    def stopService(self):
        factory = self.getFactory()
        if not factory:
            return
        factory.stopTrying()
        service.MultiService.stopService(self)

    def getFactory(self):
        if self.services:
            return self.services[0].factory
        return None

    def connect(self):
        factory = self.factoryClass(
            self._parameters,
            exchanges=self._exchanges,
            queues=self._queues,
            bindings=self._bindings,
        )
        if self._parameters.ssl_options:
            serv = SSLClient(
                host=self._parameters.host,
                port=self._parameters.port,
                factory=factory,
                contextFactory=_ssl_context_factory(self._parameters),
            )
        else:
            serv = TCPClient(
                host=self._parameters.host, port=self._parameters.port, factory=factory
            )
        serv.factory = factory
        name = "{}{}:{}".format(
            "ssl:" if self._parameters.ssl_options else "",
            self._parameters.host,
            self._parameters.port,
        )
        serv.setName(name)
        serv.setServiceParent(self)


def _ssl_context_factory(parameters):
    """
    Produce a Twisted SSL context object from a pika connection parameter object.
    This is necessary as Twisted manages the connection, not Pika.

    Args:
        parameters (pika.ConnectionParameters): The connection parameters built
            from the fedora_messaging configuration.
    """
    client_cert = None
    key = config.conf["tls"]["keyfile"]
    cert = config.conf["tls"]["certfile"]
    with open(config.conf["tls"]["ca_cert"]) as fd:
        ca_cert = ssl.Certificate.loadPEM(fd.read())
    if key and cert:
        # Note that _configure_tls_parameters sets the auth mode to EXTERNAL
        # if both key and cert are defined, so we don't need to do that here.
        with open(key) as fd:
            client_keypair = fd.read()
        with open(cert) as fd:
            client_keypair += fd.read()
        client_cert = ssl.PrivateCertificate.loadPEM(client_keypair)

    context_factory = ssl.optionsForClientTLS(
        parameters.host,
        trustRoot=ca_cert,
        clientCertificate=client_cert,
        extraCertificateOptions={"raiseMinimumTo": ssl.TLSVersion.TLSv1_2},
    )

    return context_factory
