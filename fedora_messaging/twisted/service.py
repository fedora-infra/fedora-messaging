# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

"""
Twisted Service to start and stop the Fedora Messaging Twisted Factory.

This Service makes it easier to build a Twisted application that embeds a
Fedora Messaging component. See the ``verify_missing`` service in
fedmsg-migration-tools for a use case.

See https://twistedmatrix.com/documents/current/core/howto/application.html
"""


import logging
import ssl
from collections.abc import Generator
from typing import Any, cast, Optional, TYPE_CHECKING, Union

import pika
from pika import SSLOptions
from twisted.application import service
from twisted.application.internet import SSLClient, TCPClient
from twisted.internet import defer
from twisted.internet import ssl as twisted_ssl

from .. import config, exceptions
from .factory import FedoraMessagingFactoryV2


if TYPE_CHECKING:
    from twisted.internet._sslverify import ClientTLSOptions

    from .stats import FactoryStatistics


_log = logging.getLogger(__name__)


class FedoraMessagingServiceV2(service.MultiService):
    """
    A Twisted service to connect to the Fedora Messaging broker.

    Args:
        amqp_url: URL to use for the AMQP server.
        publish_confirms: If true, use the RabbitMQ publisher confirms
            AMQP extension.
    """

    # TODO: bug in twisted: this can be a string (currently untyped and defaults to None)
    name: str = "fedora-messaging-servicev2"  # type: ignore

    def __init__(self, amqp_url: Optional[str] = None, publish_confirms: bool = True):
        """Initialize the service."""
        service.MultiService.__init__(self)
        amqp_url = amqp_url or cast(str, config.conf["amqp_url"])
        self._parameters = pika.URLParameters(amqp_url)
        self._confirms = publish_confirms
        if amqp_url.startswith("amqps"):
            _configure_tls_parameters(self._parameters)
        if self._parameters.client_properties is None:
            self._parameters.client_properties = config.conf["client_properties"]

        self.factory = FedoraMessagingFactoryV2(self._parameters, self._confirms)
        self._service: Union[SSLClient, TCPClient]
        if self._parameters.ssl_options:
            self._service = SSLClient(
                host=self._parameters.host,
                port=self._parameters.port,
                factory=self.factory,
                contextFactory=_ssl_context_factory(self._parameters),
            )
        else:
            self._service = TCPClient(
                host=self._parameters.host, port=self._parameters.port, factory=self.factory
            )
        # There's self.factory now, but keep this for compatibility
        self._service.factory = self.factory  # type: ignore
        name = "{}{}:{}".format(
            "ssl:" if self._parameters.ssl_options else "",
            self._parameters.host,
            self._parameters.port,
        )
        self._service.setName(name)
        self._service.setServiceParent(self)

    @defer.inlineCallbacks
    def stopService(  # pyright: ignore [reportIncompatibleMethodOverride]
        self,
    ) -> Generator[defer.Deferred[Any], None, defer.Deferred[Any]]:
        """
        Gracefully stop the service.

        Returns:
            a Deferred which is triggered when the service has finished shutting down.
        """
        self.factory.stopTrying()
        yield self.factory.stopFactory()
        result = yield service.MultiService.stopService(self)
        defer.returnValue(result)

    @property
    def stats(self) -> "FactoryStatistics":
        """The statistics for this service."""
        return self.factory.stats

    @property
    def consuming(self) -> bool:
        """Whether the service is currently consuming messages."""
        return self.factory.consuming


def _configure_tls_parameters(parameters: pika.connection.Parameters) -> None:
    """
    Configure the pika connection parameters for TLS based on the configuration.

    This modifies the object provided to it. This accounts for whether or not
    the new API based on the standard library's SSLContext is available for
    pika.

    Args:
        parameters: The connection parameters to apply TLS connection settings to.
    """
    cert = config.conf["tls"]["certfile"]
    key = config.conf["tls"]["keyfile"]
    if cert and key:
        _log.info(
            "Authenticating with server using x509 (certfile: %s, keyfile: %s)",
            cert,
            key,
        )
        parameters.credentials = pika.credentials.ExternalCredentials()
    else:
        cert, key = None, None

    ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
    if config.conf["tls"]["ca_cert"]:
        try:
            ssl_context.load_verify_locations(cafile=config.conf["tls"]["ca_cert"])
        except ssl.SSLError as e:
            raise exceptions.ConfigurationException(
                f'The "ca_cert" setting in the "tls" section is invalid ({e})'
            ) from e
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
    ssl_context.check_hostname = True
    if cert and key:
        try:
            ssl_context.load_cert_chain(cert, key)
        except ssl.SSLError as e:
            raise exceptions.ConfigurationException(
                f'The "keyfile" setting in the "tls" section is invalid ({e})'
            ) from e
    parameters.ssl_options = SSLOptions(ssl_context, server_hostname=parameters.host)


def _ssl_context_factory(parameters: pika.connection.Parameters) -> "ClientTLSOptions":
    """
    Produce a Twisted SSL context object from a pika connection parameter object.
    This is necessary as Twisted manages the connection, not Pika.

    Args:
        parameters: The connection parameters built from the fedora_messaging configuration.
    """
    client_cert = None
    ca_cert = None
    key = config.conf["tls"]["keyfile"]
    cert = config.conf["tls"]["certfile"]
    ca_file = config.conf["tls"]["ca_cert"]
    if ca_file:
        with open(ca_file, "rb") as fd:
            # Open it in binary mode since otherwise Twisted will immediately
            # re-encode it as ASCII, which won't work if the cert bundle has
            # comments that can't be encoded with ASCII.
            ca_cert = twisted_ssl.Certificate.loadPEM(fd.read())
    if key and cert:
        # Note that _configure_tls_parameters sets the auth mode to EXTERNAL
        # if both key and cert are defined, so we don't need to do that here.
        with open(key) as fd:
            client_keypair = fd.read()
        with open(cert) as fd:
            client_keypair += fd.read()
        client_cert = twisted_ssl.PrivateCertificate.loadPEM(client_keypair)

    hostname = parameters.host
    context_factory: ClientTLSOptions = twisted_ssl.optionsForClientTLS(
        hostname,
        trustRoot=ca_cert or twisted_ssl.platformTrust(),
        clientCertificate=client_cert,
        extraCertificateOptions={"raiseMinimumTo": twisted_ssl.TLSVersion.TLSv1_2},
    )
    return context_factory
