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


import os
from unittest import mock

import pika
import pytest
from pika import URLParameters
from twisted.application.internet import SSLClient, TCPClient
from twisted.internet import ssl as twisted_ssl

from fedora_messaging import config, exceptions
from fedora_messaging.twisted.factory import FedoraMessagingFactoryV2
from fedora_messaging.twisted.service import (
    _configure_tls_parameters,
    _ssl_context_factory,
    FedoraMessagingServiceV2,
    SSLOptions,
)


class TestService:
    def test_init(self):
        service = FedoraMessagingServiceV2("amqp://example.com:4242")
        assert isinstance(service._parameters, pika.URLParameters)
        assert service._parameters.host == "example.com"
        assert service._parameters.port == 4242
        assert getattr(service._parameters, "ssl", False) is False
        assert service._parameters.client_properties == config.conf["client_properties"]
        assert isinstance(service._service.factory, FedoraMessagingFactoryV2)
        assert len(service.services) == 1
        assert service.services[0] is service._service
        assert service._service.parent is service
        assert isinstance(service._service, TCPClient)

    def test_init_tls(self):
        """Assert creating the service with an amqps URL configures TLS."""
        service = FedoraMessagingServiceV2("amqps://")

        assert isinstance(service._parameters, pika.URLParameters)
        assert service._parameters.ssl_options is not None
        assert len(service.services) == 1
        assert isinstance(service.services[0], SSLClient)

    def test_init_client_props_override(self):
        service = FedoraMessagingServiceV2("amqp://?client_properties={'foo':'bar'}")
        assert service._parameters.client_properties == {"foo": "bar"}

    def test_stopService(self):
        service = FedoraMessagingServiceV2("amqps://")
        service._service = mock.Mock()
        service.stopService()
        service._service.factory.stopTrying.assert_called_once()
        service._service.factory.stopFactory.assert_called_once()


class ConfigureTlsParameters:
    """Tests for :func:`fedora_messaging._session._configure_tls_parameters`"""

    def test_key_and_cert(self, fixtures_dir):
        """Assert configuring a cert and key results in a TLS connection with new pika versions."""
        params = URLParameters("amqps://myhost")
        tls_conf = {
            "keyfile": os.path.join(fixtures_dir, "key.pem"),
            "certfile": os.path.join(fixtures_dir, "cert.pem"),
            "ca_cert": os.path.join(fixtures_dir, "ca_bundle.pem"),
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            _configure_tls_parameters(params)

        assert isinstance(params.ssl_options, SSLOptions)

    def test__invalid_key(self, fixtures_dir):
        """Assert a ConfigurationException is raised when the key can't be opened."""
        params = URLParameters("amqps://myhost")
        tls_conf = {
            "keyfile": os.path.join(fixtures_dir, "invalid_key.pem"),
            "certfile": os.path.join(fixtures_dir, "cert.pem"),
            "ca_cert": os.path.join(fixtures_dir, "ca_bundle.pem"),
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            with pytest.raises(exceptions.ConfigurationException):
                _configure_tls_parameters(params)

        assert isinstance(params.ssl_options, SSLOptions)

    def test_invalid_ca_cert(self, fixtures_dir):
        """Assert a ConfigurationException is raised when the CA can't be opened."""
        params = URLParameters("amqps://myhost")
        tls_conf = {
            "keyfile": os.path.join(fixtures_dir, "key.pem"),
            "certfile": os.path.join(fixtures_dir, "cert.pem"),
            "ca_cert": os.path.join(fixtures_dir, "invalid_ca.pem"),
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            with pytest.raises(exceptions.ConfigurationException):
                _configure_tls_parameters(params)

        assert isinstance(params.ssl_options, SSLOptions)


class TestSslContextFactory:
    @mock.patch("fedora_messaging.twisted.service.twisted_ssl.Certificate.loadPEM")
    @mock.patch("fedora_messaging.twisted.service.twisted_ssl.optionsForClientTLS")
    def test_no_key(self, mock_opts, mock_load_pem, fixtures_dir):
        """Assert if there's no client key, the factory isn't configured to use it."""
        tls_conf = {
            "keyfile": None,
            "certfile": os.path.join(fixtures_dir, "cert.pem"),
            "ca_cert": os.path.join(fixtures_dir, "ca_bundle.pem"),
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            factory = _ssl_context_factory(mock.Mock(host="dummy_host"))

        mock_opts.assert_called_once_with(
            "dummy_host",
            trustRoot=mock_load_pem.return_value,
            clientCertificate=None,
            extraCertificateOptions={"raiseMinimumTo": twisted_ssl.TLSVersion.TLSv1_2},
        )
        assert factory == mock_opts.return_value

    @mock.patch("fedora_messaging.twisted.service.twisted_ssl.Certificate.loadPEM")
    @mock.patch("fedora_messaging.twisted.service.twisted_ssl.optionsForClientTLS")
    def test_no_cert(self, mock_opts, mock_load_pem, fixtures_dir):
        """Assert if there's no client cert, the factory isn't configured to use it."""
        tls_conf = {
            "keyfile": os.path.join(fixtures_dir, "key.pem"),
            "certfile": None,
            "ca_cert": os.path.join(fixtures_dir, "ca_bundle.pem"),
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            factory = _ssl_context_factory(mock.Mock(host="dummy_host"))

        mock_opts.assert_called_once_with(
            "dummy_host",
            trustRoot=mock_load_pem.return_value,
            clientCertificate=None,
            extraCertificateOptions={"raiseMinimumTo": twisted_ssl.TLSVersion.TLSv1_2},
        )
        assert factory == mock_opts.return_value

    @mock.patch(
        "fedora_messaging.twisted.service.twisted_ssl.PrivateCertificate.loadPEM"
    )
    @mock.patch("fedora_messaging.twisted.service.twisted_ssl.Certificate.loadPEM")
    @mock.patch("fedora_messaging.twisted.service.twisted_ssl.optionsForClientTLS")
    def test_key_and_cert(
        self, mock_opts, mock_load_pem, mock_priv_load_pem, fixtures_dir
    ):
        """Assert if there's a client key and cert, the factory has both."""
        tls_conf = {
            "keyfile": os.path.join(fixtures_dir, "key.pem"),
            "certfile": os.path.join(fixtures_dir, "cert.pem"),
            "ca_cert": os.path.join(fixtures_dir, "ca_bundle.pem"),
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            factory = _ssl_context_factory(mock.Mock(host="dummy_host"))

        mock_opts.assert_called_once_with(
            "dummy_host",
            trustRoot=mock_load_pem.return_value,
            clientCertificate=mock_priv_load_pem.return_value,
            extraCertificateOptions={"raiseMinimumTo": twisted_ssl.TLSVersion.TLSv1_2},
        )
        assert factory == mock_opts.return_value
