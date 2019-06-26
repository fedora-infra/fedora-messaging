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

from __future__ import absolute_import, unicode_literals

import unittest
import os

from twisted.internet import ssl as twisted_ssl
from pika import URLParameters
import mock
import pika

from fedora_messaging import config, exceptions
from fedora_messaging.twisted.service import (
    FedoraMessagingService,
    _configure_tls_parameters,
    _ssl_context_factory,
    SSLOptions,
)
from fedora_messaging.tests import FIXTURES_DIR


class ServiceTests(unittest.TestCase):
    def test_init_tls(self):
        """Assert creating the service with an amqps URL configures TLS."""
        service = FedoraMessagingService("amqps://")

        self.assertTrue(isinstance(service._parameters, pika.URLParameters))
        self.assertIsNotNone(service._parameters.ssl_options)


class ConfigureTlsParameters(unittest.TestCase):
    """Tests for :func:`fedora_messaging._session._configure_tls_parameters`"""

    def test_key_and_cert(self):
        """Assert configuring a cert and key results in a TLS connection with new pika versions."""
        params = URLParameters("amqps://myhost")
        tls_conf = {
            "keyfile": os.path.join(FIXTURES_DIR, "key.pem"),
            "certfile": os.path.join(FIXTURES_DIR, "cert.pem"),
            "ca_cert": os.path.join(FIXTURES_DIR, "ca_bundle.pem"),
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            _configure_tls_parameters(params)

        self.assertTrue(isinstance(params.ssl_options, SSLOptions))

    def test__invalid_key(self):
        """Assert a ConfigurationException is raised when the key can't be opened."""
        params = URLParameters("amqps://myhost")
        tls_conf = {
            "keyfile": os.path.join(FIXTURES_DIR, "invalid_key.pem"),
            "certfile": os.path.join(FIXTURES_DIR, "cert.pem"),
            "ca_cert": os.path.join(FIXTURES_DIR, "ca_bundle.pem"),
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            self.assertRaises(
                exceptions.ConfigurationException, _configure_tls_parameters, params
            )

        self.assertTrue(isinstance(params.ssl_options, SSLOptions))

    def test_invalid_ca_cert(self):
        """Assert a ConfigurationException is raised when the CA can't be opened."""
        params = URLParameters("amqps://myhost")
        tls_conf = {
            "keyfile": os.path.join(FIXTURES_DIR, "key.pem"),
            "certfile": os.path.join(FIXTURES_DIR, "cert.pem"),
            "ca_cert": os.path.join(FIXTURES_DIR, "invalid_ca.pem"),
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            self.assertRaises(
                exceptions.ConfigurationException, _configure_tls_parameters, params
            )

        self.assertTrue(isinstance(params.ssl_options, SSLOptions))


class SslContextFactoryTests(unittest.TestCase):
    @mock.patch("fedora_messaging.twisted.service.twisted_ssl.Certificate.loadPEM")
    @mock.patch("fedora_messaging.twisted.service.twisted_ssl.optionsForClientTLS")
    def test_no_key(self, mock_opts, mock_load_pem):
        """Assert if there's no client key, the factory isn't configured to use it."""
        tls_conf = {
            "keyfile": None,
            "certfile": os.path.join(FIXTURES_DIR, "cert.pem"),
            "ca_cert": os.path.join(FIXTURES_DIR, "ca_bundle.pem"),
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            factory = _ssl_context_factory(mock.Mock(host="dummy_host"))

        mock_opts.assert_called_once_with(
            "dummy_host",
            trustRoot=mock_load_pem.return_value,
            clientCertificate=None,
            extraCertificateOptions={"raiseMinimumTo": twisted_ssl.TLSVersion.TLSv1_2},
        )
        self.assertEqual(factory, mock_opts.return_value)

    @mock.patch("fedora_messaging.twisted.service.twisted_ssl.Certificate.loadPEM")
    @mock.patch("fedora_messaging.twisted.service.twisted_ssl.optionsForClientTLS")
    def test_no_cert(self, mock_opts, mock_load_pem):
        """Assert if there's no client cert, the factory isn't configured to use it."""
        tls_conf = {
            "keyfile": os.path.join(FIXTURES_DIR, "key.pem"),
            "certfile": None,
            "ca_cert": os.path.join(FIXTURES_DIR, "ca_bundle.pem"),
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            factory = _ssl_context_factory(mock.Mock(host="dummy_host"))

        mock_opts.assert_called_once_with(
            "dummy_host",
            trustRoot=mock_load_pem.return_value,
            clientCertificate=None,
            extraCertificateOptions={"raiseMinimumTo": twisted_ssl.TLSVersion.TLSv1_2},
        )
        self.assertEqual(factory, mock_opts.return_value)

    @mock.patch(
        "fedora_messaging.twisted.service.twisted_ssl.PrivateCertificate.loadPEM"
    )
    @mock.patch("fedora_messaging.twisted.service.twisted_ssl.Certificate.loadPEM")
    @mock.patch("fedora_messaging.twisted.service.twisted_ssl.optionsForClientTLS")
    def test_key_and_cert(self, mock_opts, mock_load_pem, mock_priv_load_pem):
        """Assert if there's a client key and cert, the factory has both."""
        tls_conf = {
            "keyfile": os.path.join(FIXTURES_DIR, "key.pem"),
            "certfile": os.path.join(FIXTURES_DIR, "cert.pem"),
            "ca_cert": os.path.join(FIXTURES_DIR, "ca_bundle.pem"),
        }

        with mock.patch.dict(config.conf, {"tls": tls_conf}):
            factory = _ssl_context_factory(mock.Mock(host="dummy_host"))

        mock_opts.assert_called_once_with(
            "dummy_host",
            trustRoot=mock_load_pem.return_value,
            clientCertificate=mock_priv_load_pem.return_value,
            extraCertificateOptions={"raiseMinimumTo": twisted_ssl.TLSVersion.TLSv1_2},
        )
        self.assertEqual(factory, mock_opts.return_value)
