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

from twisted.internet import ssl
import mock
import pika

from fedora_messaging import config
from fedora_messaging.twisted.service import (
    FedoraMessagingService,
    _ssl_context_factory,
)
from fedora_messaging.tests import FIXTURES_DIR


class ServiceTests(unittest.TestCase):
    def test_init(self):
        callback = mock.Mock()
        service = FedoraMessagingService(
            callback, "amqp://example.com:4242", {"binding": "value"}
        )
        self.assertTrue(isinstance(service._parameters, pika.URLParameters))
        self.assertEqual(service._parameters.host, "example.com")
        self.assertEqual(service._parameters.port, 4242)
        self.assertEqual(getattr(service._parameters, "ssl", False), False)
        self.assertEqual(
            service._parameters.client_properties, config.conf["client_properties"]
        )
        self.assertEqual(service._bindings, {"binding": "value"})
        self.assertTrue(service._on_message is callback)

    def test_init_client_props_override(self):
        callback = mock.Mock()
        service = FedoraMessagingService(
            callback, "amqp://?client_properties={'foo':'bar'}"
        )
        self.assertEqual(service._parameters.client_properties, {"foo": "bar"})

    def test_connect(self):
        callback = mock.Mock()
        service = FedoraMessagingService(callback)
        factory = mock.Mock()
        service.factoryClass = mock.Mock(side_effect=lambda *a: factory)
        service.connect()
        service.factoryClass.assert_called_once_with(
            service._parameters, service._bindings
        )
        self.assertEqual(len(service.services), 1)
        serv = service.services[0]
        self.assertTrue(serv.factory is factory)
        self.assertTrue(serv.parent is service)

    def test_startService(self):
        callback = mock.Mock()
        service = FedoraMessagingService(callback)
        serv = mock.Mock()
        service.addService(serv)
        service.connect = mock.Mock()
        service.startService()
        service.connect.assert_called_once()
        serv.factory.consume.assert_called_once_with(callback)

    def test_startService_no_consume(self):
        service = FedoraMessagingService(None)
        serv = mock.Mock()
        service.addService(serv)
        service.connect = mock.Mock()
        service.startService()
        service.connect.assert_called_once()
        serv.factory.consume.assert_not_called()

    def test_stopService(self):
        callback = mock.Mock()
        service = FedoraMessagingService(callback)
        serv = mock.Mock()
        service.addService(serv)
        service.stopService()
        serv.factory.stopTrying.assert_called_once()

    def test_stopService_no_factory(self):
        service = FedoraMessagingService(None)
        ss_path = "fedora_messaging.twisted.service.service." "MultiService.stopService"
        with mock.patch(ss_path) as stopService:
            service.stopService()
            stopService.assert_not_called()


class SslContextFactoryTests(unittest.TestCase):
    @mock.patch("fedora_messaging.twisted.service.ssl.Certificate.loadPEM")
    @mock.patch("fedora_messaging.twisted.service.ssl.optionsForClientTLS")
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
            extraCertificateOptions={"raiseMinimumTo": ssl.TLSVersion.TLSv1_2},
        )
        self.assertEqual(factory, mock_opts.return_value)

    @mock.patch("fedora_messaging.twisted.service.ssl.Certificate.loadPEM")
    @mock.patch("fedora_messaging.twisted.service.ssl.optionsForClientTLS")
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
            extraCertificateOptions={"raiseMinimumTo": ssl.TLSVersion.TLSv1_2},
        )
        self.assertEqual(factory, mock_opts.return_value)

    @mock.patch("fedora_messaging.twisted.service.ssl.PrivateCertificate.loadPEM")
    @mock.patch("fedora_messaging.twisted.service.ssl.Certificate.loadPEM")
    @mock.patch("fedora_messaging.twisted.service.ssl.optionsForClientTLS")
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
            extraCertificateOptions={"raiseMinimumTo": ssl.TLSVersion.TLSv1_2},
        )
        self.assertEqual(factory, mock_opts.return_value)
