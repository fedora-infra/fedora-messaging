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
"""Unit tests for :module:`fedora_messaging.config`."""

import unittest

import mock

from fedora_messaging import config as msg_config
from fedora_messaging.exceptions import ConfigurationException


full_config = """
amqp_url = "amqp://guest:guest@rabbit-server1:5672/%2F"

publish_exchange = "special_exchange"

callback = "fedora_messaging.examples:print_msg"

bindings = [
    {"queue" = "my_queue", "exchange" = "amq.topic", "routing_keys" = ["#"]},
]

[tls]
ca_cert = "/etc/pki/tls/certs/ca-bundle.crt"
keyfile = "/my/client/key.pem"
certfile = "/my/client/cert.pem"

[client_properties]
app = "Example App"

[exchanges.custom_exchange]
type = "fanout"
durable = false
auto_delete = false
arguments = {}

[queues.my_queue]
durable = true
auto_delete = false
exclusive = false
arguments = {}

[qos]
prefetch_size = 25
prefetch_count = 25

[consumer_config]
example_key = "for my consumer"

[log_config]
version = 1
disable_existing_loggers = true

[log_config.formatters.simple]
format = "[%(name)s %(levelname)s] %(message)s"

[log_config.handlers.console]
class = "logging.StreamHandler"
formatter = "simple"
stream = "ext://sys.stderr"

[log_config.loggers.fedora_messaging]
level = "INFO"
propagate = false
handlers = ["console"]

[log_config.root]
level = "DEBUG"
handlers = ["console"]
"""
empty_config = '# publish_exchange = "special_exchange"'
partial_config = 'publish_exchange = "special_exchange"'
malformed_config = 'publish_exchange = "special_exchange'  # missing close quote


class LoadTests(unittest.TestCase):
    """Unit tests for :func:`fedora_messaging.config.load`."""

    @mock.patch("fedora_messaging.config._log", autospec=True)
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=False)
    def test_missing_config_file(self, mock_exists, mock_log):
        """Assert loading the config with a missing file works."""
        config = msg_config.LazyConfig().load_config()
        self.assertEqual(msg_config.DEFAULTS, config)
        mock_exists.assert_called_once_with("/etc/fedora-messaging/config.toml")
        mock_log.info.assert_called_once_with(
            "The configuration file, /etc/fedora-messaging/config.toml, does not exist."
        )

    @mock.patch(
        "fedora_messaging.config.open", mock.mock_open(read_data='bad_key = "val"')
    )
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_override_client_props(self, mock_exists):
        """Assert overriding reserved keys in client properties fails."""
        conf = '[client_properties]\n{} = "val"'
        for key in ("version", "information", "product"):
            with mock.patch(
                "fedora_messaging.config.open",
                mock.mock_open(read_data=conf.format(key)),
            ):
                config = msg_config.LazyConfig()
                self.assertRaises(ConfigurationException, config.load_config)

    @mock.patch(
        "fedora_messaging.config.open", mock.mock_open(read_data='bad_key = "val"')
    )
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_invalid_key(self, mock_exists):
        """Assert an unknown config key raises an exception."""
        config = msg_config.LazyConfig()
        self.assertRaises(ConfigurationException, config.load_config)

    @mock.patch("fedora_messaging.config.open", mock.mock_open(read_data="Ni!"))
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_bad_config_file(self, mock_exists):
        """Assert an invalid TOML file raises a ConfigurationException."""
        with self.assertRaises(ConfigurationException) as cm:
            msg_config.LazyConfig().load_config()
        error = "Failed to parse /etc/fedora-messaging/config.toml: error at line 1, column 1"
        self.assertEqual(error, cm.exception.message)

    @mock.patch(
        "fedora_messaging.config.open", mock.mock_open(read_data=partial_config)
    )
    @mock.patch("fedora_messaging.config._log", autospec=True)
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_partial_config_file(self, mock_exists, mock_log):
        """Assert a config file that uses a subset of keys works as expected"""
        config = msg_config.LazyConfig().load_config()
        self.assertNotEqual("special_exchange", msg_config.DEFAULTS["publish_exchange"])
        self.assertEqual("special_exchange", config["publish_exchange"])
        mock_exists.assert_called_once_with("/etc/fedora-messaging/config.toml")
        mock_log.info.assert_called_once_with(
            "Loading configuration from /etc/fedora-messaging/config.toml"
        )
        self.assertEqual(0, mock_log.warning.call_count)

    @mock.patch("fedora_messaging.config.open", mock.mock_open(read_data=full_config))
    @mock.patch("fedora_messaging.config._log", autospec=True)
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_full_config_file(self, mock_exists, mock_log):
        """Assert a config with the full set of configurations loads correctly."""
        expected_config = dict(
            amqp_url="amqp://guest:guest@rabbit-server1:5672/%2F",
            client_properties={
                "app": "Example App",
                "product": "Fedora Messaging with Pika",
                "information": "https://fedora-messaging.readthedocs.io/en/stable/",
                "version": msg_config.DEFAULTS["client_properties"]["version"],
            },
            publish_exchange="special_exchange",
            exchanges={
                "custom_exchange": {
                    "type": "fanout",
                    "durable": False,
                    "auto_delete": False,
                    "arguments": {},
                }
            },
            queues={
                "my_queue": {
                    "durable": True,
                    "auto_delete": False,
                    "exclusive": False,
                    "arguments": {},
                }
            },
            bindings=[
                {"queue": "my_queue", "exchange": "amq.topic", "routing_keys": ["#"]}
            ],
            qos={"prefetch_size": 25, "prefetch_count": 25},
            callback="fedora_messaging.examples:print_msg",
            consumer_config={"example_key": "for my consumer"},
            tls={
                "ca_cert": "/etc/pki/tls/certs/ca-bundle.crt",
                "keyfile": "/my/client/key.pem",
                "certfile": "/my/client/cert.pem",
            },
            log_config={
                "version": 1,
                "disable_existing_loggers": True,
                "formatters": {
                    "simple": {"format": "[%(name)s %(levelname)s] %(message)s"}
                },
                "handlers": {
                    "console": {
                        "class": "logging.StreamHandler",
                        "formatter": "simple",
                        "stream": "ext://sys.stderr",
                    }
                },
                "loggers": {
                    "fedora_messaging": {
                        "level": "INFO",
                        "propagate": False,
                        "handlers": ["console"],
                    }
                },
                "root": {"level": "DEBUG", "handlers": ["console"]},
            },
        )
        config = msg_config.LazyConfig().load_config()
        self.assertEqual(sorted(expected_config.keys()), sorted(config.keys()))
        for key in expected_config:
            self.assertEqual(expected_config[key], config[key])
        mock_exists.assert_called_once_with("/etc/fedora-messaging/config.toml")
        mock_log.info.assert_called_once_with(
            "Loading configuration from /etc/fedora-messaging/config.toml"
        )
        self.assertEqual(0, mock_log.warning.call_count)

    @mock.patch(
        "fedora_messaging.config.open", mock.mock_open(read_data=partial_config)
    )
    @mock.patch.dict(
        "fedora_messaging.config.os.environ", {"FEDORA_MESSAGING_CONF": "/my/config"}
    )
    @mock.patch("fedora_messaging.config._log", autospec=True)
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_custom_config_file(self, mock_exists, mock_log):
        """Assert using the environment variable to set the config path works."""
        config = msg_config.LazyConfig().load_config()
        self.assertNotEqual("special_exchange", msg_config.DEFAULTS["publish_exchange"])
        self.assertEqual("special_exchange", config["publish_exchange"])
        mock_exists.assert_called_once_with("/my/config")
        mock_log.info.assert_called_once_with("Loading configuration from /my/config")
        self.assertEqual(0, mock_log.warning.call_count)

    @mock.patch("fedora_messaging.config.open", mock.mock_open(read_data=empty_config))
    @mock.patch("fedora_messaging.config._log", autospec=True)
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_empty_config_file(self, mock_exists, mock_log):
        """Assert loading the config with an empty file that exists works."""
        config = msg_config.LazyConfig().load_config()
        self.assertEqual(msg_config.DEFAULTS, config)
        mock_exists.assert_called_once_with("/etc/fedora-messaging/config.toml")
        mock_log.info.assert_called_once_with(
            "Loading configuration from /etc/fedora-messaging/config.toml"
        )

    @mock.patch("fedora_messaging.config.open", mock.mock_open(read_data=empty_config))
    @mock.patch("fedora_messaging.config.logging.config.dictConfig", autospec=True)
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_setup_logging(self, mock_exists, mock_dictConfig):
        """Assert setup_logging passes the log_config key to dictConfig."""
        config = msg_config.LazyConfig().load_config()
        config.setup_logging()
        mock_dictConfig.assert_called_once_with(config["log_config"])

    @mock.patch("fedora_messaging.config.open", mock.mock_open(read_data=empty_config))
    @mock.patch("fedora_messaging.config._log", autospec=True)
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_load_on_copy(self, mock_exists, mock_log):
        """Assert the config is loaded when copy is called."""
        config = msg_config.LazyConfig()
        copy = config.copy()
        self.assertEqual(msg_config.DEFAULTS, copy)
        mock_exists.assert_called_once_with("/etc/fedora-messaging/config.toml")
        mock_log.info.assert_called_once_with(
            "Loading configuration from /etc/fedora-messaging/config.toml"
        )

    @mock.patch("fedora_messaging.config.open", mock.mock_open(read_data=empty_config))
    @mock.patch("fedora_messaging.config._log", autospec=True)
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_load_on_get(self, mock_exists, mock_log):
        """Assert the config is loaded when get is called."""
        config = msg_config.LazyConfig()
        self.assertEqual(msg_config.DEFAULTS["callback"], config.get("callback"))
        self.assertEqual(msg_config.DEFAULTS["amqp_url"], config.get("amqp_url"))
        mock_exists.assert_called_once_with("/etc/fedora-messaging/config.toml")
        mock_log.info.assert_called_once_with(
            "Loading configuration from /etc/fedora-messaging/config.toml"
        )

    def test_explode_on_pop(self):
        """Assert calling pop raises an exception."""
        config = msg_config.LazyConfig()
        self.assertRaises(ConfigurationException, config.pop)

    @mock.patch("fedora_messaging.config.open", mock.mock_open(read_data=empty_config))
    @mock.patch("fedora_messaging.config._log", autospec=True)
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_load_on_update(self, mock_exists, mock_log):
        """Assert the config is loaded when update is called."""
        config = msg_config.LazyConfig()
        config.update({})
        self.assertEqual(msg_config.DEFAULTS, config)
        mock_exists.assert_called_once_with("/etc/fedora-messaging/config.toml")
        mock_log.info.assert_called_once_with(
            "Loading configuration from /etc/fedora-messaging/config.toml"
        )

    @mock.patch("fedora_messaging.config.open", mock.mock_open(read_data=empty_config))
    @mock.patch("fedora_messaging.config._log", autospec=True)
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_load_on_setup_logging(self, mock_exists, mock_log):
        """Assert the config is loaded when setup_logging is called."""
        config = msg_config.LazyConfig()
        config.setup_logging()
        self.assertEqual(msg_config.DEFAULTS, config)
        mock_exists.assert_called_once_with("/etc/fedora-messaging/config.toml")
        mock_log.info.assert_called_once_with(
            "Loading configuration from /etc/fedora-messaging/config.toml"
        )
