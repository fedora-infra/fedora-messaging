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

topic_prefix = ""

callback = "fedora_messaging.examples:print_msg"

bindings = [
    {queue = "my_queue", exchange = "amq.topic", routing_keys = ["#"]},
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


class TestObj(object):
    """This exists purely to make __repr__ the same on Python 2 and 3."""


class ValidateBindingsTests(unittest.TestCase):
    """Unit tests for :func:`fedora_messaging.config.validate_bindings`."""

    def test_valid(self):
        """Assert no exceptions are raised if the bindings are valid."""
        bindings = [
            {"queue": "q1", "exchange": "e1", "routing_keys": ["#"]},
            {"queue": "q2", "exchange": "e2", "routing_keys": ("#",)},
        ]

        msg_config.validate_bindings(bindings)

    def test_wrong_type(self):
        """Assert a useful message is provided if bindings isn't a list or tuple"""
        with self.assertRaises(ConfigurationException) as cm:
            msg_config.validate_bindings(TestObj())
        self.assertEqual(
            "Configuration error: bindings must be a list or tuple of dictionaries, "
            "but was a <class 'fedora_messaging.tests.unit.test_config.TestObj'>",
            str(cm.exception),
        )

    def test_missing_keys(self):
        """Assert a useful message is provided if "queue" is missing from the config."""
        bindings = [{}]
        with self.assertRaises(ConfigurationException) as cm:
            msg_config.validate_bindings(bindings)
        self.assertIn(
            "Configuration error: a binding is missing the following keys",
            str(cm.exception),
        )
        self.assertIn("queue", str(cm.exception))
        self.assertIn("exchange", str(cm.exception))
        self.assertIn("routing_keys", str(cm.exception))

    def test_routing_key_str(self):
        """Assert a useful message is provided if "routing_keys" is not a list or tuple."""
        bindings = [{"exchange": "e1", "queue": "q1", "routing_keys": TestObj()}]
        with self.assertRaises(ConfigurationException) as cm:
            msg_config.validate_bindings(bindings)
        self.assertEqual(
            "Configuration error: routing_keys must be a list or tuple, but was a "
            "<class 'fedora_messaging.tests.unit.test_config.TestObj'>",
            str(cm.exception),
        )


class ValidateQueuesTests(unittest.TestCase):
    """Unit tests for :func:`fedora_messaging.config.validate_queues`."""

    def test_valid(self):
        """Assert no exception is raised with a valid configuration."""
        queues = {
            "q1": {
                "durable": True,
                "auto_delete": False,
                "exclusive": False,
                "arguments": {},
            }
        }

        msg_config.validate_queues(queues)

    def test_invalid_type(self):
        with self.assertRaises(ConfigurationException) as cm:
            msg_config.validate_queues([])
        self.assertEqual(
            "Configuration error: 'queues' must be a dictionary mapping queue names to settings.",
            str(cm.exception),
        )

    def test_settings_invalid_type(self):
        with self.assertRaises(ConfigurationException) as cm:
            msg_config.validate_queues({"q1": TestObj()})
        self.assertEqual(
            "Configuration error: the q1 queue in the 'queues' setting has a value of type "
            "<class 'fedora_messaging.tests.unit.test_config.TestObj'>, but it should be a "
            "dictionary of settings.",
            str(cm.exception),
        )
        self.assertIn("it should be a dictionary of settings.", str(cm.exception))

    def test_missing_keys(self):
        with self.assertRaises(ConfigurationException) as cm:
            msg_config.validate_queues({"q1": {}})
        self.assertIn(
            "Configuration error: the q1 queue is missing the following keys from its settings",
            str(cm.exception),
        )
        self.assertIn("durable", str(cm.exception))
        self.assertIn("auto_delete", str(cm.exception))
        self.assertIn("exclusive", str(cm.exception))
        self.assertIn("arguments", str(cm.exception))


class LoadTests(unittest.TestCase):
    """Unit tests for :func:`fedora_messaging.config.load`."""

    def test_deep_copy(self):
        """Assert nested dictionaries in DEFAULTS are not copied into the config instance."""
        config = msg_config.LazyConfig().load_config()

        config["queues"]["somequeue"] = {}

        self.assertNotIn("somequeue", msg_config.DEFAULTS["queues"])

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
        error = (
            "Failed to parse /etc/fedora-messaging/config.toml: error at line 1, column 3: "
            "Found invalid character in key name: '!'. Try quoting the key name."
        )
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
            topic_prefix="",
            publish_exchange="special_exchange",
            passive_declares=False,
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

    def test_load_on_get_item(self):
        """Assert load_config is called when __getitem__ is invoked."""
        config = msg_config.LazyConfig()
        config.load_config = mock.Mock()

        try:
            config["some_key"]
        except KeyError:
            pass

        config.load_config.assert_called_once_with()
