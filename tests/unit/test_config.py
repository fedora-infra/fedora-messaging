# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

"""Unit tests for :module:`fedora_messaging.config`."""

from unittest import mock

import pytest

from fedora_messaging import config as msg_config
from fedora_messaging.exceptions import ConfigurationException


full_config = b"""
amqp_url = "amqp://guest:guest@rabbit-server1:5672/%2F"

publish_exchange = "special_exchange"

topic_prefix = ""

publish_priority = 42

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
empty_config = b'# publish_exchange = "special_exchange"'
partial_config = b'publish_exchange = "special_exchange"'
malformed_config = b'publish_exchange = "special_exchange'  # missing close quote
empty_monitoring_config = b"[monitoring]\n"
monitoring_config_with_port = b"[monitoring]\nport = 42\n"
monitoring_config_without_port = b"[monitoring]\naddress = ''\n"


class TestObj:
    pass


class TestValidateBindings:
    """Unit tests for :func:`fedora_messaging.config.validate_bindings`."""

    def test_valid(self) -> None:
        """Assert no exceptions are raised if the bindings are valid."""
        bindings: msg_config.BindingsType = [
            {"queue": "q1", "exchange": "e1", "routing_keys": ["#"]},
            {"queue": "q2", "exchange": "e2", "routing_keys": ("#",)},
        ]

        msg_config.validate_bindings(bindings)

    def test_wrong_type(self) -> None:
        """Assert a useful message is provided if bindings isn't a list or tuple"""
        with pytest.raises(ConfigurationException) as cm:
            msg_config.validate_bindings(TestObj())  # type: ignore
        assert (
            "Configuration error: bindings must be a list or tuple of dictionaries, "
            "but was a <class 'tests.unit.test_config.TestObj'>" == str(cm.value)
        )

    def test_missing_keys(self):
        """Assert a useful message is provided if "queue" is missing from the config."""
        bindings = [{}]
        with pytest.raises(ConfigurationException) as cm:
            msg_config.validate_bindings(bindings)  # type: ignore
        assert "Configuration error: a binding is missing the following keys" in str(cm.value)
        assert "exchange" in str(cm.value)
        assert "routing_keys" in str(cm.value)

    def test_routing_key_str(self) -> None:
        """Assert a useful message is provided if "routing_keys" is not a list or tuple."""
        bindings = [{"exchange": "e1", "queue": "q1", "routing_keys": TestObj()}]
        with pytest.raises(ConfigurationException) as cm:
            msg_config.validate_bindings(bindings)  # type: ignore
        assert (
            "Configuration error: routing_keys must be a list or tuple, but was a "
            "<class 'tests.unit.test_config.TestObj'>" == str(cm.value)
        )


class TestValidateQueues:
    """Unit tests for :func:`fedora_messaging.config.validate_queues`."""

    def test_valid(self) -> None:
        """Assert no exception is raised with a valid configuration."""
        queues: dict[str, msg_config.QueueConfig] = {
            "q1": {
                "durable": True,
                "auto_delete": False,
                "exclusive": False,
                "arguments": {},
            }
        }

        msg_config.validate_queues(queues)

    def test_invalid_type(self) -> None:
        with pytest.raises(ConfigurationException) as cm:
            msg_config.validate_queues([])  # type: ignore
        assert (
            "Configuration error: 'queues' must be a dictionary mapping queue names to settings."
            == str(cm.value)
        )

    def test_settings_invalid_type(self) -> None:
        with pytest.raises(ConfigurationException) as cm:
            msg_config.validate_queues({"q1": TestObj()})  # type: ignore
        assert (
            "Configuration error: the q1 queue in the 'queues' setting has a value of type "
            "<class 'tests.unit.test_config.TestObj'>, but it should be a "
            "dictionary of settings." == str(cm.value)
        )
        assert "it should be a dictionary of settings." in str(cm.value)

    def test_missing_keys(self) -> None:
        with pytest.raises(ConfigurationException) as cm:
            msg_config.validate_queues({"q1": {}})  # type: ignore
        assert (
            "Configuration error: the q1 queue is missing the following keys from its settings"
            in str(cm.value)
        )
        assert "durable" in str(cm.value)
        assert "auto_delete" in str(cm.value)
        assert "exclusive" in str(cm.value)
        assert "arguments" in str(cm.value)


class TestLoad:
    """Unit tests for :func:`fedora_messaging.config.load`."""

    def test_deep_copy(self) -> None:
        """Assert nested dictionaries in DEFAULTS are not copied into the config instance."""
        config = msg_config.LazyConfig().load_config()

        config["queues"]["somequeue"] = {}

        assert "somequeue" not in msg_config.DEFAULTS["queues"]

    @mock.patch("fedora_messaging.config._log", autospec=True)
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=False)
    def test_missing_config_file(self, mock_exists, mock_log):
        """Assert loading the config with a missing file works."""
        config = msg_config.LazyConfig().load_config()
        assert msg_config.DEFAULTS == config
        mock_exists.assert_called_once_with("/etc/fedora-messaging/config.toml")
        mock_log.info.assert_called_once_with(
            "The configuration file, /etc/fedora-messaging/config.toml, does not exist."
        )

    @mock.patch("fedora_messaging.config.open", mock.mock_open(read_data=b'bad_key = "val"'))
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_override_client_props(self, mock_exists):
        """Assert overriding reserved keys in client properties fails."""
        conf = '[client_properties]\n{} = "val"'
        for key in ("version", "information", "product"):
            with mock.patch(
                "fedora_messaging.config.open",
                mock.mock_open(read_data=conf.format(key).encode("utf-8")),
            ):
                config = msg_config.LazyConfig()
                with pytest.raises(ConfigurationException):
                    config.load_config()

    @mock.patch("fedora_messaging.config.open", mock.mock_open(read_data=b'bad_key = "val"'))
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_invalid_key(self, mock_exists):
        """Assert an unknown config key raises an exception."""
        config = msg_config.LazyConfig()
        with pytest.raises(ConfigurationException):
            config.load_config()

    @mock.patch("fedora_messaging.config.open", mock.mock_open(read_data=b"Ni!"))
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_bad_config_file(self, mock_exists):
        """Assert an invalid TOML file raises a ConfigurationException."""
        with pytest.raises(ConfigurationException) as cm:
            msg_config.LazyConfig().load_config()
        error = (
            "Failed to parse /etc/fedora-messaging/config.toml: "
            "Expected '=' after a key in a key/value pair (at line 1, column 3)"
        )
        # older tomli version used in Python 3.6 uses double-quotes
        error_old = error.replace("'", '"')
        assert cm.value.message in (error, error_old)

    @mock.patch("fedora_messaging.config.open", mock.mock_open(read_data=partial_config))
    @mock.patch("fedora_messaging.config._log", autospec=True)
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_partial_config_file(self, mock_exists, mock_log):
        """Assert a config file that uses a subset of keys works as expected"""
        config = msg_config.LazyConfig().load_config()
        assert "special_exchange" != msg_config.DEFAULTS["publish_exchange"]
        assert "special_exchange" == config["publish_exchange"]
        mock_exists.assert_called_once_with("/etc/fedora-messaging/config.toml")
        mock_log.info.assert_called_once_with(
            "Loading configuration from /etc/fedora-messaging/config.toml"
        )
        assert 0 == mock_log.warning.call_count

    @mock.patch("fedora_messaging.config.open", mock.mock_open(read_data=empty_monitoring_config))
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_empty_monitoring_section(self, mock_exists):
        """Assert the monitoring port is mandatory"""
        config = msg_config.LazyConfig().load_config()
        assert config["monitoring"] == {}

    @mock.patch(
        "fedora_messaging.config.open", mock.mock_open(read_data=monitoring_config_without_port)
    )
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_monitoring_section_without_port(self, mock_exists):
        """Assert the monitoring port is mandatory"""
        with pytest.raises(ConfigurationException) as cm:
            msg_config.LazyConfig().load_config()
        assert cm.value.message == "The port must be defined in [monitoring] to activate it"

    @mock.patch(
        "fedora_messaging.config.open", mock.mock_open(read_data=monitoring_config_with_port)
    )
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_monitoring_section_with_port(self, mock_exists):
        """Assert the monitoring address default is set if absent"""
        config = msg_config.LazyConfig().load_config()
        assert config["monitoring"]["port"] == 42
        assert config["monitoring"]["address"] == ""

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
            publish_priority=42,
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
            bindings=[{"queue": "my_queue", "exchange": "amq.topic", "routing_keys": ["#"]}],
            qos={"prefetch_size": 25, "prefetch_count": 25},
            monitoring={},
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
                "formatters": {"simple": {"format": "[%(name)s %(levelname)s] %(message)s"}},
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
        assert sorted(expected_config.keys()) == sorted(config.keys())
        for key in expected_config:
            assert expected_config[key] == config[key]
        mock_exists.assert_called_once_with("/etc/fedora-messaging/config.toml")
        mock_log.info.assert_called_once_with(
            "Loading configuration from /etc/fedora-messaging/config.toml"
        )
        assert 0 == mock_log.warning.call_count

    @mock.patch("fedora_messaging.config.open", mock.mock_open(read_data=partial_config))
    @mock.patch.dict("fedora_messaging.config.os.environ", {"FEDORA_MESSAGING_CONF": "/my/config"})
    @mock.patch("fedora_messaging.config._log", autospec=True)
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_custom_config_file(self, mock_exists, mock_log):
        """Assert using the environment variable to set the config path works."""
        config = msg_config.LazyConfig().load_config()
        assert "special_exchange" != msg_config.DEFAULTS["publish_exchange"]
        assert "special_exchange" == config["publish_exchange"]
        mock_exists.assert_called_once_with("/my/config")
        mock_log.info.assert_called_once_with("Loading configuration from /my/config")
        assert 0 == mock_log.warning.call_count

    @mock.patch("fedora_messaging.config.open", mock.mock_open(read_data=empty_config))
    @mock.patch("fedora_messaging.config._log", autospec=True)
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_empty_config_file(self, mock_exists, mock_log):
        """Assert loading the config with an empty file that exists works."""
        config = msg_config.LazyConfig().load_config()
        assert msg_config.DEFAULTS == config
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
        assert msg_config.DEFAULTS == copy
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
        assert msg_config.DEFAULTS["callback"] == config.get("callback")
        assert msg_config.DEFAULTS["amqp_url"] == config.get("amqp_url")
        mock_exists.assert_called_once_with("/etc/fedora-messaging/config.toml")
        mock_log.info.assert_called_once_with(
            "Loading configuration from /etc/fedora-messaging/config.toml"
        )

    def test_explode_on_pop(self):
        """Assert calling pop raises an exception."""
        config = msg_config.LazyConfig()
        with pytest.raises(ConfigurationException):
            config.pop()

    @mock.patch("fedora_messaging.config.open", mock.mock_open(read_data=empty_config))
    @mock.patch("fedora_messaging.config._log", autospec=True)
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_load_on_update(self, mock_exists, mock_log):
        """Assert the config is loaded when update is called."""
        config = msg_config.LazyConfig()
        config.update({})
        assert msg_config.DEFAULTS == config
        mock_exists.assert_called_once_with("/etc/fedora-messaging/config.toml")
        mock_log.info.assert_called_once_with(
            "Loading configuration from /etc/fedora-messaging/config.toml"
        )

    @mock.patch("fedora_messaging.config.logging.config.dictConfig", mock.Mock())
    @mock.patch("fedora_messaging.config.open", mock.mock_open(read_data=empty_config))
    @mock.patch("fedora_messaging.config._log", autospec=True)
    @mock.patch("fedora_messaging.config.os.path.exists", return_value=True)
    def test_load_on_setup_logging(self, mock_exists, mock_log):
        """Assert the config is loaded when setup_logging is called."""
        config = msg_config.LazyConfig()
        config.setup_logging()
        assert msg_config.DEFAULTS == config
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
