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
"""Tests for the :module:`fedora_messaging.cli` module."""

import os
import unittest

from click.testing import CliRunner
import mock

from fedora_messaging import cli, exceptions, config

FIXTURES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../fixtures/"))
GOOD_CONF = os.path.join(FIXTURES_DIR, "good_conf.toml")
BAD_CONF = os.path.join(FIXTURES_DIR, "bad_conf.toml")


def echo(message):
    """Echo the message received to standard output."""
    print(str(message))


class BaseCliTests(unittest.TestCase):
    """Unit tests for the base command of the CLI."""

    def test_no_conf(self):
        """Assert the CLI runs without exploding."""
        runner = CliRunner()
        result = runner.invoke(cli.cli)
        assert result.exit_code == 0


class ConsumeCliTests(unittest.TestCase):
    """Unit tests for the 'consume' command of the CLI."""

    def setUp(self):
        self.runner = CliRunner()

    def tearDown(self):
        """Make sure each test has a fresh default configuration."""
        config.conf = config.LazyConfig()
        config.conf.load_config()

    @mock.patch("fedora_messaging.cli.api.consume")
    def test_good_conf(self, mock_consume):
        """Assert providing a configuration file via the CLI works."""
        result = self.runner.invoke(cli.cli, ["--conf=" + GOOD_CONF, "consume"])
        mock_consume.assert_called_with(
            echo,
            bindings=[{"exchange": "e", "queue": "q", "routing_keys": ["#"]}],
            queues=config.conf["queues"],
        )
        self.assertEqual(0, result.exit_code)

    @mock.patch("fedora_messaging.cli.api.consume")
    def test_conf_env_support(self, mock_consume):
        """Assert FEDORA_MESSAGING_CONF environment variable is supported."""
        result = self.runner.invoke(
            cli.cli, ["consume"], env={"FEDORA_MESSAGING_CONF": GOOD_CONF}
        )
        mock_consume.assert_called_with(
            echo,
            bindings=[{"exchange": "e", "queue": "q", "routing_keys": ["#"]}],
            queues=config.conf["queues"],
        )
        self.assertEqual(0, result.exit_code)

    @mock.patch("fedora_messaging.cli.api.consume")
    def test_bad_conf(self, mock_consume):
        """Assert a bad configuration file is reported."""
        expected_err = (
            "Error: Invalid value: Configuration error: Failed to parse"
            " {}: error at line 1, column 1".format(BAD_CONF)
        )
        result = self.runner.invoke(cli.cli, ["--conf=" + BAD_CONF, "consume"])
        self.assertEqual(2, result.exit_code)
        self.assertIn(expected_err, result.output)

    @mock.patch("fedora_messaging.cli.api.consume")
    def test_missing_conf(self, mock_consume):
        """Assert a missing configuration file is reported."""
        result = self.runner.invoke(cli.cli, ["--conf=thispathdoesnotexist", "consume"])
        self.assertEqual(2, result.exit_code)
        self.assertIn(
            "Error: Invalid value: thispathdoesnotexist is not a file", result.output
        )

    @mock.patch("fedora_messaging.cli.api.consume")
    def test_good_cli_bindings(self, mock_consume):
        """Assert providing a bindings via the CLI works."""
        config.conf["callback"] = "fedora_messaging.tests.unit.test_cli:echo"

        result = self.runner.invoke(
            cli.cli,
            [
                "consume",
                "--exchange=e",
                "--queue-name=qn",
                "--routing-key=rk1",
                "--routing-key=rk2",
            ],
        )
        mock_consume.assert_called_once_with(
            echo,
            bindings=[{"exchange": "e", "queue": "qn", "routing_keys": ("rk1", "rk2")}],
            queues={
                "qn": {
                    "durable": True,
                    "auto_delete": False,
                    "exclusive": False,
                    "arguments": {},
                }
            },
        )
        self.assertEqual(0, result.exit_code)

    @mock.patch("fedora_messaging.cli.api.consume")
    def test_no_cli_bindings(self, mock_consume):
        """Assert providing a bindings via configuration works."""
        expected_bindings = [{"exchange": "e", "queue": "q", "routing_keys": ["#"]}]

        result = self.runner.invoke(cli.cli, ["--conf=" + GOOD_CONF, "consume"])

        self.assertEqual(0, result.exit_code)
        mock_consume.assert_called_once_with(
            echo, bindings=expected_bindings, queues=config.conf["queues"]
        )

    @mock.patch("fedora_messaging.cli.api.consume")
    def test_queue_and_routing_key(self, mock_consume):
        """Assert providing improper bindings is reported."""
        config.conf["callback"] = "fedora_messaging.tests.unit.test_cli:echo"

        result = self.runner.invoke(
            cli.cli, ["consume", "--queue-name=qn", "--routing-key=rk"]
        )

        mock_consume.assert_called_once_with(
            echo,
            bindings=[
                {"exchange": "amq.topic", "queue": "qn", "routing_keys": ("rk",)}
            ],
            queues={
                "qn": {
                    "durable": True,
                    "auto_delete": False,
                    "exclusive": False,
                    "arguments": {},
                }
            },
        )
        self.assertEqual(0, result.exit_code)

    @mock.patch("fedora_messaging.cli.api.consume")
    def test_good_cli_callable(self, mock_consume):
        """Assert providing a callable via the CLI works."""
        result = self.runner.invoke(
            cli.cli, ["consume", "--callback=fedora_messaging.tests.unit.test_cli:echo"]
        )

        mock_consume.assert_called_once_with(
            echo, bindings=config.conf["bindings"], queues=config.conf["queues"]
        )
        self.assertEqual(0, result.exit_code)

    @mock.patch.dict("fedora_messaging.config.conf", {"bindings": "b", "queues": "c"})
    @mock.patch("fedora_messaging.cli.importlib")
    @mock.patch("fedora_messaging.cli.api.consume")
    def test_app_name(self, mock_consume, mock_importlib, *args, **kwargs):
        """Assert provided app name is saved in config."""
        cli_options = {"callback": "mod:callable", "app-name": "test_app_name"}
        mock_mod_with_callable = mock.Mock(spec=["callable"])
        mock_importlib.import_module.return_value = mock_mod_with_callable
        result = self.runner.invoke(
            cli.cli,
            [
                "consume",
                "--callback=" + cli_options["callback"],
                "--app-name=" + cli_options["app-name"],
            ],
        )
        self.assertEqual(
            config.conf["client_properties"]["app"], cli_options["app-name"]
        )
        mock_importlib.import_module.called_once_with("mod")
        mock_consume.assert_called_once_with(
            mock_mod_with_callable.callable, bindings="b", queues="c"
        )
        self.assertEqual(0, result.exit_code)

    @mock.patch.dict(
        "fedora_messaging.config.conf", {"bindings": "b", "callback": None}
    )
    @mock.patch("fedora_messaging.cli.importlib")
    @mock.patch("fedora_messaging.cli.api.consume")
    def test_missing_cli_and_conf_callable(self, mock_consume, mock_importlib):
        """Assert missing callable via cli and in conf is reported."""
        mock_mod_with_callable = mock.Mock(spec=["callable"])
        mock_importlib.import_module.return_value = mock_mod_with_callable
        result = self.runner.invoke(cli.cli, ["consume"])
        mock_importlib.import_module.not_called()
        mock_consume.assert_not_called()
        self.assertIn(
            "A Python path to a callable object that accepts the message must be provided"
            ' with the "--callback" command line option or in the configuration file',
            result.output,
        )
        self.assertEqual(1, result.exit_code)

    @mock.patch.dict("fedora_messaging.config.conf", {"bindings": "b"})
    @mock.patch("fedora_messaging.cli.importlib")
    @mock.patch("fedora_messaging.cli.api.consume")
    def test_cli_callable_wrong_format(self, mock_consume, mock_importlib):
        """Assert a wrong callable format is reported."""
        cli_options = {"callback": "modcallable"}
        mock_mod_with_callable = mock.Mock(spec=["callable"])
        mock_importlib.import_module.return_value = mock_mod_with_callable
        result = self.runner.invoke(
            cli.cli, ["consume", "--callback=" + cli_options["callback"]]
        )
        mock_importlib.import_module.not_called()
        mock_consume.assert_not_called()
        self.assertIn(
            "Unable to parse the callback path ({}); the "
            'expected format is "my_package.module:'
            'callable_object"'.format(cli_options["callback"]),
            result.output,
        )
        self.assertEqual(1, result.exit_code)

    @mock.patch.dict("fedora_messaging.config.conf", {"bindings": "b"})
    @mock.patch("fedora_messaging.cli.importlib")
    @mock.patch("fedora_messaging.cli.api.consume")
    def test_cli_callable_import_failure_cli_opt(self, mock_consume, mock_importlib):
        """Assert module with callable import failure is reported."""
        cli_options = {"callback": "mod:callable"}
        error_message = "No module named 'mod'"
        mock_importlib.import_module.side_effect = ImportError(error_message)
        result = self.runner.invoke(
            cli.cli, ["consume", "--callback=" + cli_options["callback"]]
        )
        mock_importlib.import_module.called_once_with("mod")
        mock_consume.assert_not_called()
        self.assertIn(
            "Failed to import the callback module ({}) provided in the --callback argument".format(
                error_message
            ),
            result.output,
        )
        self.assertEqual(1, result.exit_code)

    def test_cli_callable_import_failure_conf(self):
        """Assert module with callable import failure is reported."""
        config.conf["callback"] = "donotmakethismoduleorthetestbreaks:function"

        result = self.runner.invoke(cli.cli, ["consume"])

        # Python 2 import exceptions print differently, so break this assert up :(
        self.assertIn("Failed to import the callback module", result.output)
        self.assertIn("donotmakethismoduleorthetestbreaks", result.output)
        self.assertIn("provided in the configuration file", result.output)
        self.assertEqual(1, result.exit_code)

    @mock.patch("fedora_messaging.cli.getattr")
    @mock.patch.dict("fedora_messaging.config.conf", {"bindings": "b"})
    @mock.patch("fedora_messaging.cli.importlib")
    @mock.patch("fedora_messaging.cli.api.consume")
    def test_callable_getattr_failure(self, mock_consume, mock_importlib, mock_getattr):
        """Assert finding callable in module failure is reported."""
        cli_options = {"callback": "mod:callable"}
        error_message = "module 'mod' has no attribute 'callable'"
        mock_mod_with_callable = mock.Mock(spec=["callable"])
        mock_importlib.import_module.return_value = mock_mod_with_callable
        mock_getattr.side_effect = AttributeError(error_message)
        result = self.runner.invoke(
            cli.cli, ["consume", "--callback=" + cli_options["callback"]]
        )
        mock_importlib.import_module.called_once_with("mod")
        mock_consume.assert_not_called()
        self.assertIn(
            "Unable to import {} ({}); is the package installed? The python path should "
            'be in the format "my_package.module:callable_object"'.format(
                cli_options["callback"], error_message
            ),
            result.output,
        )
        self.assertEqual(1, result.exit_code)

    def test_consume_improper_callback_object(self):
        """Assert improper callback object type failure is reported."""
        error_message = (
            "Callback must be a class that implements __call__ or a function."
        )

        result = self.runner.invoke(
            cli.cli,
            ["consume", "--callback=fedora_messaging.tests.unit.test_cli:FIXTURES_DIR"],
        )

        self.assertIn(error_message, result.output)
        self.assertEqual(2, result.exit_code)

    @mock.patch("fedora_messaging.cli.importlib")
    @mock.patch("fedora_messaging.cli.api.consume")
    def test_consume_halt_with_exitcode(self, mock_consume, mock_importlib):
        """Assert user execution halt with reason and exit_code is reported."""
        halt_message = "User halted execution"
        halt_exit_code = 5
        mock_mod_with_callable = mock.Mock(spec=["callable"])
        mock_importlib.import_module.return_value = mock_mod_with_callable
        mock_consume.side_effect = exceptions.HaltConsumer(
            exit_code=halt_exit_code, reason=halt_message
        )

        with mock.patch("fedora_messaging.cli._log") as mock_log:
            result = self.runner.invoke(cli.cli, ["consume", "--callback=mod:callable"])

        mock_consume.assert_called_once_with(
            mock_mod_with_callable.callable,
            bindings=config.conf["bindings"],
            queues=config.conf["queues"],
        )
        mock_log.error.assert_called_once_with(
            "Consumer halted with non-zero exit code (%d): %s",
            5,
            "User halted execution",
        )
        self.assertEqual(halt_exit_code, result.exit_code)

    @mock.patch("fedora_messaging.cli.importlib")
    @mock.patch("fedora_messaging.cli.api.consume")
    def test_consume_halt_without_exitcode(self, mock_consume, mock_importlib):
        """Assert user execution halt is reported."""
        mock_mod_with_callable = mock.Mock(spec=["callable"])
        mock_importlib.import_module.return_value = mock_mod_with_callable
        mock_consume.side_effect = exceptions.HaltConsumer(exit_code=0)

        result = self.runner.invoke(cli.cli, ["consume", "--callback=mod:callable"])

        mock_consume.assert_called_once_with(
            mock_mod_with_callable.callable,
            bindings=config.conf["bindings"],
            queues=config.conf["queues"],
        )
        self.assertEqual(0, result.exit_code)
