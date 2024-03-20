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


import errno
import os
import sys
from unittest import mock

import click
import pytest
import requests
from click.testing import CliRunner
from twisted.internet import error
from twisted.python import failure

from fedora_messaging import cli, config, exceptions, message, testing
from fedora_messaging.twisted import consumer


@pytest.fixture
def good_conf(fixtures_dir):
    return os.path.join(fixtures_dir, "good_conf.toml")


@pytest.fixture
def good_msg_dump(fixtures_dir):
    return os.path.join(fixtures_dir, "good_msg_dump.txt")


def echo(message):
    """Echo the message received to standard output."""
    print(str(message))


plain_object = object()


@mock.patch("fedora_messaging.config.conf.setup_logging", mock.Mock())
class TestBaseCli:
    """Unit tests for the base command of the CLI."""

    def test_no_conf(self):
        """Assert the CLI runs without exploding."""
        runner = CliRunner()
        result = runner.invoke(cli.cli)
        assert result.exit_code == 0


@mock.patch("fedora_messaging.cli.reactor", mock.Mock())
@mock.patch("fedora_messaging.config.conf.setup_logging", mock.Mock())
class TestConsumeCli:
    """Unit tests for the 'consume' command of the CLI."""

    def setup_method(self, method):
        self.runner = CliRunner()

    def teardown_method(self, method):
        """Make sure each test has a fresh default configuration."""
        config.conf = config.LazyConfig()
        config.conf.load_config(config_path="")

    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_good_conf(self, mock_consume, good_conf):
        """Assert providing a configuration file via the CLI works."""
        result = self.runner.invoke(cli.cli, ["--conf=" + good_conf, "consume"])
        mock_consume.assert_called_with(
            echo,
            bindings=[{"exchange": "e", "queue": "q", "routing_keys": ["#"]}],
            queues=config.conf["queues"],
        )
        assert 0 == result.exit_code

    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_conf_env_support(self, mock_consume, good_conf):
        """Assert FEDORA_MESSAGING_CONF environment variable is supported."""
        result = self.runner.invoke(
            cli.cli, ["consume"], env={"FEDORA_MESSAGING_CONF": good_conf}
        )
        mock_consume.assert_called_with(
            echo,
            bindings=[{"exchange": "e", "queue": "q", "routing_keys": ["#"]}],
            queues=config.conf["queues"],
        )
        assert 0 == result.exit_code

    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_bad_conf(self, mock_consume, fixtures_dir):
        """Assert a bad configuration file is reported."""
        BAD_CONF = os.path.join(fixtures_dir, "bad_conf.toml")
        expected_err = (
            "Error: Invalid value: Configuration error: Failed to parse"
            " {}: Invalid value (at line 1, column 20)".format(BAD_CONF)
        )
        result = self.runner.invoke(cli.cli, ["--conf=" + BAD_CONF, "consume"])
        assert 2 == result.exit_code
        assert expected_err in result.output

    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_missing_conf(self, mock_consume):
        """Assert a missing configuration file is reported."""
        result = self.runner.invoke(cli.cli, ["--conf=thispathdoesnotexist", "consume"])
        assert 2 == result.exit_code
        assert (
            "Error: Invalid value: thispathdoesnotexist is not a file" in result.output
        )

    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_good_cli_bindings(self, mock_consume):
        """Assert providing a bindings via the CLI works."""
        config.conf["callback"] = "tests.unit.test_cli:echo"

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
                    "durable": False,
                    "auto_delete": True,
                    "exclusive": True,
                    "arguments": {},
                }
            },
        )
        assert 0 == result.exit_code

    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_no_cli_bindings(self, mock_consume, good_conf):
        """Assert providing a bindings via configuration works."""
        expected_bindings = [{"exchange": "e", "queue": "q", "routing_keys": ["#"]}]

        result = self.runner.invoke(cli.cli, ["--conf=" + good_conf, "consume"])

        assert 0 == result.exit_code
        mock_consume.assert_called_once_with(
            echo, bindings=expected_bindings, queues=config.conf["queues"]
        )

    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_queue_and_routing_key(self, mock_consume):
        """Asser  providing improper bindings is reported."""
        config.conf["callback"] = "tests.unit.test_cli:echo"

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
                    "durable": False,
                    "auto_delete": True,
                    "exclusive": True,
                    "arguments": {},
                }
            },
        )
        assert 0 == result.exit_code

    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_good_cli_callable(self, mock_consume):
        """Assert providing a callable via the CLI works."""
        result = self.runner.invoke(
            cli.cli, ["consume", "--callback=tests.unit.test_cli:echo"]
        )

        mock_consume.assert_called_once_with(
            echo, bindings=config.conf["bindings"], queues=config.conf["queues"]
        )
        assert 0 == result.exit_code

    @mock.patch("fedora_messaging.cli.importlib")
    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_app_name(self, mock_consume, mock_importlib, monkeypatch):
        """Assert provided app name is saved in config."""
        monkeypatch.setitem(config.conf, "bindings", "b")
        monkeypatch.setitem(config.conf, "queues", "c")
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
        assert config.conf["client_properties"]["app"] == cli_options["app-name"]
        mock_importlib.import_module.assert_called_once_with("mod")
        mock_consume.assert_called_once_with(
            mock_mod_with_callable.callable, bindings="b", queues="c"
        )
        assert 0 == result.exit_code

    @mock.patch("fedora_messaging.cli.importlib")
    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_missing_cli_and_conf_callable(
        self, mock_consume, mock_importlib, monkeypatch
    ):
        """Assert missing callable via cli and in conf is reported."""
        monkeypatch.setitem(config.conf, "bindings", "b")
        monkeypatch.setitem(config.conf, "callback", None)
        mock_mod_with_callable = mock.Mock(spec=["callable"])
        mock_importlib.import_module.return_value = mock_mod_with_callable
        result = self.runner.invoke(cli.cli, ["consume"])
        mock_importlib.import_module.assert_not_called()
        mock_consume.assert_not_called()
        assert (
            "A Python path to a callable object that accepts the message must be provided"
            ' with the "--callback" command line option or in the configuration file'
            in result.output
        )
        assert 1 == result.exit_code

    @mock.patch("fedora_messaging.cli.importlib")
    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_cli_callable_wrong_format(self, mock_consume, mock_importlib, monkeypatch):
        """Assert a wrong callable format is reported."""
        cli_options = {"callback": "modcallable"}
        mock_mod_with_callable = mock.Mock(spec=["callable"])
        mock_importlib.import_module.return_value = mock_mod_with_callable
        result = self.runner.invoke(
            cli.cli, ["consume", "--callback=" + cli_options["callback"]]
        )
        mock_importlib.import_module.assert_not_called()
        mock_consume.assert_not_called()
        assert (
            "Unable to parse the callback path ({}); the "
            'expected format is "my_package.module:'
            'callable_object"'.format(cli_options["callback"]) in result.output
        )
        assert 1 == result.exit_code

    @mock.patch("fedora_messaging.cli.importlib")
    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_cli_callable_import_failure_cli_opt(
        self, mock_consume, mock_importlib, monkeypatch
    ):
        """Assert module with callable import failure is reported."""
        monkeypatch.setitem(config.conf, "bindings", "b")
        cli_options = {"callback": "mod:callable"}
        error_message = "No module named 'mod'"
        mock_importlib.import_module.side_effect = ImportError(error_message)
        result = self.runner.invoke(
            cli.cli, ["consume", "--callback=" + cli_options["callback"]]
        )
        mock_importlib.import_module.assert_called_once_with("mod")
        mock_consume.assert_not_called()
        assert (
            "Failed to import the callback module ({}) provided in the --callback argument".format(
                error_message
            )
            in result.output
        )
        assert 1 == result.exit_code

    def test_cli_callable_import_failure_conf(self):
        """Assert module with callable import failure is reported."""
        config.conf["callback"] = "donotmakethismoduleorthetestbreaks:function"

        result = self.runner.invoke(cli.cli, ["consume"])

        assert (
            result.output == "Error: Failed to import the callback module "
            "(No module named 'donotmakethismoduleorthetestbreaks') "
            "provided in the configuration file\n"
        )
        assert 1 == result.exit_code

    @mock.patch("fedora_messaging.cli.getattr")
    @mock.patch("fedora_messaging.cli.importlib")
    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_callable_getattr_failure(
        self, mock_consume, mock_importlib, mock_getattr, monkeypatch
    ):
        """Assert finding callable in module failure is reported."""
        monkeypatch.setitem(config.conf, "bindings", "b")
        cli_options = {"callback": "mod:callable"}
        error_message = "module 'mod' has no attribute 'callable'"
        mock_mod_with_callable = mock.Mock(spec=["callable"])
        mock_importlib.import_module.return_value = mock_mod_with_callable
        mock_getattr.side_effect = AttributeError(error_message)
        result = self.runner.invoke(
            cli.cli, ["consume", "--callback=" + cli_options["callback"]]
        )
        mock_importlib.import_module.assert_called_once_with("mod")
        mock_consume.assert_not_called()
        assert (
            "Unable to import {} ({}); is the package installed? The python path should "
            'be in the format "my_package.module:callable_object"'.format(
                cli_options["callback"], error_message
            )
            in result.output
        )
        assert 1 == result.exit_code

    def test_consume_improper_callback_object(self):
        """Assert improper callback object type failure is reported."""
        error_message = (
            "Callback must be a class that implements __call__ or a function."
        )

        result = self.runner.invoke(
            cli.cli,
            ["consume", "--callback=tests.unit.test_cli:plain_object"],
        )

        assert error_message in result.output
        assert 2 == result.exit_code


@mock.patch("fedora_messaging.cli.reactor")
class TestConsumeCallback:
    """Unit tests for the twisted_consume callback."""

    def test_callback(self, mock_reactor):
        """Assert when the last consumer calls back, the reactor stops."""
        consumers = (consumer.Consumer(), consumer.Consumer())
        cli._consume_callback(consumers)

        consumers[0].result.callback(consumers[0])
        assert 0 == mock_reactor.stop.call_count
        consumers[1].result.callback(consumers[1])
        assert 1 == mock_reactor.stop.call_count

    def test_callback_reactor_stopped(self, mock_reactor):
        """Assert already-stopped reactor is handled."""
        consumers = (consumer.Consumer(), consumer.Consumer())
        mock_reactor.stop.side_effect = error.ReactorNotRunning()

        cli._consume_callback(consumers)
        try:
            consumers[0].result.callback(consumers[0])
            consumers[1].result.callback(consumers[1])
            assert 1 == mock_reactor.stop.call_count
        except error.ReactorNotRunning:
            pytest.fail("ReactorNotRunning exception wasn't handled.")

    def test_errback_halt_consumer(self, mock_reactor):
        """Assert _exit_code is set with the HaltConsumer code."""
        consumers = (consumer.Consumer(),)
        e = exceptions.HaltConsumer()
        f = failure.Failure(e, exceptions.HaltConsumer)
        cli._consume_callback(consumers)

        consumers[0].result.errback(f)
        assert 1 == mock_reactor.stop.call_count
        assert 0 == cli._exit_code

    @mock.patch("fedora_messaging.cli._log")
    def test_errback_halt_consumer_nonzero(self, mock_log, mock_reactor):
        """Assert _exit_code is set with the HaltConsumer code and logged if non-zero"""
        consumers = (consumer.Consumer(),)
        e = exceptions.HaltConsumer(exit_code=42)
        f = failure.Failure(e, exceptions.HaltConsumer)
        cli._consume_callback(consumers)

        consumers[0].result.errback(f)
        assert 1 == mock_reactor.stop.call_count
        assert 42 == cli._exit_code
        mock_log.error.assert_called_with(
            "Consumer halted with non-zero exit code (%d): %s", 42, "None"
        )

    def test_errback_canceled(self, mock_reactor):
        """Assert exit code is 2 when the consumer is canceled."""
        consumers = (consumer.Consumer(),)
        e = exceptions.ConsumerCanceled()
        f = failure.Failure(e, exceptions.ConsumerCanceled)
        cli._consume_callback(consumers)

        consumers[0].result.errback(f)
        assert 1 == mock_reactor.stop.call_count
        assert 12 == cli._exit_code

    @mock.patch("fedora_messaging.cli._log")
    def test_errback_general_exception(self, mock_log, mock_reactor):
        """Assert exit code is 1 when an unexpected error occurs."""
        consumers = (consumer.Consumer(),)
        e = Exception("boom")
        f = failure.Failure(e, Exception)
        cli._consume_callback(consumers)

        consumers[0].result.errback(f)
        assert 1 == mock_reactor.stop.call_count
        assert 13 == cli._exit_code
        mock_log.error.assert_called_once_with(
            "Unexpected error occurred in consumer %r: %r", consumers[0], f
        )


class TestConsumeErrback:
    """Unit tests for the twisted_consume errback."""

    def setup_method(self, method):
        cli._exit_code = 0

    def teardown_method(self, method):
        cli._exit_code = 0

    @mock.patch("fedora_messaging.cli.reactor")
    def test_errback_permission(self, mock_reactor):
        """Assert permission exceptions are caught and exit with 15."""
        f = failure.Failure(exceptions.PermissionException("queue", "boop", "none"))

        cli._consume_errback(f)

        assert 15 == cli._exit_code

    @mock.patch("fedora_messaging.cli.reactor")
    def test_errback_bad_declaration(self, mock_reactor):
        """Assert declaration exceptions are caught and exit with 10."""
        f = failure.Failure(exceptions.BadDeclaration("queue", "boop", "none"))

        cli._consume_errback(f)

        assert 10 == cli._exit_code

    @mock.patch("fedora_messaging.cli.reactor")
    def test_errback_connection_exception(self, mock_reactor):
        """Assert connection exceptions are caught and exit with 14."""
        f = failure.Failure(exceptions.ConnectionException(reason="eh"))

        cli._consume_errback(f)

        assert 14 == cli._exit_code

    @mock.patch("fedora_messaging.cli.reactor")
    def test_errback_general_exception(self, mock_reactor):
        """Assert general exceptions are caught and exit with 11."""
        f = failure.Failure(Exception("boop"))

        cli._consume_errback(f)

        assert 11 == cli._exit_code


class CallbackFromFilesytem:
    """Unit tests for :func:`fedora_messaging.cli._callback_from_filesystem`."""

    def test_good_callback(self, fixtures_dir):
        """Assert loading a callback from a file works."""
        cb = cli._callback_from_filesystem(
            os.path.join(fixtures_dir, "callback.py") + ":rand"
        )
        assert 4 == cb(None)

    def test_bad_format(self):
        """Assert an exception is raised if the format is bad."""
        with pytest.raises(click.ClickException) as cm:
            cli._callback_from_filesystem("file/with/no/function.py")

        assert (
            "Unable to parse the '--callback-file' option; the "
            'expected format is "path/to/file.py:callable_object" where '
            '"callable_object" is the name of the function or class in the '
            "Python file" == cm.value.message
        )

    def test_invalid_file(self, fixtures_dir):
        """Assert an exception is raised if the Python file can't be executed."""
        with pytest.raises(click.ClickException) as cm:
            cli._callback_from_filesystem(
                os.path.join(fixtures_dir, "bad_cb") + ":missing"
            )

        if sys.version_info >= (3, 10) and sys.version_info < (3, 10, 4):
            # https://github.com/python/cpython/issues/90398
            exc_msg = "invalid syntax. Perhaps you forgot a comma?"
        else:
            exc_msg = "invalid syntax"
        assert (
            "The {} file raised the following exception during execution: "
            "{} (bad_cb, line 1)".format(
                os.path.join(fixtures_dir, "bad_cb"),
                exc_msg,
            )
            == cm.value.message
        )

    def test_callable_does_not_exist(self, fixtures_dir):
        """Assert an exception is raised if the callable is missing."""
        with pytest.raises(click.ClickException) as cm:
            cli._callback_from_filesystem(
                os.path.join(fixtures_dir, "callback.py") + ":missing"
            )

        assert (
            "The 'missing' object was not found in the '{}' file."
            "".format(os.path.join(fixtures_dir, "callback.py")) == cm.value.message
        )

    def test_file_does_not_exist(self):
        """Assert an exception is raised if the file doesn't exist."""
        with pytest.raises(click.ClickException) as cm:
            cli._callback_from_filesystem("file/that/is/missing.py:callable")

        assert (
            "An IO error occurred: [Errno 2] No such file or directory: 'file/that/is/missing.py'"
            == cm.value.message
        )


@mock.patch("fedora_messaging.config.conf.setup_logging", mock.Mock())
class TestPublishCli:
    """Unit tests for the 'publish' command of the CLI."""

    def setup_method(self, method):
        self.runner = CliRunner()

    def teardown_method(self, method):
        """Make sure each test has a fresh default configuration."""
        config.conf = config.LazyConfig()
        config.conf.load_config()

    def test_correct_msg_in_file(self, good_conf, good_msg_dump):
        """Assert providing path to file with correct message via the CLI works."""
        cli_options = {"file": good_msg_dump, "exchange": "test_pe"}
        expected_msg = message.Message(
            body={"test_key1": "test_value1"}, topic="test_topic", severity=message.INFO
        )

        with testing.mock_sends(expected_msg):
            result = self.runner.invoke(
                cli.cli,
                [
                    "--conf=" + good_conf,
                    "publish",
                    "--exchange=" + cli_options["exchange"],
                    cli_options["file"],
                ],
            )
        assert "Publishing message with topic test_topic" in result.output
        assert 0 == result.exit_code

    @mock.patch("fedora_messaging.cli.api.publish")
    def test_file_with_corrupted_json(self, mock_publish, fixtures_dir, good_conf):
        """Assert providing path to file with corrupted message json via the CLI works."""
        wrong_json_msg_dump = os.path.join(fixtures_dir, "wrong_json_msg_dump.txt")
        cli_options = {"file": wrong_json_msg_dump, "exchange": "test_pe"}
        result = self.runner.invoke(
            cli.cli,
            [
                "--conf=" + good_conf,
                "publish",
                "--exchange=" + cli_options["exchange"],
                cli_options["file"],
            ],
        )
        assert "Error: Unable to validate message:" in result.output
        mock_publish.assert_not_called()
        assert 2 == result.exit_code

    @mock.patch("fedora_messaging.cli.api.publish")
    def test_file_with_msg_without_topic(self, mock_publish, fixtures_dir, good_conf):
        """Assert providing path to file with incorrect message via the CLI works."""
        msg_without_topic_dump = os.path.join(
            fixtures_dir, "msg_without_topic_dump.txt"
        )
        cli_options = {"file": msg_without_topic_dump, "exchange": "test_pe"}
        result = self.runner.invoke(
            cli.cli,
            [
                "--conf=" + good_conf,
                "publish",
                "--exchange=" + cli_options["exchange"],
                cli_options["file"],
            ],
        )
        assert (
            "Error: Unable to validate message: 'topic' is a required property"
            in result.output
        )
        mock_publish.assert_not_called()
        assert 2 == result.exit_code

    @mock.patch("fedora_messaging.cli.api.publish")
    def test_file_with_invalid_msg(self, mock_publish, fixtures_dir, good_conf):
        """Assert providing path to file with incorrect message via the CLI works."""
        invalid_msg_dump = os.path.join(fixtures_dir, "invalid_msg_dump.txt")
        cli_options = {"file": invalid_msg_dump, "exchange": "test_pe"}
        result = self.runner.invoke(
            cli.cli,
            [
                "--conf=" + good_conf,
                "publish",
                "--exchange=" + cli_options["exchange"],
                cli_options["file"],
            ],
        )
        assert (
            "Error: Unable to validate message: [] is not of type 'object'"
            in result.output
        )
        mock_publish.assert_not_called()
        assert 2 == result.exit_code

    @mock.patch("fedora_messaging.cli.api.publish")
    def test_publish_rejected_message(self, mock_publish, good_conf, good_msg_dump):
        """Assert a rejected message is reported."""
        cli_options = {"file": good_msg_dump, "exchange": "test_pe"}
        error_message = "Message rejected"
        mock_publish.side_effect = exceptions.PublishReturned(error_message)
        result = self.runner.invoke(
            cli.cli,
            [
                "--conf=" + good_conf,
                "publish",
                "--exchange=" + cli_options["exchange"],
                cli_options["file"],
            ],
        )
        assert ("Unable to publish message: " + error_message) in result.output
        mock_publish.assert_called_once()
        assert errno.EREMOTEIO == result.exit_code

    @mock.patch("fedora_messaging.cli.api.publish")
    def test_publish_connection_failed(self, mock_publish, good_conf, good_msg_dump):
        """Assert a connection problem is reported."""
        cli_options = {"file": good_msg_dump, "exchange": "test_pe"}
        mock_publish.side_effect = exceptions.PublishTimeout(reason="timeout")
        result = self.runner.invoke(
            cli.cli,
            [
                "--conf=" + good_conf,
                "publish",
                "--exchange=" + cli_options["exchange"],
                cli_options["file"],
            ],
        )
        assert "Unable to connect to the message broker: timeout" in result.output
        mock_publish.assert_called_once()
        assert errno.ECONNREFUSED == result.exit_code

    @mock.patch("fedora_messaging.cli.api.publish")
    def test_publish_general_publish_error(
        self, mock_publish, good_conf, good_msg_dump
    ):
        """Assert a connection problem is reported."""
        cli_options = {"file": good_msg_dump, "exchange": "test_pe"}
        mock_publish.side_effect = exceptions.PublishException(reason="eh")
        result = self.runner.invoke(
            cli.cli,
            [
                "--conf=" + good_conf,
                "publish",
                "--exchange=" + cli_options["exchange"],
                cli_options["file"],
            ],
        )
        assert "A general publish exception occurred: eh" in result.output
        mock_publish.assert_called_once()
        assert 1 == result.exit_code


@mock.patch("fedora_messaging.config.conf.setup_logging", mock.Mock())
class TestRecordCli:
    """Unit tests for the 'record' command of the CLI."""

    def setup_method(self, method):
        self.runner = CliRunner()

    def teardown_method(self, method):
        """Make sure each test has a fresh default configuration."""
        config.conf = config.LazyConfig()
        config.conf.load_config(config_path="")

    @mock.patch("fedora_messaging.cli._consume")
    def test_good_cli_bindings(self, mock_consume):
        """Assert arguments are forwarded to the _consume function."""
        cli_options = {
            "file": "test_file.txt",
            "exchange": "e",
            "queue-name": "qn",
            "routing-keys": ("rk1", "rk2"),
        }
        self.runner.invoke(
            cli.cli,
            [
                "record",
                cli_options["file"],
                "--exchange=" + cli_options["exchange"],
                "--queue-name=" + cli_options["queue-name"],
                "--routing-key=" + cli_options["routing-keys"][0],
                "--routing-key=" + cli_options["routing-keys"][1],
            ],
        )
        mock_consume.assert_called_once_with(
            "e", "qn", ("rk1", "rk2"), mock.ANY, "recorder"
        )


class TestRecorderClass:
    """Unit tests for the 'Recorder' class."""

    def test_save_recorded_messages_when_limit_is_reached(self):
        """Assert that collected messages are saved to file when limit is reached."""
        msg1 = message.Message(
            body={"test_key1": "test_value1"},
            topic="test_topic1",
            severity=message.INFO,
        )
        msg1._properties.headers["sent-at"] = "2018-11-18T10:11:41+00:00"
        msg1.id = "273ed91d-b8b5-487a-9576-95b9fbdf3eec"

        msg2 = message.Message(
            body={"test_key2": "test_value2"},
            topic="test_topic2",
            severity=message.INFO,
        )
        msg2._properties.headers["sent-at"] = "2018-11-18T10:11:41+00:00"
        msg2.id = "273ed91d-b8b5-487a-9576-95b9fbdf3eec"

        mock_file = mock.MagicMock()
        test_recorder = cli.Recorder(2, mock_file)
        test_recorder.collect_message(msg1)
        mock_file.write.assert_called_with(
            '{"body": {"test_key1": "test_value1"}, "headers"'
            ': {"fedora_messaging_schema": "base.message", "fedora_messaging_severity": 20, '
            '"priority": 0, "sent-at": "2018-11-18T10:11:41+00:00"}, '
            '"id": "273ed91d-b8b5-487a-9576-95b9fbdf3eec", '
            '"priority": 0, "queue": null, "topic": "test_topic1"}\n'
        )

        with pytest.raises(exceptions.HaltConsumer) as cm:
            test_recorder.collect_message(msg2)
        the_exception = cm.value
        assert the_exception.exit_code == 0
        assert test_recorder.counter == 2
        mock_file.write.assert_called_with(
            '{"body": {"test_key2": "test_value2"}, "headers": '
            '{"fedora_messaging_schema": "base.message", "fedora_messaging_severity": '
            '20, "priority": 0, "sent-at": "2018-11-18T10:11:41+00:00"}, "id": '
            '"273ed91d-b8b5-487a-9576-95b9fbdf3eec", "priority": 0, "queue": null, '
            '"topic": "test_topic2"}\n'
        )

    def test_recorded_messages_dumps_failed(self):
        """Assert that attempt to save improper recorded message is reported."""
        mock_file = mock.MagicMock()
        test_recorder = cli.Recorder(1, mock_file)
        with pytest.raises(exceptions.HaltConsumer) as cm:
            test_recorder.collect_message("msg1")
        the_exception = cm.value
        assert the_exception.exit_code == 1
        assert test_recorder.counter == 0
        mock_file.write.assert_not_called()


@mock.patch("fedora_messaging.config.conf.setup_logging", mock.Mock())
class TestReplayCli:
    """Unit tests for the 'replay' command of the CLI."""

    def setup_method(self, method):
        """Setup test method environment."""
        self.runner = CliRunner()

    def teardown_method(self, method):
        """Reset configuration after each test."""
        config.conf = config.LazyConfig()
        config.conf.load_config()

    @mock.patch("fedora_messaging.cli._get_message")
    @mock.patch("fedora_messaging.api.publish")
    def test_successful_message_replay(self, mock_publish, mock_get_message):
        """Test successful replay of a message."""
        message_id = "123"
        datagrepper_url = "http://example.com"

        message_data = {
            "topic": "test.topic",
            "body": {"data_key": "data_value"},
        }
        mock_get_message.return_value = message_data

        result = self.runner.invoke(
            cli.replay, [message_id, f"--datagrepper-url={datagrepper_url}"]
        )

        assert (
            result.exit_code == 0
        ), f"Command did not exit as expected. Output: {result.output}"
        assert "has been successfully replayed" in result.output

    @mock.patch("fedora_messaging.cli.requests.get", side_effect=requests.HTTPError)
    def test_datagrepper_http_error(self, mock_get):
        """Test handling of HTTP errors when fetching message data."""
        message_id = "123"
        result = self.runner.invoke(cli.replay, [message_id])
        assert "Failed to retrieve message from Datagrepper" in result.output
        assert result.exit_code != 0

    @mock.patch("fedora_messaging.cli._get_message", return_value={"some": "data"})
    @mock.patch(
        "fedora_messaging.cli.api.publish", side_effect=Exception("Publish failure")
    )
    def test_publish_failure(self, mock_publish, mock_get_message):
        """Test handling of exceptions during message publishing."""
        message_id = "123"
        result = self.runner.invoke(cli.replay, [message_id])
        assert result.exit_code != 0
