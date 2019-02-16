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
from __future__ import absolute_import

import os
import unittest
import errno

from click.testing import CliRunner
from twisted.internet import error
from twisted.python import failure
import click
import mock

from fedora_messaging import cli, config, exceptions, message, testing
from fedora_messaging.tests import FIXTURES_DIR
from fedora_messaging.twisted import consumer

GOOD_CONF = os.path.join(FIXTURES_DIR, "good_conf.toml")
BAD_CONF = os.path.join(FIXTURES_DIR, "bad_conf.toml")
EMPTY_FILE = os.path.join(FIXTURES_DIR, "empty.txt")
GOOD_MSG_DUMP = os.path.join(FIXTURES_DIR, "good_msg_dump.txt")
WRONG_JSON_MSG_DUMP = os.path.join(FIXTURES_DIR, "wrong_json_msg_dump.txt")
MSG_WITHOUT_ID_DUMP = os.path.join(FIXTURES_DIR, "msg_without_id_dump.txt")
INVALID_MSG_DUMP = os.path.join(FIXTURES_DIR, "invalid_msg_dump.txt")


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


@mock.patch("fedora_messaging.cli.reactor", mock.Mock())
class ConsumeCliTests(unittest.TestCase):
    """Unit tests for the 'consume' command of the CLI."""

    def setUp(self):
        self.runner = CliRunner()

    def tearDown(self):
        """Make sure each test has a fresh default configuration."""
        config.conf = config.LazyConfig()
        config.conf.load_config(config_path="")

    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_good_conf(self, mock_consume):
        """Assert providing a configuration file via the CLI works."""
        result = self.runner.invoke(cli.cli, ["--conf=" + GOOD_CONF, "consume"])
        mock_consume.assert_called_with(
            echo,
            bindings=[{"exchange": "e", "queue": "q", "routing_keys": ["#"]}],
            queues=config.conf["queues"],
        )
        self.assertEqual(0, result.exit_code)

    @mock.patch("fedora_messaging.cli.api.twisted_consume")
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

    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_bad_conf(self, mock_consume):
        """Assert a bad configuration file is reported."""
        expected_err = (
            "Error: Invalid value: Configuration error: Failed to parse"
            " {}: error at line 1, column 1".format(BAD_CONF)
        )
        result = self.runner.invoke(cli.cli, ["--conf=" + BAD_CONF, "consume"])
        self.assertEqual(2, result.exit_code)
        self.assertIn(expected_err, result.output)

    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_missing_conf(self, mock_consume):
        """Assert a missing configuration file is reported."""
        result = self.runner.invoke(cli.cli, ["--conf=thispathdoesnotexist", "consume"])
        self.assertEqual(2, result.exit_code)
        self.assertIn(
            "Error: Invalid value: thispathdoesnotexist is not a file", result.output
        )

    @mock.patch("fedora_messaging.cli.api.twisted_consume")
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

    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_no_cli_bindings(self, mock_consume):
        """Assert providing a bindings via configuration works."""
        expected_bindings = [{"exchange": "e", "queue": "q", "routing_keys": ["#"]}]

        result = self.runner.invoke(cli.cli, ["--conf=" + GOOD_CONF, "consume"])

        self.assertEqual(0, result.exit_code)
        mock_consume.assert_called_once_with(
            echo, bindings=expected_bindings, queues=config.conf["queues"]
        )

    @mock.patch("fedora_messaging.cli.api.twisted_consume")
    def test_queue_and_routing_key(self, mock_consume):
        """Asser  providing improper bindings is reported."""
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

    @mock.patch("fedora_messaging.cli.api.twisted_consume")
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
    @mock.patch("fedora_messaging.cli.api.twisted_consume")
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
    @mock.patch("fedora_messaging.cli.api.twisted_consume")
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
    @mock.patch("fedora_messaging.cli.api.twisted_consume")
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
    @mock.patch("fedora_messaging.cli.api.twisted_consume")
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
    @mock.patch("fedora_messaging.cli.api.twisted_consume")
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


@mock.patch("fedora_messaging.cli.reactor")
class ConsumeCallbackTests(unittest.TestCase):
    """Unit tests for the twisted_consume callback."""

    def test_callback(self, mock_reactor):
        """Assert when the last consumer calls back, the reactor stops."""
        consumers = (consumer.Consumer(), consumer.Consumer())
        cli._consume_callback(consumers)

        consumers[0].result.callback(consumers[0])
        self.assertEqual(0, mock_reactor.stop.call_count)
        consumers[1].result.callback(consumers[1])
        self.assertEqual(1, mock_reactor.stop.call_count)

    def test_callback_reactor_stopped(self, mock_reactor):
        """Assert already-stopped reactor is handled."""
        consumers = (consumer.Consumer(), consumer.Consumer())
        mock_reactor.stop.side_effect = error.ReactorNotRunning()

        cli._consume_callback(consumers)
        try:
            consumers[0].result.callback(consumers[0])
            consumers[1].result.callback(consumers[1])
            self.assertEqual(1, mock_reactor.stop.call_count)
        except error.ReactorNotRunning:
            self.fail("ReactorNotRunning exception wasn't handled.")

    def test_errback_halt_consumer(self, mock_reactor):
        """Assert _exit_code is set with the HaltConsumer code."""
        consumers = (consumer.Consumer(),)
        e = exceptions.HaltConsumer()
        f = failure.Failure(e, exceptions.HaltConsumer)
        cli._consume_callback(consumers)

        consumers[0].result.errback(f)
        self.assertEqual(1, mock_reactor.stop.call_count)
        self.assertEqual(0, cli._exit_code)

    @mock.patch("fedora_messaging.cli._log")
    def test_errback_halt_consumer_nonzero(self, mock_log, mock_reactor):
        """Assert _exit_code is set with the HaltConsumer code and logged if non-zero"""
        consumers = (consumer.Consumer(),)
        e = exceptions.HaltConsumer(exit_code=42)
        f = failure.Failure(e, exceptions.HaltConsumer)
        cli._consume_callback(consumers)

        consumers[0].result.errback(f)
        self.assertEqual(1, mock_reactor.stop.call_count)
        self.assertEqual(42, cli._exit_code)
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
        self.assertEqual(1, mock_reactor.stop.call_count)
        self.assertEqual(12, cli._exit_code)

    @mock.patch("fedora_messaging.cli._log")
    def test_errback_general_exception(self, mock_log, mock_reactor):
        """Assert exit code is 1 when an unexpected error occurs."""
        consumers = (consumer.Consumer(),)
        e = Exception("boom")
        f = failure.Failure(e, Exception)
        cli._consume_callback(consumers)

        consumers[0].result.errback(f)
        self.assertEqual(1, mock_reactor.stop.call_count)
        self.assertEqual(13, cli._exit_code)
        mock_log.error.assert_called_once_with(
            "Unexpected error occurred in consumer %r: %r", consumers[0], f
        )


class ConsumeErrbackTests(unittest.TestCase):
    """Unit tests for the twisted_consume errback."""

    def setUp(self):
        cli._exit_code = 0

    def tearDown(self):
        cli._exit_code = 0

    @mock.patch("fedora_messaging.cli.reactor")
    def test_errback_permission(self, mock_reactor):
        """Assert permission exceptions are caught and exit with 15."""
        f = failure.Failure(exceptions.PermissionException("queue", "boop", "none"))

        cli._consume_errback(f)

        self.assertEqual(15, cli._exit_code)

    @mock.patch("fedora_messaging.cli.reactor")
    def test_errback_bad_declaration(self, mock_reactor):
        """Assert declaration exceptions are caught and exit with 10."""
        f = failure.Failure(exceptions.BadDeclaration("queue", "boop", "none"))

        cli._consume_errback(f)

        self.assertEqual(10, cli._exit_code)

    @mock.patch("fedora_messaging.cli.reactor")
    def test_errback_connection_exception(self, mock_reactor):
        """Assert connection exceptions are caught and exit with 14."""
        f = failure.Failure(exceptions.ConnectionException(reason="eh"))

        cli._consume_errback(f)

        self.assertEqual(14, cli._exit_code)

    @mock.patch("fedora_messaging.cli.reactor")
    def test_errback_general_exception(self, mock_reactor):
        """Assert general exceptions are caught and exit with 11."""
        f = failure.Failure(Exception("boop"))

        cli._consume_errback(f)

        self.assertEqual(11, cli._exit_code)


class CallbackFromFilesytem(unittest.TestCase):
    """Unit tests for :func:`fedora_messaging.cli._callback_from_filesystem`."""

    def test_good_callback(self):
        """Assert loading a callback from a file works."""
        cb = cli._callback_from_filesystem(
            os.path.join(FIXTURES_DIR, "callback.py") + ":rand"
        )
        self.assertEqual(4, cb(None))

    def test_bad_format(self):
        """Assert an exception is raised if the format is bad."""
        with self.assertRaises(click.ClickException) as cm:
            cli._callback_from_filesystem("file/with/no/function.py")

        self.assertEqual(
            "Unable to parse the '--callback-file' option; the "
            'expected format is "path/to/file.py:callable_object" where '
            '"callable_object" is the name of the function or class in the '
            "Python file",
            cm.exception.message,
        )

    def test_invalid_file(self):
        """Assert an exception is raised if the Python file can't be executed."""
        with self.assertRaises(click.ClickException) as cm:
            cli._callback_from_filesystem(
                os.path.join(FIXTURES_DIR, "bad_cb") + ":missing"
            )

        self.assertEqual(
            "The {} file raised the following exception during execution: "
            "invalid syntax (bad_cb, line 1)".format(
                os.path.join(FIXTURES_DIR, "bad_cb")
            ),
            cm.exception.message,
        )

    def test_callable_does_not_exist(self):
        """Assert an exception is raised if the callable is missing."""
        with self.assertRaises(click.ClickException) as cm:
            cli._callback_from_filesystem(
                os.path.join(FIXTURES_DIR, "callback.py") + ":missing"
            )

        self.assertEqual(
            "The 'missing' object was not found in the '{}' file."
            "".format(os.path.join(FIXTURES_DIR, "callback.py")),
            cm.exception.message,
        )

    def test_file_does_not_exist(self):
        """Assert an exception is raised if the file doesn't exist."""
        with self.assertRaises(click.ClickException) as cm:
            cli._callback_from_filesystem("file/that/is/missing.py:callable")

        self.assertEqual(
            "An IO error occurred: [Errno 2] No such file or directory: 'file/that/is/missing.py'",
            cm.exception.message,
        )


class PublishCliTests(unittest.TestCase):
    """Unit tests for the 'publish' command of the CLI."""

    def setUp(self):
        self.runner = CliRunner()

    def tearDown(self):
        """Make sure each test has a fresh default configuration."""
        config.conf = config.LazyConfig()
        config.conf.load_config()

    def test_correct_msg_in_file(self):
        """Assert providing path to file with correct message via the CLI works."""
        cli_options = {"file": GOOD_MSG_DUMP, "exchange": "test_pe"}
        expected_msg = message.Message(
            body={"test_key1": "test_value1"}, topic="test_topic", severity=message.INFO
        )

        with testing.mock_sends(expected_msg):
            result = self.runner.invoke(
                cli.cli,
                [
                    "--conf=" + GOOD_CONF,
                    "publish",
                    "--exchange=" + cli_options["exchange"],
                    cli_options["file"],
                ],
            )
        self.assertIn("Publishing message with topic test_topic", result.output)
        self.assertEqual(0, result.exit_code)

    @mock.patch("fedora_messaging.cli.api.publish")
    def test_file_with_corrupted_json(self, mock_publish):
        """Assert providing path to file with corrupted message json via the CLI works."""
        cli_options = {"file": WRONG_JSON_MSG_DUMP, "exchange": "test_pe"}
        result = self.runner.invoke(
            cli.cli,
            [
                "--conf=" + GOOD_CONF,
                "publish",
                "--exchange=" + cli_options["exchange"],
                cli_options["file"],
            ],
        )
        self.assertIn("Error: Unable to validate message:", result.output)
        mock_publish.assert_not_called()
        self.assertEqual(2, result.exit_code)

    @mock.patch("fedora_messaging.cli.api.publish")
    def test_file_with_msg_without_id(self, mock_publish):
        """Assert providing path to file with incorrect message via the CLI works."""
        cli_options = {"file": MSG_WITHOUT_ID_DUMP, "exchange": "test_pe"}
        result = self.runner.invoke(
            cli.cli,
            [
                "--conf=" + GOOD_CONF,
                "publish",
                "--exchange=" + cli_options["exchange"],
                cli_options["file"],
            ],
        )
        self.assertIn(
            "Error: Unable to validate message: 'id' is a required property",
            result.output,
        )
        mock_publish.assert_not_called()
        self.assertEqual(2, result.exit_code)

    @mock.patch("fedora_messaging.cli.api.publish")
    def test_file_with_invalid_msg(self, mock_publish):
        """Assert providing path to file with incorrect message via the CLI works."""
        cli_options = {"file": INVALID_MSG_DUMP, "exchange": "test_pe"}
        result = self.runner.invoke(
            cli.cli,
            [
                "--conf=" + GOOD_CONF,
                "publish",
                "--exchange=" + cli_options["exchange"],
                cli_options["file"],
            ],
        )
        self.assertIn(
            "Error: Unable to validate message: [] is not of type 'object'",
            result.output,
        )
        mock_publish.assert_not_called()
        self.assertEqual(2, result.exit_code)

    @mock.patch("fedora_messaging.cli.api.publish")
    def test_publish_rejected_message(self, mock_publish):
        """Assert a rejected message is reported."""
        cli_options = {"file": GOOD_MSG_DUMP, "exchange": "test_pe"}
        error_message = "Message rejected"
        mock_publish.side_effect = exceptions.PublishReturned(error_message)
        result = self.runner.invoke(
            cli.cli,
            [
                "--conf=" + GOOD_CONF,
                "publish",
                "--exchange=" + cli_options["exchange"],
                cli_options["file"],
            ],
        )
        self.assertIn("Unable to publish message: " + error_message, result.output)
        mock_publish.assert_called_once()
        self.assertEqual(errno.EREMOTEIO, result.exit_code)

    @mock.patch("fedora_messaging.cli.api.publish")
    def test_publish_connection_failed(self, mock_publish):
        """Assert a connection problem is reported."""
        cli_options = {"file": GOOD_MSG_DUMP, "exchange": "test_pe"}
        mock_publish.side_effect = exceptions.PublishTimeout(reason="timeout")
        result = self.runner.invoke(
            cli.cli,
            [
                "--conf=" + GOOD_CONF,
                "publish",
                "--exchange=" + cli_options["exchange"],
                cli_options["file"],
            ],
        )
        self.assertIn("Unable to connect to the message broker: timeout", result.output)
        mock_publish.assert_called_once()
        self.assertEqual(errno.ECONNREFUSED, result.exit_code)

    @mock.patch("fedora_messaging.cli.api.publish")
    def test_publish_general_publish_error(self, mock_publish):
        """Assert a connection problem is reported."""
        cli_options = {"file": GOOD_MSG_DUMP, "exchange": "test_pe"}
        mock_publish.side_effect = exceptions.PublishException(reason="eh")
        result = self.runner.invoke(
            cli.cli,
            [
                "--conf=" + GOOD_CONF,
                "publish",
                "--exchange=" + cli_options["exchange"],
                cli_options["file"],
            ],
        )
        self.assertIn("A general publish exception occurred: eh", result.output)
        mock_publish.assert_called_once()
        self.assertEqual(1, result.exit_code)


class RecordCliTests(unittest.TestCase):
    """Unit tests for the 'record' command of the CLI."""

    def setUp(self):
        self.runner = CliRunner()

    def tearDown(self):
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


class RecorderClassTests(unittest.TestCase):
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
            '"sent-at": "2018-11-18T10:11:41+00:00"}, "id": "273ed91d-b8b5-487a-9576-95b9fbdf3eec"'
            ', "queue": null, "topic": "test_topic1"}\n'
        )

        with self.assertRaises(exceptions.HaltConsumer) as cm:
            test_recorder.collect_message(msg2)
        the_exception = cm.exception
        self.assertEqual(the_exception.exit_code, 0)
        self.assertEqual(test_recorder.counter, 2)
        mock_file.write.assert_called_with(
            '{"body": {"test_key2": "test_value2"}, "headers": '
            '{"fedora_messaging_schema": "base.message", "fedora_messaging_severity": '
            '20, "sent-at": "2018-11-18T10:11:41+00:00"}, "id": '
            '"273ed91d-b8b5-487a-9576-95b9fbdf3eec", "queue": null, "topic": "test_topic2"}\n'
        )

    def test_recorded_messages_dumps_failed(self):
        """Assert that attempt to save improper recorded message is reported."""
        mock_file = mock.MagicMock()
        test_recorder = cli.Recorder(1, mock_file)
        with self.assertRaises(exceptions.HaltConsumer) as cm:
            test_recorder.collect_message("msg1")
        the_exception = cm.exception
        self.assertEqual(the_exception.exit_code, 1)
        self.assertEqual(test_recorder.counter, 0)
        mock_file.write.assert_not_called()
