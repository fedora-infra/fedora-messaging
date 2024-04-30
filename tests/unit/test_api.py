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
"""Tests for the :module:`fedora_messaging.api` module."""

import logging
from unittest import mock

import pytest
import pytest_twisted
from twisted.internet import defer, threads

from fedora_messaging import api, config
from fedora_messaging.exceptions import PublishException
from fedora_messaging.signals import (
    pre_publish_signal,
    publish_failed_signal,
    publish_signal,
)
from fedora_messaging.twisted import consumer


class TestCheckCallback:
    """Tests for :func:`api._check_callback`"""

    def test_method(self):
        """Assert methods are valid."""

        class Callback:
            def callback(self):
                return

        class_instance = Callback()

        callback = api._check_callback(class_instance.callback)

        assert callback == class_instance.callback

    def test_func(self):
        """Assert functions are valid."""

        def callback(message):
            return

        cb = api._check_callback(callback)

        assert cb is callback

    def test_class(self):
        """Assert classes are instantiated."""

        class Callback:
            def __call__(self, message):
                return "It worked"

        callback = api._check_callback(Callback)

        assert callback(None) == "It worked"

    def test_class_no_call(self):
        """Assert classes are instantiated."""

        class Callback:
            pass

        with pytest.raises(ValueError):
            api._check_callback(Callback)

    def test_class_init_args(self):
        """Assert classes are instantiated."""

        class Callback:
            def __init__(self, args):
                self.args = args

        with pytest.raises(TypeError):
            api._check_callback(Callback)

    def test_not_callable(self):
        """Assert a ValueError is raised for things that can't be called."""
        with pytest.raises(ValueError):
            api._check_callback("")


@mock.patch("fedora_messaging.api._twisted_service")
class TestTwistedConsume:
    """Tests for :func:`api.twisted_consume`"""

    def dummy_callback(self):
        pass

    def test_wrap_bindings(self, mock_service):
        """Assert bindings are always passed to the factory as a list."""
        bindings = {"queue": "q1", "exchange": "e1", "routing_keys": ["#"]}

        def callback(msg):
            pass

        api.twisted_consume(callback, bindings, {})

        mock_service._service.factory.consume.assert_called_once_with(callback, [bindings], {})

    def test_defaults(self, mock_service):
        """Assert that bindings and queues come from the config if not provided."""

        api.twisted_consume(self.dummy_callback)

        mock_service._service.factory.consume.assert_called_once_with(
            self.dummy_callback,
            config.conf["bindings"],
            config.conf["queues"],
        )

    def test_bindings_dict(self, mock_service):
        """Assert consume wraps bindings in a list of just a plain dict"""
        bindings = {"queue": "q1", "exchange": "e1", "routing_keys": ["#"]}

        api.twisted_consume(self.dummy_callback, bindings)

        mock_service._service.factory.consume.assert_called_once_with(
            self.dummy_callback, [bindings], config.conf["queues"]
        )

    def test_bindings_invalid_type(self, mock_service):
        """Assert bindings are validated and result in a value error if they are invalid."""
        with pytest.raises(ValueError):
            api.twisted_consume(self.dummy_callback, "test_bindings")

    def test_bindings_list_of_dict(self, mock_service):
        """Assert consume is working(bindings type is dict)"""
        bindings = [{"queue": "q1", "exchange": "e1", "routing_keys": ["#"]}]

        api.twisted_consume(self.dummy_callback, bindings)

        mock_service._service.factory.consume.assert_called_once_with(
            self.dummy_callback, bindings, config.conf["queues"]
        )

    def test_queues_invalid_type(self, mock_service):
        """Assert queues are validated and result in a value error if they are invalid."""
        with pytest.raises(ValueError):
            api.twisted_consume(self.dummy_callback, None, "should be a dict")

    def test_with_queues(self, mock_service):
        """Assert queues is used over the config if provided."""
        queues = {
            "q1": {
                "durable": True,
                "auto_delete": False,
                "exclusive": False,
                "arguments": {},
            }
        }

        api.twisted_consume(self.dummy_callback, bindings=[], queues=queues)
        mock_service._service.factory.consume.assert_called_once_with(
            self.dummy_callback, [], queues
        )


class TestConsume:
    @mock.patch("fedora_messaging.api.crochet")
    @mock.patch("fedora_messaging.api._twisted_consume_wrapper")
    def test_consume(self, mock_wrapper, mock_crochet):
        """Assert the consume call forwards to the twisted wrapper."""
        api.consume(1, 2, 3)

        mock_crochet.setup.assert_called_once_with()
        mock_wrapper.assert_called_once_with(1, 2, 3)


@mock.patch("fedora_messaging.api._twisted_publish_wrapper")
class TestPublish:

    def test_publish_to_exchange(self, mock_twisted_publish):
        """Assert a message can be published to the exchange."""
        message = "test_message"
        exchange = "test_exchange"

        api.publish(message, exchange)

        mock_twisted_publish.assert_called_once_with(message, exchange)
        mock_twisted_publish.return_value.wait.assert_called_once_with(timeout=30)

    def test_publish_failed(self, mock_twisted_publish):
        """Assert an exception is raised when message can't be published."""
        message = "test_message"
        exchange = "test_exchange"
        expected_exception = PublishException(reason="Unable to publish message")
        mock_twisted_publish.return_value.wait.side_effect = expected_exception

        with pytest.raises(type(expected_exception)):
            api.publish(message, exchange=exchange)

        mock_twisted_publish.assert_called_once_with(message, exchange)


class TestTwistedPublishWrapper:

    @pytest_twisted.inlineCallbacks
    def test_publish_to_exchange(self):
        """Assert a message can be published to the exchange."""
        message = "test_message"
        exchange = "test_exchange"

        with mock.patch("fedora_messaging.api.twisted_publish") as mock_twisted_publish:
            result = api._twisted_publish_wrapper(message, exchange)
            yield threads.deferToThread(result.wait, timeout=1)

        mock_twisted_publish.assert_called_once_with(message, exchange)

    @pytest_twisted.inlineCallbacks
    def test_publish_failed(self):
        """Assert an exception is raised when message can't be published."""
        message = "test_message"
        exchange = "test_exchange"
        expected_exception = PublishException(reason="Unable to publish message")

        with mock.patch("fedora_messaging.api.twisted_publish") as mock_twisted_publish:
            mock_twisted_publish.side_effect = expected_exception
            with pytest.raises(type(expected_exception)):
                result = api._twisted_publish_wrapper(message, exchange=exchange)
                yield threads.deferToThread(result.wait, timeout=1)

        mock_twisted_publish.assert_called_once_with(message, exchange)

    @pytest_twisted.inlineCallbacks
    def test_publish_cancelled(self, caplog):
        """Assert an exception is raised when message can't be published."""
        message = "test_message"
        exchange = "test_exchange"
        expected_exception = defer.CancelledError("dummy error")
        caplog.set_level(logging.DEBUG)

        with mock.patch("fedora_messaging.api.twisted_publish") as mock_twisted_publish:
            mock_twisted_publish.side_effect = expected_exception
            result = api._twisted_publish_wrapper(message, exchange=exchange)
            yield threads.deferToThread(result.wait, timeout=1)

        mock_twisted_publish.assert_called_once_with(message, exchange)
        log_records = [r.getMessage() for r in caplog.records if r.name == "fedora_messaging.api"]
        assert log_records == ["Canceled publish of 'test_message' to test_exchange due to timeout"]


class TestTwistedPublish:
    def setup_method(self, method):
        self.pre_publish_signal_data = {"called": False, "sender": None, "args": None}
        self.publish_signal_data = {"called": False, "sender": None, "args": None}
        self.publish_failed_signal_data = {
            "called": False,
            "sender": None,
            "args": None,
        }

        @pre_publish_signal.connect
        def pre_publish_signal_handler(sender, **kwargs):
            self.pre_publish_signal_data = {
                "called": True,
                "sender": sender,
                "args": kwargs,
            }

        @publish_signal.connect
        def publish_signal_handler(sender, **kwargs):
            self.publish_signal_data = {
                "called": True,
                "sender": sender,
                "args": kwargs,
            }

        @publish_failed_signal.connect
        def publish_failed_signal_handler(sender, **kwargs):
            self.publish_failed_signal_data = {
                "called": True,
                "sender": sender,
                "args": kwargs,
            }

        self.pre_publish_signal_handler = pre_publish_signal_handler
        self.publish_signal_handler = publish_signal_handler
        self.publish_failed_signal_handler = publish_failed_signal_handler

    def teardown_method(self, method):
        pre_publish_signal.disconnect(self.pre_publish_signal_handler)
        publish_signal.disconnect(self.publish_signal_handler)
        publish_failed_signal.disconnect(self.publish_failed_signal_handler)

    @pytest_twisted.inlineCallbacks
    def test_publish_to_exchange(self):
        """Assert a message can be published to the exchange."""
        message = "test_message"
        exchange = "test_exchange"
        expected_pre_publish_signal_data = {
            "called": True,
            "sender": api.publish,
            "args": {"message": message},
        }
        expected_publish_signal_data = {
            "called": True,
            "sender": api.publish,
            "args": {"message": message},
        }
        expected_publish_failed_signal_data = {
            "called": False,
            "sender": None,
            "args": None,
        }

        with mock.patch("fedora_messaging.api._twisted_service") as mock_service:
            yield api.twisted_publish(message, exchange)

        mock_service._service.factory.publish.assert_called_once_with(message, exchange=exchange)
        assert self.pre_publish_signal_data == expected_pre_publish_signal_data
        assert self.publish_signal_data == expected_publish_signal_data
        assert self.publish_failed_signal_data == expected_publish_failed_signal_data

    @pytest_twisted.inlineCallbacks
    def test_publish_failed(self):
        """Assert an exception is raised when message can't be published."""
        message = "test_message"
        exchange = "test_exchange"
        expected_exception = PublishException(reason="Unable to publish message")
        expected_pre_publish_signal_data = {
            "called": True,
            "sender": api.publish,
            "args": {"message": message},
        }
        expected_publish_signal_data = {"called": False, "sender": None, "args": None}
        expected_publish_failed_signal_data = {
            "called": True,
            "sender": api.publish,
            "args": {"message": message, "reason": expected_exception},
        }

        with mock.patch("fedora_messaging.api._twisted_service") as mock_service:
            mock_service._service.factory.publish.side_effect = expected_exception
            with pytest.raises(type(expected_exception)):
                yield api.twisted_publish(message, exchange=exchange)

        mock_service._service.factory.publish.assert_called_once_with(message, exchange=exchange)
        assert self.pre_publish_signal_data == expected_pre_publish_signal_data
        assert self.publish_signal_data == expected_publish_signal_data
        assert self.publish_failed_signal_data == expected_publish_failed_signal_data

    @pytest_twisted.inlineCallbacks
    def test_publish_to_config_exchange(self):
        """Assert a message can be published to the exchange from config."""
        message = "test_message"
        with mock.patch.dict(config.conf, {"publish_exchange": "test_public_exchange"}):
            with mock.patch("fedora_messaging.api._twisted_service") as mock_service:
                yield api.twisted_publish(message)
        mock_service._service.factory.publish.assert_called_once_with(
            message, exchange="test_public_exchange"
        )


@pytest_twisted.inlineCallbacks
def test_consume_unexpected_crash():
    """Assert unexpected exceptions raised inside the Twisted thread are raised."""
    try:
        with mock.patch("fedora_messaging.api._check_callback") as mock_check:
            mock_check.side_effect = RuntimeError("Beep boop you messed up")
            yield threads.deferToThread(api.consume, None)
        pytest.fail("Expected a RuntimeException to be raised")
    except RuntimeError as e:
        assert e.args[0] == "Beep boop you messed up"


@pytest_twisted.inlineCallbacks
def test_consume_successful_halt():
    """Assert consume halts when all consumer.result deferreds have succeeded."""
    consumers = [consumer.Consumer()]
    consumers[0].result = defer.succeed(None)
    try:
        with mock.patch("fedora_messaging.api.twisted_consume") as mock_consume:
            mock_consume.return_value = defer.succeed(consumers)
            d = threads.deferToThread(api.consume, None)
            consumer._add_timeout(d, 0.1)
            yield d
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Expected the consume call to immediately finish, not time out")
