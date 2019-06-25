# coding: utf-8

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
from __future__ import absolute_import

import unittest
import mock

from twisted.internet import threads, defer
import pytest
import pytest_twisted

from fedora_messaging import api, config
from fedora_messaging.exceptions import PublishException
from fedora_messaging.twisted import consumer, protocol
from fedora_messaging.signals import (
    pre_publish_signal,
    publish_signal,
    publish_failed_signal,
)


class CheckCallbackTests(unittest.TestCase):
    """Tests for :func:`api._check_callback`"""

    def test_method(self):
        """Assert methods are valid."""

        class Callback(object):
            def callback(self):
                return

        class_instance = Callback()

        callback = api._check_callback(class_instance.callback)

        self.assertIs(callback, callback)

    def test_func(self):
        """Assert functions are valid."""

        def callback(message):
            return

        cb = api._check_callback(callback)

        self.assertIs(cb, callback)

    def test_class(self):
        """Assert classes are instantiated."""

        class Callback(object):
            def __call__(self, message):
                return "It worked"

        callback = api._check_callback(Callback)

        self.assertEqual(callback(None), "It worked")

    def test_class_no_call(self):
        """Assert classes are instantiated."""

        class Callback(object):
            pass

        try:
            api._check_callback(Callback)
            self.fail("_check_callback failed to raise an exception")
        except ValueError:
            pass

    def test_class_init_args(self):
        """Assert classes are instantiated."""

        class Callback(object):
            def __init__(self, args):
                self.args = args

        try:
            api._check_callback(Callback)
            self.fail("_check_callback failed to raise a TypeError")
        except TypeError:
            pass

    def test_not_callable(self):
        """Assert a ValueError is raised for things that can't be called."""
        try:
            api._check_callback("")
            self.fail("_check_callback failed to raise an ValueError")
        except ValueError:
            pass


@mock.patch("fedora_messaging.api._twisted_service")
class TwistedConsumeTests(unittest.TestCase):
    """Tests for :func:`api.twisted_consume`"""

    def dummy_callback(self):
        pass

    def test_wrap_bindings(self, mock_service):
        """Assert bindings are always passed to the factory as a list."""

        def callback(msg):
            pass

        api.twisted_consume(callback)

        mock_service._service.factory.consume.called_once_with(callback, [{}], {})

    def test_defaults(self, mock_service):
        """Assert that bindings and queues come from the config if not provided."""

        api.twisted_consume(self.dummy_callback)

        mock_service._service.factory.consume.called_once_with(
            self.dummy_callback,
            bindings=config.conf["bindings"],
            queues=config.conf["queues"],
        )

    def test_bindings_dict(self, mock_service):
        """Assert consume wraps bindings in a list of just a plain dict"""
        bindings = {"queue": "q1", "exchange": "e1", "routing_keys": ["#"]}

        api.twisted_consume(self.dummy_callback, bindings)

        mock_service._service.factory.consume.called_once_with(
            self.dummy_callback, bindings=[bindings], queues=config.conf["queues"]
        )

    def test_bindings_invalid_type(self, mock_service):
        """Assert bindings are validated and result in a value error if they are invalid."""
        self.assertRaises(
            ValueError, api.twisted_consume, self.dummy_callback, "test_bindings"
        )

    def test_bindings_list_of_dict(self, mock_service):
        """Assert consume is working(bindings type is dict)"""
        bindings = [{"queue": "q1", "exchange": "e1", "routing_keys": ["#"]}]

        api.twisted_consume(self.dummy_callback, bindings)

        mock_service._service.factory.consume.called_once_with(
            self.dummy_callback, bindings=bindings, queues=config.conf["queues"]
        )

    def test_queues_invalid_type(self, mock_service):
        """Assert queues are validated and result in a value error if they are invalid."""
        self.assertRaises(
            ValueError,
            api.twisted_consume,
            self.dummy_callback,
            None,
            "should be a dict",
        )

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
        mock_service._service.factory.consume.called_once_with(
            self.dummy_callback, bindings=[], queues=queues
        )


class ConsumeTests(unittest.TestCase):
    @mock.patch("fedora_messaging.api.crochet")
    @mock.patch("fedora_messaging.api._twisted_consume_wrapper")
    def test_consume(self, mock_wrapper, mock_crochet):
        """Assert the consume call forwards to the twisted wrapper."""
        api.consume(1, 2, 3)

        mock_crochet.setup.assert_called_once_with()
        mock_wrapper.assert_called_once_with(1, 2, 3)


@mock.patch("fedora_messaging.api._twisted_publish")
class PublishTests(unittest.TestCase):
    def setUp(self):
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

    def tearDown(self):
        pre_publish_signal.disconnect(self.pre_publish_signal_handler)
        publish_signal.disconnect(self.publish_signal_handler)
        publish_failed_signal.disconnect(self.publish_failed_signal_handler)

    def test_publish_to_exchange(self, mock_twisted_publish):
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

        api.publish(message, exchange)

        mock_twisted_publish.assert_called_once_with(message, exchange)
        mock_twisted_publish.return_value.wait.assert_called_once_with(timeout=30)
        self.assertEqual(self.pre_publish_signal_data, expected_pre_publish_signal_data)
        self.assertEqual(self.publish_signal_data, expected_publish_signal_data)
        self.assertEqual(
            self.publish_failed_signal_data, expected_publish_failed_signal_data
        )

    @mock.patch.dict(
        "fedora_messaging.config.conf", {"publish_exchange": "test_public_exchange"}
    )
    def test_publish_to_config_exchange(self, mock_twisted_publish):
        """Assert a message can be published to the exchange from config."""
        message = "test_message"
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

        api.publish(message)

        mock_twisted_publish.assert_called_once_with(message, "test_public_exchange")
        self.assertEqual(self.pre_publish_signal_data, expected_pre_publish_signal_data)
        self.assertEqual(self.publish_signal_data, expected_publish_signal_data)
        self.assertEqual(
            self.publish_failed_signal_data, expected_publish_failed_signal_data
        )

    def test_publish_failed(self, mock_twisted_publish):
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
        mock_twisted_publish.return_value.wait.side_effect = expected_exception

        self.assertRaises(
            type(expected_exception), api.publish, message, exchange=exchange
        )

        mock_twisted_publish.assert_called_once_with(message, exchange)
        self.assertEqual(self.pre_publish_signal_data, expected_pre_publish_signal_data)
        self.assertEqual(self.publish_signal_data, expected_publish_signal_data)
        self.assertEqual(
            self.publish_failed_signal_data, expected_publish_failed_signal_data
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
            protocol._add_timeout(d, 0.1)
            yield d
    except (defer.TimeoutError, defer.CancelledError):
        pytest.fail("Expected the consume call to immediately finish, not time out")
