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
"""Tests for the testing utilities."""

from unittest import mock

import pytest

from fedora_messaging import api, message, testing


class CustomMessage(api.Message):
    pass


class TestMockSends:
    """Tests for the :func:`fedora_messaging.testing.mock_sends` function."""

    def test_class(self):
        """Assert all goes well if the message published matches the asserted class."""

        def pub():
            api.publish(api.Message())

        with testing.mock_sends(api.Message) as sent:
            pub()
        assert len(sent) == 1
        assert isinstance(sent[0], api.Message)

    def test_instance(self):
        """Assert all goes well if the message published matches the asserted instance."""

        def pub():
            api.publish(api.Message())

        with testing.mock_sends(api.Message()) as sent:
            pub()
        assert len(sent) == 1
        assert isinstance(sent[0], api.Message)

    def test_expected_none(self):
        """Assert failure if a message is unexpectedly sent."""

        def pub():
            api.publish(api.Message())

        with pytest.raises(AssertionError) as cm:
            with testing.mock_sends() as sent:
                pub()
        assert "Expected 0 messages to be sent, but 1 were sent" == cm.value.args[0]
        assert len(sent) == 1
        assert isinstance(sent[0], api.Message)

    def test_mix_class_instance(self):
        """Assert a mix of class and instance works."""

        def pub():
            api.publish(api.Message())
            api.publish(CustomMessage())

        with mock.patch.dict(message._class_to_schema_name, {CustomMessage: "custom"}):
            with testing.mock_sends(api.Message(), CustomMessage) as sent:
                pub()
        assert len(sent) == 2
        assert isinstance(sent[1], CustomMessage)

    def test_mix_class_instance_order_matters(self):
        """Assert the order of messages matters."""
        expected_err = (
            "Expected message of type <class 'tests.unit.test_testing"
            ".CustomMessage'>, but <class 'fedora_messaging.message.Message'> was sent"
        )

        def pub():
            api.publish(api.Message())
            api.publish(CustomMessage())

        with mock.patch.dict(message._class_to_schema_name, {CustomMessage: "custom"}):
            with pytest.raises(AssertionError) as cm:
                with testing.mock_sends(CustomMessage, api.Message()):
                    pub()
        assert expected_err == cm.value.args[0]

    def test_too_many(self):
        """Assert publishing more messages than expected fails with an AssertionError."""

        def pub():
            api.publish(api.Message())
            api.publish(api.Message())

        with pytest.raises(AssertionError) as cm:
            with testing.mock_sends(api.Message) as sent:
                pub()
        assert "Expected 1 messages to be sent, but 2 were sent" == cm.value.args[0]
        assert len(sent) == 2

    def test_wrong_type(self):
        """Assert sending the wrong type of message raises an AssertionError."""
        expected_err = (
            "Expected message of type <class 'tests.unit.test_testing"
            ".CustomMessage'>, but <class 'fedora_messaging.message.Message'> was sent"
        )

        def pub():
            api.publish(api.Message())

        with pytest.raises(AssertionError) as cm:
            with testing.mock_sends(CustomMessage) as sent:
                pub()
        assert expected_err == cm.value.args[0]
        assert len(sent) == 1
        assert isinstance(sent[0], api.Message)
        assert not isinstance(sent[0], CustomMessage)
