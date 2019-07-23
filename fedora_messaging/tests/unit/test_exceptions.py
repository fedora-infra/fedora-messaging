# This file is part of fedora_messaging.
# Copyright (C) 2019 Red Hat, Inc.
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
"""Tests for the :module:`fedora_messaging.exceptions` module."""
import unittest

from fedora_messaging.exceptions import ConnectionException, PublishException


class ConnectionExceptionTests(unittest.TestCase):
    def test_str(self):
        self.assertEqual(
            "Connection error: REASON", str(ConnectionException(reason="REASON"))
        )

    def test_str_with_undefined_reason(self):
        self.assertEqual("Connection error", str(ConnectionException()))

    def test_repr(self):
        self.assertEqual(
            "ConnectionException(reason=REASON)",
            repr(ConnectionException(reason="REASON")),
        )


class PublishExceptionTests(unittest.TestCase):
    def test_str(self):
        self.assertEqual(
            "Publish error: REASON", str(PublishException(reason="REASON"))
        )

    def test_str_with_undefined_reason(self):
        self.assertEqual("Publish error", str(PublishException()))

    def test_repr(self):
        self.assertEqual(
            "PublishException(reason=REASON)", repr(PublishException(reason="REASON"))
        )
