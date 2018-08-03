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

from fedora_messaging import cli

FIXTURES_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../fixtures/'))
GOOD_CONF = os.path.join(FIXTURES_DIR, 'good_conf.toml')
BAD_CONF = os.path.join(FIXTURES_DIR, 'bad_conf.toml')


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

    @mock.patch('fedora_messaging.cli.api.consume')
    def test_good_conf(self, mock_consume):
        """Assert providing a configuration file via the CLI works."""
        runner = CliRunner()
        result = runner.invoke(cli.cli, ['--conf=' + GOOD_CONF, 'consume'])
        mock_consume.assert_called_with(
                echo,
                [{'exchange': 'e', 'queue_name': 'q', 'routing_key': '#'}])
        self.assertEqual(0, result.exit_code)

    @mock.patch('fedora_messaging.cli.api.consume')
    def test_conf_env_support(self, mock_consume):
        """Assert FEDORA_MESSAGING_CONF environment variable is supported."""
        runner = CliRunner()
        result = runner.invoke(
            cli.cli, ['consume'], env={'FEDORA_MESSAGING_CONF': GOOD_CONF})
        mock_consume.assert_called_with(
                echo,
                [{'exchange': 'e', 'queue_name': 'q', 'routing_key': '#'}])
        self.assertEqual(0, result.exit_code)

    @mock.patch('fedora_messaging.cli.api.consume')
    def test_bad_conf(self, mock_consume):
        """Assert a bad configuration file is reported."""
        expected_err = ('Error: Invalid value: Configuration error: Failed to parse'
                        ' {}: error at line 1, column 1'.format(BAD_CONF))
        runner = CliRunner()
        result = runner.invoke(cli.cli, ['--conf=' + BAD_CONF, 'consume'])
        self.assertEqual(2, result.exit_code)
        self.assertIn(expected_err, result.output)

    @mock.patch('fedora_messaging.cli.api.consume')
    def test_missing_conf(self, mock_consume):
        """Assert a missing configuration file is reported."""
        runner = CliRunner()
        result = runner.invoke(cli.cli, ['--conf=thispathdoesnotexist', 'consume'])
        self.assertEqual(2, result.exit_code)
        self.assertIn('Error: Invalid value: thispathdoesnotexist is not a file', result.output)
