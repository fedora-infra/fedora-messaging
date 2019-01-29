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

import unittest

from six.moves.urllib import parse

from fedora_messaging import schema_utils


class AvatarURLTests(unittest.TestCase):
    def test_base(self):
        expected = (
            "https://seccdn.libravatar.org/avatar/"
            "7cebe82e9588dcccabc7f32d50901befeb4dfa42baf08044791feea4b48b2e34"
            "?s=64&d=retro"
        )
        assert schema_utils.user_avatar_url("testuser") == expected

    def test_size(self):
        # Check default size == 64
        default_url = parse.urlparse(schema_utils.user_avatar_url("testuser"))
        assert ("s", "64") in parse.parse_qsl(default_url.query)
        # Check a different size
        custom_url = parse.urlparse(schema_utils.user_avatar_url("testuser", size=128))
        assert ("s", "128") in parse.parse_qsl(custom_url.query)

    def test_different_default(self):
        url = schema_utils.user_avatar_url("testuser", default="testdefault")
        assert ("d", "testdefault") in parse.parse_qsl(parse.urlparse(url).query)


class LibravatarURLTests(unittest.TestCase):
    def test_openid(self):
        expected = (
            "https://seccdn.libravatar.org/avatar/"
            "ae5deb822e0d71992900471a7199d0d95b8e7c9d05c40a8245a281fd2c1d6684"
            "?s=64&d=retro"
        )
        assert schema_utils.libravatar_url(openid="testuser") == expected

    def test_email(self):
        expected = (
            "https://seccdn.libravatar.org/avatar/"
            "ae5deb822e0d71992900471a7199d0d95b8e7c9d05c40a8245a281fd2c1d6684"
            "?s=64&d=retro"
        )
        assert schema_utils.libravatar_url(email="testuser") == expected

    def test_invalid_params(self):
        self.assertRaises(ValueError, schema_utils.libravatar_url)
