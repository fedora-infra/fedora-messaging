# Copyright (C) 2018  Red Hat, Inc.
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
"""
The ``schema_utils`` module contains utilities that may be useful when writing
the Python API of your message schemas.
"""

import collections
from hashlib import sha256

from six.moves.urllib import parse


def user_avatar_url(username, size=64, default="retro"):
    """Get the avatar URL of the provided Fedora username.

    The URL is returned from the Libravatar service.

    Args:
        username (str): The username to get the avatar of.
        size (int): Size of the avatar in pixels (it's a square).
        default (str): Default avatar to return if not found.
    Returns:
        str: The URL to the avatar image.
    """
    openid = "http://{}.id.fedoraproject.org/".format(username)
    return libravatar_url(openid=openid, size=size, default=default)


def libravatar_url(email=None, openid=None, size=64, default="retro"):
    """Get the URL to an avatar from libravatar.

    Either the user's email or openid must be provided.

    If you want to use Libravatar federation (through DNS), you should install
    and use the ``libravatar`` library instead. Check out the
    ``libravatar.libravatar_url()`` function.

    Args:
        email (str): The user's email
        openid (str): The user's OpenID
        size (int): Size of the avatar in pixels (it's a square).
        default (str): Default avatar to return if not found.
    Returns:
        str: The URL to the avatar image.
    Raises:
        ValueError: If neither email nor openid are provided.
    """
    # We use an OrderedDict here to make testing easier (URL strings become
    # predictable).
    params = collections.OrderedDict([("s", size), ("d", default)])
    query = parse.urlencode(params)
    if email:
        value = email
    elif openid:
        value = openid
    else:
        raise ValueError("You must provide either the email or the openid.")
    idhash = sha256(value.encode("utf-8")).hexdigest()
    return "https://seccdn.libravatar.org/avatar/%s?%s" % (idhash, query)
