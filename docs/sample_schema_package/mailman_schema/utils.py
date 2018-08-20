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
"""This contains utils to parse the message."""

import email
from collections import OrderedDict
from hashlib import sha256

from six.moves.urllib import parse


def get_avatar(from_header, size=64, default="retro"):
    """Get the avatar URL from the email's From header.

    Args:
        from_header (str): The email's From header. May contain the sender's full name.

    Returns:
        str: The URL to that sender's avatar.
    """
    params = OrderedDict([("s", size), ("d", default)])
    query = parse.urlencode(params)
    address = email.utils.parseaddr(from_header)[1]
    value_hash = sha256(address.encode("utf-8")).hexdigest()
    return "https://seccdn.libravatar.org/avatar/{}?{}".format(value_hash, query)
