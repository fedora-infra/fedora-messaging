# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

"""
The ``schema_utils`` module contains utilities that may be useful when writing
the Python API of your message schemas.
"""

import collections
from collections.abc import Mapping
from hashlib import sha256
from typing import Optional
from urllib import parse


def user_avatar_url(username: str, size: int = 64, default: str = "retro") -> str:
    """Get the avatar URL of the provided Fedora username.

    The URL is returned from the Libravatar service.

    Args:
        username: The username to get the avatar of.
        size: Size of the avatar in pixels (it's a square).
        default: Default avatar to return if not found.
    Returns:
        The URL to the avatar image.
    """
    openid = f"http://{username}.id.fedoraproject.org/"
    return libravatar_url(openid=openid, size=size, default=default)


def libravatar_url(
    email: Optional[str] = None,
    openid: Optional[str] = None,
    size: int = 64,
    default: str = "retro",
) -> str:
    """Get the URL to an avatar from libravatar.

    Either the user's email or openid must be provided.

    If you want to use Libravatar federation (through DNS), you should install
    and use the ``libravatar`` library instead. Check out the
    ``libravatar.libravatar_url()`` function.

    Args:
        email: The user's email
        openid: The user's OpenID
        size: Size of the avatar in pixels (it's a square).
        default: Default avatar to return if not found.
    Returns:
        The URL to the avatar image.
    Raises:
        ValueError: If neither email nor openid are provided.
    """
    # We use an OrderedDict here to make testing easier (URL strings become
    # predictable).
    params: Mapping[str, object] = collections.OrderedDict([("s", size), ("d", default)])
    query = parse.urlencode(params)
    if email:
        value = email
    elif openid:
        value = openid
    else:
        raise ValueError("You must provide either the email or the openid.")
    idhash = sha256(value.encode("utf-8")).hexdigest()
    return f"https://seccdn.libravatar.org/avatar/{idhash}?{query}"
