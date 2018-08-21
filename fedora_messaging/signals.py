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
"""
Signals sent by fedora_messaging APIs using :class:`blinker.base.Signal` signals.
"""
from __future__ import absolute_import, unicode_literals

import blinker

_signals = blinker.Namespace()

pre_publish_signal = _signals.signal("pre_publish")
pre_publish_signal.__doc__ = """
A signal triggered before the message is published. The signal handler should
accept a single keyword argument, ``message``, which is the instance of the
:class:`fedora_messaging.message.Message` being sent. It is acceptable to
mutate the message, but the ``validate`` method will be called on it after this
signal.
"""

publish_signal = _signals.signal("publish_success")
publish_signal.__doc__ = """
A signal triggered after a message is published successfully. The signal
handler should accept a single keyword argument, ``message``, which is the
instance of the :class:`fedora_messaging.message.Message` that was sent.
"""

publish_failed_signal = _signals.signal("publish_failed_signal")
publish_failed_signal.__doc__ = """
A signal triggered after a message fails to publish for some reason. The signal
handler should accept two keyword argument, ``message``, which is the instance
of the :class:`fedora_messaging.message.Message` that failed to be sent, and
``error``, the exception that was raised.
"""
