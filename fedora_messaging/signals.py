# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

"""
Signals sent by fedora_messaging APIs using :class:`blinker.base.Signal` signals.
"""


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
