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

import pytest

from fedora_messaging.twisted.stats import ConsumerStatistics


def test_repr():
    expected = "<ConsumerStatistics {'received': 0, 'processed': 0, 'dropped': 0, 'rejected': 0, 'failed': 0}>"
    assert repr(ConsumerStatistics()) == expected
    assert str(ConsumerStatistics()) == expected


def test_stats_add():
    stats_1 = ConsumerStatistics()
    stats_1.received = 42
    stats_1.processed = 43
    stats_2 = ConsumerStatistics()
    stats_2.received = 1
    stats_2.processed = 2
    stats_2.dropped = 10
    combined = stats_1 + stats_2
    assert combined.as_dict() == {
        "received": 43,
        "processed": 45,
        "dropped": 10,
        "rejected": 0,
        "failed": 0,
    }


def test_stats_add_bad_type():
    with pytest.raises(TypeError) as handler:
        ConsumerStatistics() + 42
    assert str(handler.value) == (
        "ConsumerStatistics instances can only be added to other ConsumerStatistics instances."
    )


def test_stats_bad_attr():
    with pytest.raises(AttributeError) as handler:
        ConsumerStatistics().dummy = 42
    assert str(handler.value) == (
        "ConsumerStatistics does not have a dummy attribute. Available attributes: dropped, "
        "failed, processed, received, rejected."
    )
