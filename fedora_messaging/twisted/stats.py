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
Datastructures to store consumer and producer statistics.
"""


from typing import Any


class Statistics:
    """A datastructure to manager integers as attributes."""

    names = []

    def __init__(self):
        for name in self.names:
            setattr(self, name, 0)

    def __setattr__(self, name: str, value: Any) -> None:
        if name not in self.names:
            raise AttributeError(
                f"{self.__class__.__name__} does not have a {name} attribute. "
                f"Available attributes: {', '.join(sorted(self.names))}."
            )
        return super().__setattr__(name, value)

    def __add__(self, other):
        if not isinstance(other, self.__class__):
            raise TypeError(
                f"{self.__class__.__name__} instances can only be added to other "
                f"{self.__class__.__name__} instances."
            )
        new_stats = self.__class__()
        for name in self.names:
            setattr(new_stats, name, getattr(self, name) + getattr(other, name))
        return new_stats

    def as_dict(self):
        return {name: getattr(self, name) for name in self.names}

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.as_dict()}>"


class ConsumerStatistics(Statistics):
    """Statistics for a :class:`Consumer`."""

    names = (
        "received",
        "processed",
        "dropped",
        "rejected",
        "failed",
    )


class FactoryStatistics(Statistics):
    """Statistics for a :class:`FedoraMessagingFactoryV2`."""

    names = ("published", "consumed")

    def __init__(self):
        super().__init__()
        self.consumed = ConsumerStatistics()

    def as_dict(self):
        d = super().as_dict()
        d["consumed"] = self.consumed.as_dict()
        return d
