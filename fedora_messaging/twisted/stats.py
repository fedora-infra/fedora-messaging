# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

"""
Datastructures to store consumer and producer statistics.
"""

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class Statistics:
    """A datastructure to manager integers as attributes."""

    def __add__(self, other: "Statistics") -> "Statistics":
        if not isinstance(other, self.__class__):
            raise TypeError(
                f"{self.__class__.__name__} instances can only be added to other "
                f"{self.__class__.__name__} instances."
            )
        new_stats = self.__class__()
        for name in self.as_dict():
            setattr(new_stats, name, getattr(self, name) + getattr(other, name))
        return new_stats

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ConsumerStatistics(Statistics):
    """Statistics for a :class:`Consumer`."""

    received: int = 0
    processed: int = 0
    dropped: int = 0
    rejected: int = 0
    failed: int = 0


@dataclass
class FactoryStatistics(Statistics):
    """Statistics for a :class:`FedoraMessagingFactoryV2`."""

    published: int = 0
    consumed: ConsumerStatistics = field(default_factory=ConsumerStatistics)
