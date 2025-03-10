# SPDX-FileCopyrightText: 2024 Red Hat, Inc
#
# SPDX-License-Identifier: GPL-2.0-or-later

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
