# coding: utf-8
"""
Functions used as keys to sort positions (i.e. lists of Lots).
"""

__all__ = [
    "SortType",
    "sort_oldest",
    "sort_cheapest",
    "sort_dearest",
    "FIFO",
    "LIFO",
    "MINGAIN",
    "MAXGAIN",
]


# stdlib imports
from typing import Tuple, Mapping, Callable, Union


# local imports
from .types import Lot


SortType = Mapping[str, Union[bool, Callable[[Lot], Tuple]]]


def sort_oldest(lot: Lot) -> Tuple:
    """Sort by holding period, then by opening Transaction.uniqueid.

    Args:
        lot: a Lot instance.

    Returns:
        (Lot.opentransaction.datetime, Lot.opentransaction.uniqueid)
    """
    opentx = lot.opentransaction
    return (opentx.datetime, opentx.uniqueid or "")


def sort_cheapest(lot: Lot) -> Tuple:
    """Sort by price, then by opening Transaction.uniqueid.

    Args:
        lot: a Lot instance.

    Returns:
        (Lot.price, Lot.opentransaction.uniqueid)
    """
    # FIXME this doesn't sort stably for identical prices but different lots
    return (lot.price, lot.opentransaction.uniqueid or "")


def sort_dearest(lot: Lot) -> Tuple:
    """Sort by inverse price, then by opening Transaction.uniqueid.

    Args:
        lot: a Lot instance.

    Returns:
        (-Lot.price, Lot.opentransaction.uniqueid)
    """
    # FIXME this doesn't sort stably for identical prices but different lots
    return (-lot.price, lot.opentransaction.uniqueid or "")


FIFO = {"key": sort_oldest, "reverse": False}
LIFO = {"key": sort_oldest, "reverse": True}
MINGAIN = {"key": sort_dearest, "reverse": False}
MAXGAIN = {"key": sort_cheapest, "reverse": False}
