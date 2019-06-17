# coding: utf-8
"""
Functions used as filter predicates to select Lots from positins.
"""

__all__ = ["PredicateType", "openAsOf", "longAsOf", "closableBy"]


# stdlib imports
from decimal import Decimal
import datetime as _datetime
from typing import Callable


# local imports
from .types import Lot, TransactionType


PredicateType = Callable[[Lot], bool]


def openAsOf(datetime: _datetime.datetime) -> PredicateType:
    """Factory for functions that select open Lots created on or before datetime.

    Note:
        This matches both long and short lots.

    Args:
        datetime: a datetime.datetime instance.

    Returns:
        Filter function accepting a Lot instance and returning bool.
    """

    def isOpen(lot: Lot) -> bool:
        return lot.createtransaction.datetime <= datetime

    return isOpen


def longAsOf(datetime: _datetime.datetime) -> PredicateType:
    """Factory for functions that select open long Lots created on or before datetime.

    Note:
        "long" Lots have positive units.

    Args:
        datetime: a datetime.datetime instance.

    Returns:
        Filter function accepting a Lot instance and returning bool.
    """

    def isOpen(lot: Lot) -> bool:
        lot_open = lot.createtransaction.datetime <= datetime
        lot_long = lot.units > 0
        return lot_open and lot_long

    return isOpen


def closable(units: Decimal, datetime: _datetime.datetime) -> PredicateType:
    """Factory for functions selecting Lots that can be closed by a transaction's units.

    The relevent criteria are an open Lot created on or before the given
    datetime, with sign opposite to the given units.

    Args:
        units: security amount being booked to inventory (i.e. transaction.units).
        datetime: moment to determine impacted position (i.e. transaction.datetime).

    Returns:
        Filter function accepting a Lot instance and returning bool.
    """

    def closeMe(lot):
        lot_open = lot.createtransaction.datetime <= datetime
        opposite_sign = lot.units * units < 0
        return lot_open and opposite_sign

    return closeMe
