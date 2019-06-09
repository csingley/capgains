# coding: utf-8
"""
Functions used as filter predicates to select Lots from positins.
"""

__all__ = ["PredicateType", "openAsOf", "longAsOf", "closableBy"]


# stdlib imports
import datetime as _datetime
from typing import Callable


# local imports
from .models import Lot, TransactionType


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


def closableBy(transaction: TransactionType) -> PredicateType:
    """Factory for functions that select Lots that can be closed by a transaction.

    The relevent criteria are an open Lot created on or before the given
    transaction.datetime, with sign opposite to the given transaction.units

    Args:
        transaction: a Transaction instance.

    Returns:
        Filter function accepting a transaction and returning bool.
    """

    def closeMe(lot):
        lot_open = lot.createtransaction.datetime <= transaction.datetime
        opposite_sign = lot.units * transaction.units < 0
        return lot_open and opposite_sign

    return closeMe
