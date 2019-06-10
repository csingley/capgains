# coding: utf-8
"""
"""

__all__ = ["part_units", "part_basis"]


# stdlib imports
from decimal import Decimal
from typing import Tuple, List, Sequence, Generator, Optional


# local imports
from .models import Lot
from .predicates import PredicateType
from capgains import utils


def part_units(
    position: List[Lot],
    predicate: Optional[PredicateType] = None,
    max_units: Optional[Decimal] = None,
) -> Tuple[List[Lot], List[Lot]]:
    """Partition a position according to some predicate.

    Note:
        If `max_units` is set, then `predicate` must match only Lots where
        units are the same sign as `max_units`.ab.

    Args:
        position: list of Lots.  Must be presorted by caller.
        predicate: filter function that accepts a Lot instance and returns bool,
                   e.g. openAsOf(datetime) or closableBy(transaction).
                   By default, matches everything.
        max_units: limit of units matching predicate to take.  Sign convention
                   is SAME SIGN as position, i.e. `max_units` is positive for long,
                   negative for short. By default, take all units that match predicate.

    Returns:
        (matching Lots, nonmatching Lots)
    """

    if not position:
        return [], []

    if predicate is None:
        predicate = utils.matchEverything

    lots_taken, lots_left = zip(*_iterpart_lot_units(position, predicate, max_units))
    return (
        [lot for lot in lots_taken if lot is not None],
        [lot for lot in lots_left if lot is not None],
    )


def _iterpart_lot_units(
    position: Sequence[Lot], predicate: PredicateType, max_units: Optional[Decimal]
) -> Generator[Tuple[Optional[Lot], Optional[Lot]], None, None]:
    """Iterator over Lots; partition according to `predicate`/`max_units` constraints.

    Args:
        position: list of Lots.  Must be presorted by caller.
        predicate: filter function that accepts a Lot instance and returns bool,
                   e.g. openAsOf(datetime) or closableBy(transaction).
                   By default, matches everything.
        max_units: limit of units matching predicate to take.  Sign convention
                   is SAME SIGN as position, i.e. `max_units` is positive for long,
                   negative for short. By default, take all units that match predicate.

    Yields:
        2-tuple of:
            0) Lot instance matching `predicate`/`max_units` constraints, or None.
            1) Lot instance failing `predicate`/`max_units` constrants, or None.
    """

    units_remain = max_units
    for lot in position:
        # Failing the predicate trumps any consideration of max_units.
        if not predicate(lot):
            yield (None, lot)
        # All cases below here have matched the predicate.
        # Now consider max_units constraint.
        elif units_remain is None:
            # args passed in max_units=None -> take all predicate matches
            yield (lot, None)
        elif units_remain == 0:
            # max_units already filled; we're done.
            yield (None, lot)
        else:
            # Predicate matched; max_units unfilled.
            # Take as many units as possible until we run out of Lot.units or max_units.
            assert lot.units * units_remain > 0
            if lot.units / units_remain <= 1:
                # Taking the whole Lot won't exceed max_units (but might reach it).
                units_remain -= lot.units
                yield (lot, None)
            else:
                # The Lot more than suffices to fulfill max_units -> split the Lot
                taken, left = (
                    lot._replace(units=units_remain),
                    lot._replace(units=lot.units - units_remain),
                )
                units_remain = Decimal("0")
                yield (taken, left)


def part_basis(
    position: List[Lot], predicate: Optional[PredicateType], fraction: Decimal
) -> Tuple[List[Lot], List[Lot]]:
    """Remove a fraction of the cost from each Lot in the Position.

    Args:
        position: list of Lots.  Must be presorted by caller.
        predicate: filter function that accepts a Lot instance and returns bool,
                   e.g. openAsOf(datetime) or closableBy(transaction).
                   By default, matches everything.
        fraction: portion of cost to remove from each Lot matching predicate.

    Returns:
        2-tuple of:
            0) list of Lots (copies of Lots meeting predicate, with each
               price updated to reflect the basis removed from the original).
            1) list of Lots (original position, less basis removed).

    Raises:
        ValueError: if `fraction` isn't between 0 and 1.
    """

    if not position:
        return [], []

    if predicate is None:
        predicate = utils.matchEverything

    if not (0 <= fraction <= 1):
        msg = f"fraction must be between 0 and 1 (inclusive), not '{fraction}'"
        raise ValueError(msg)

    lots_taken, lots_left = zip(
        *(_part_lot_basis(lot, predicate, fraction) for lot in position)
    )
    return (
        [lot for lot in lots_taken if lot is not None],
        [lot for lot in lots_left if lot is not None],
    )


def _part_lot_basis(
    lot: Lot, predicate: PredicateType, fraction: Decimal
) -> Tuple[Optional[Lot], Lot]:
    """
    Args:
        lot: a Lot instance.
        predicate: filter function that accepts a Lot instance and returns bool,
        fraction: portion of cost to remove from Lot if predicate matches.

    Returns:
        2-tuple of:
            0) If Lot matches predicate - copy of Lot, with basis fraction removed.
                If Lot fails predicate - None.
            1) Original Lot, with basis reduced by fraction (if applicable).
    """
    if not predicate(lot):
        return (None, lot)

    takenprice = lot.price * fraction
    return (lot._replace(price=takenprice), lot._replace(price=lot.price - takenprice))
