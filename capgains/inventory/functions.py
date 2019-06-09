# coding: utf-8
"""
"""

__all__ = ["part_position", "part_basis"]


# stdlib imports
from decimal import Decimal
import itertools
from typing import Tuple, List, Sequence, Generator, Optional


# local imports
from .models import Lot
from .predicates import PredicateType
from capgains import utils


########################################################################################
#  FUNCTIONS OPERATING ON POSITIONS
########################################################################################
def part_position(
    position: List[Lot],
    predicate: Optional[PredicateType] = None,
    max_units: Optional[Decimal] = None,
) -> Tuple[List[Lot], List[Lot]]:
    """Partition a position according to some predicate.

    Note:
        If `max_units` is set, then `predicate` must match only Lots where
        units are the same sign.

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
    if predicate is None:
        predicate = utils.matchEverything

    # Lots not matching the predicate definitely stay with the position.
    nonmatch, maybe = utils.partition(predicate, position)
    nonmatch = list(nonmatch)

    # Lots matching the predicate may be subject to `max_units` limit.
    if max_units is None:
        return list(maybe), list(nonmatch)

    # Decorate the sequence of lots matching predicate with the running total of units.
    maybe, maybe_clone = itertools.tee(maybe)
    accumulated_units = itertools.accumulate(lot.units for lot in maybe_clone)
    decorated_maybe = zip(accumulated_units, maybe)

    # Lots where the running total is under `max_units` are definitely partitioned out.
    #
    # Note: it might be better to use a tee()/dropwhile()/takewhile() pipeline
    # instead of the tee()/filterfalse()/filter() pipeline used by utils.partition().
    # However, the accumulated_units decoration is strictly monotonically increasing,
    # so the results should be the equivalent.
    decorated_still_maybe, decorated_match = utils.partition(
        lambda deco: deco[0] / max_units < 1, decorated_maybe
    )

    # Determine how many more units we need to fulfill the requested `max_units`.
    decorated_match = tuple(decorated_match)
    if not decorated_match:
        match: List = []
        units_unfulfilled = max_units
    else:
        accumulated_units, match = zip(*decorated_match)
        match = list(match)
        # mypy doesn't seem to understand that unpacking an unzipped tuple
        # return tuples instead of iterators.
        units_unfulfilled = max_units - accumulated_units[-1]  # type: ignore

    # The first Lot in the "still maybe" sequence is where the running total of units
    # exceeded `max_units`.  If it exists, take the units we need from the first Lot
    # and partition that out; any excess units of that Lot stay with the position.
    if units_unfulfilled:
        try:
            # mypy doesn't seem to understand that itertools.filterfalse()
            # returns an iterator instead of an iterable.
            head = next(decorated_still_maybe)  # type: ignore
        except StopIteration:
            pass
        else:
            _, lot = head
            assert abs(lot.units) >= abs(units_unfulfilled)
            match.append(lot._replace(units=units_unfulfilled))
            units_leftover = lot.units - units_unfulfilled
            if units_leftover:
                nonmatch.append(lot._replace(units=units_leftover))

    # Remaining Lots that matched `predicate` but weren't needed to satsify `max_units`
    # stay with the position.
    nonmatch.extend([t[1] for t in decorated_still_maybe])

    return match, nonmatch


def part_position_alt(
    position: List[Lot],
    predicate: Optional[PredicateType] = None,
    max_units: Optional[Decimal] = None,
) -> Tuple[List[Lot], List[Lot]]:
    """Partition a position according to some predicate.

    Note:
        This is an alternative implementation of `part_position()` that uses a
        generator instead of an itertools pipeline.  It's lazier than the above
        implementation and it short-circuits faster, but the nested if/else chains
        are unpleasant.  The logic remains here unused, as a possible basis for
        future enhancement.
    """

    if not position:
        return [], []

    if predicate is None:
        predicate = utils.matchEverything

    def take(
        position: Sequence[Lot], predicate: PredicateType, max_units: Optional[Decimal]
    ) -> Generator[Tuple[Optional[Lot], Optional[Lot]], None, None]:

        units_remain = max_units
        for lot in position:
            if not predicate(lot):
                yield (None, lot)
            elif max_units is None:
                yield (lot, None)
            elif units_remain == 0:
                yield (None, lot)
            else:
                assert units_remain is not None
                assert lot.units * units_remain > 0
                if abs(lot.units) <= abs(units_remain):
                    units_remain -= lot.units
                    yield (lot, None)
                else:
                    taken, left = (
                        lot._replace(units=units_remain),
                        lot._replace(units=lot.units - units_remain),
                    )
                    units_remain -= taken.units
                    yield (taken, left)

    lots_taken, lots_left = zip(*take(position, predicate, max_units))
    return (
        [lot for lot in lots_taken if lot is not None],
        [lot for lot in lots_left if lot is not None],
    )


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
        max_units: limit of units matching predicate to take.  Sign convention
                   is SAME SIGN as position, i.e. units arg must be positive for long,
                   negative for short. By default, take all units that match predicate.

    Returns:
        2-tuple of:
            0) list of Lots (copies of Lots meeting predicate, with each
               price updated to reflect the basis removed from the original).
            1) list of Lots (original position, less basis removed).

    Raises:
        ValueError: if `fraction` isn't between 0 and 1.
    """

    if predicate is None:
        predicate = utils.matchEverything

    if not (0 <= fraction <= 1):
        msg = f"fraction must be between 0 and 1 (inclusive), not '{fraction}'"
        raise ValueError(msg)

    def part_lot_basis(
        lot: Lot, predicate: PredicateType, fraction: Decimal
    ) -> Tuple[Optional[Lot], Lot]:
        if not predicate(lot):
            return (None, lot)

        takenprice = lot.price * fraction
        return (
            lot._replace(price=takenprice),
            lot._replace(price=lot.price - takenprice),
        )

    lots_taken, lots_left = zip(
        *(part_lot_basis(lot, predicate, fraction) for lot in position)
    )
    return (
        [lot for lot in lots_taken if lot is not None],
        [lot for lot in lots_left if lot is not None],
    )
