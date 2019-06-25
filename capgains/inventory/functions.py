# coding: utf-8
"""Base functions used by inventory.api to mutate the Portfolio.
"""
from __future__ import annotations


__all__ = [
    "load_transaction",
    "part_units",
    "part_basis",
    "adjust_price",
    "scale_units",
    "load_lots",
]


# stdlib imports
from decimal import Decimal
import functools
import itertools
from typing import TYPE_CHECKING, Tuple, List, Iterable, Callable, Optional, Union


# local imports
from capgains import models, utils
from .types import (
    Lot,
    Gain,
    ReturnOfCapital,
    Transfer,
    Spinoff,
    Exercise,
    TransactionType,
)
from . import predicates
from . import sortkeys

# Avoid recursive imports inventory.api <-> inventory.functions
# We only need inventory.api namespace for type annotations.
if TYPE_CHECKING:
    from .api import PortfolioType


def load_transaction(
    portfolio: PortfolioType,
    transaction: TransactionType,
    units: Decimal,
    cash: Decimal,
    currency: models.Currency,
    *,
    opentransaction: Optional[TransactionType] = None,
    sort: Optional[sortkeys.SortType] = None,
) -> List[Gain]:
    """Apply a Transaction to a Portfolio, opening/closing Lots as appropriate.

    Units/money values are sourced from args, not the Transaction itself, so that
    complex Transaction types (Transfer, Spinoff, Exercise) can apply basis from the
    source position to the destination position, bound to the Transaction.

    The Portfolio is modified in place as a side effect; return vals are realized Gains.

    Args:
        portfolio: map of (FiAccount, Security) to list of Lots.
        transaction: Transaction indicating destination (Fiaccount, Security).
        units: amount of Security to add to/subtract from position.
        cash: money amount (basis/proceeds) attributable to the units.
        currency: currency denomination of basis/proceeds
        opentransaction: opening Transaction of record (establishing holding period)
                         for any Lots created.  By default, use the Transaction itself.
        sort: sort algorithm for gain recognition e.g. FIFO, used to order closed Lots.

    Returns:
        A sequence of Gain instances, reflecting Lots closed by the Transaction.
    """
    pocket = (transaction.fiaccount, transaction.security)
    position = portfolio.get(pocket, [])
    position.sort(**(sort or sortkeys.FIFO))

    price = abs(cash / units)

    # First remove existing Position Lots closed by the Transaction.
    lotsClosed, position = part_units(
        position=position,
        predicate=predicates.closable(units, transaction.datetime),
        max_units=-units,
    )

    # Units not consumed in closing existing Lots are applied as basis in a new Lot.
    units += sum([lot.units for lot in lotsClosed])
    if units != 0:
        newLot = Lot(
            opentransaction=opentransaction or transaction,
            createtransaction=transaction,
            units=units,
            price=price,
            currency=currency,
        )
        position.append(newLot)

    portfolio[pocket] = position

    # Bind closed Lots to realizing Transaction to generate Gains.
    return [Gain(lot=lot, transaction=transaction, price=price) for lot in lotsClosed]


def load_lots(
    portfolio: PortfolioType,
    transaction: Union[Transfer, Spinoff, Exercise, models.Transaction],
    lots: Iterable[Lot],
    sort: Optional[sortkeys.SortType] = None,
) -> List[Gain]:
    """Apply a sequence of Lots holding units/basis to Portfolio, bound to Transaction.

    Used by complex Transactions (Transfer, Spinoff, Exercise) as the second step
    to transfer basis extracted from source position to destination position.

    Args:
        portfolio: map of (FI account, security) to list of Lots.
        transaction: Transaction indicating destination (Fiaccount, Security).
        lots: list of Lots; doesn't need to be sorted.
        sort: sort algorithm for gain recognition e.g. FIFO, used to order closed Lots.
        extra_price: additional
    """
    gains = (
        load_transaction(
            portfolio=portfolio,
            transaction=transaction,
            units=lot.units,
            currency=lot.currency,
            cash=lot.price * -lot.units,
            opentransaction=lot.opentransaction,
            sort=sort,
        )
        for lot in lots
    )
    return list(itertools.chain.from_iterable(gains))


def part_units(
    position: List[Lot],
    predicate: Optional[predicates.PredicateType] = None,
    max_units: Optional[Decimal] = None,
) -> Tuple[List[Lot], List[Lot]]:
    """Partition Lots according to some predicate, limiting max units taken.

    Note:
        If `max_units` is set, then the caller must ensure that `predicate` only
        matches Lots whose units are the same sign as `max_units`.

    Args:
        position: list of Lots; must be presorted by caller.
        predicate: filter function that accepts a Lot instance and returns bool,
                   e.g. openAsOf(datetime) or closableBy(transaction).
                   By default, matches everything.
        max_units: limit of units matching predicate to take.  Sign convention
                   is SAME SIGN as Lot, i.e. `max_units` is positive for long,
                   negative for short. By default, take all units that match predicate.

    Returns:
        (matching Lots, nonmatching Lots)
    """

    if predicate is None:
        predicate = utils.matchEverything

    Accumulator = Tuple[List[Lot], List[Lot], Optional[Decimal]]

    def make_accum(
        predicate: predicates.PredicateType
    ) -> Callable[[Accumulator, Lot], Accumulator]:
        """Factory to produce accumulator function from predicate"""

        def accum_part(accum: Accumulator, lot: Lot) -> Accumulator:
            taken, left, units_remain = accum

            # Failing the predicate trumps any consideration of max_units.
            if not predicate(lot):
                left.append(lot)
            # All cases below here have matched the predicate.
            # Now consider max_units constraint.
            elif units_remain is None:
                # args passed in max_units=None -> take all predicate matches
                taken.append(lot)
            elif units_remain == 0:
                # max_units already filled; we're done.
                left.append(lot)
            else:
                # Predicate matched; max_units unfilled.
                # Take all units we can until we run out of Lot.units or max_units.
                assert lot.units * units_remain > 0
                if lot.units / units_remain <= 1:
                    # Taking the whole Lot won't exceed max_units (but might reach it).
                    units_remain -= lot.units
                    taken.append(lot)
                else:
                    # The Lot more than suffices to fulfill max_units -> split the Lot
                    take, leave = (
                        lot._replace(units=units_remain),
                        lot._replace(units=lot.units - units_remain),
                    )
                    taken.append(take)
                    left.append(leave)
                    units_remain = Decimal("0")

            return taken, left, units_remain

        return accum_part

    initial: Accumulator = ([], [], max_units)
    return functools.reduce(make_accum(predicate), position, initial)[:-1]


def part_basis(
    position: List[Lot],
    predicate: Optional[predicates.PredicateType],
    fraction: Decimal,
) -> Tuple[List[Lot], List[Lot]]:
    """Remove a fraction of the cost from each Lot matching a predicate.

    Args:
        position: list of Lots; doesn't need to be sorted.
        predicate: filter function that accepts a Lot instance and returns bool,
                   e.g. openAsOf(datetime) or closableBy(transaction).
                   By default, matches everything.
        fraction: portion of cost to remove from each Lot matching predicate.

    Returns:
        2-tuple of:
            0) list of Lots (copies of Lots meeting predicate, with each
               price updated to reflect the basis removed from the original).
            1) list of Lots (original Lots, less basis removed).

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

    Accumulator = Tuple[List[Lot], List[Lot]]

    def make_accum(
        predicate: predicates.PredicateType
    ) -> Callable[[Accumulator, Lot], Accumulator]:
        """Factory to produce accumulator function from predicate"""

        def accum_part(accum: Accumulator, lot: Lot) -> Accumulator:
            taken, left = accum
            if not predicate(lot):
                left.append(lot)
            else:
                takenprice = lot.price * fraction
                taken.append(lot._replace(price=takenprice))
                left.append(lot._replace(price=lot.price - takenprice))
            return taken, left

        return accum_part

    initial: Accumulator = ([], [])
    return functools.reduce(make_accum(predicate), position, initial)


def adjust_price(
    lots: List[Lot], transaction: Union[ReturnOfCapital, Exercise, models.Transaction]
) -> Tuple[List[Lot], List[Gain]]:
    """Apply Transaction cash pro rata to directly adjust cost basis of Lots.

    If reducing basis, cost has a floor of zero; realize Gain for portion of cash that
    would reduce basis below zero.

    Args:
        lots: sequence of Lots; doesn't need to be sorted.
        transaction: Transaction whose cash is a basis adjustment.

    Returns:
        2-tuple of:
            0) list of Lots, with each price adjusted.
            1) list of Gains realized by Transaction.
    """

    assert lots
    assert isinstance(transaction.cash, Decimal)
    # FIXME - test case of short Lots (negative units) for sign of priceChange
    priceChange = transaction.cash / sum(lot.units for lot in lots)

    Accumulator = Tuple[List[Lot], List[Gain]]

    def make_accum(priceChange: Decimal) -> Callable[[Accumulator, Lot], Accumulator]:
        """Factory to produce accumulator function from price adjustment"""

        def accum_price_adj(accum: Accumulator, lot: Lot) -> Accumulator:
            lots, gains = accum

            new_price = lot.price - priceChange
            if new_price < 0:
                gains.append(Gain(lot=lot, transaction=transaction, price=priceChange))
                new_price = Decimal("0")
            lots.append(lot._replace(price=new_price))
            return lots, gains

        return accum_price_adj

    # Need annotation to determine type of list elements
    initial: Accumulator = ([], [])
    return functools.reduce(make_accum(priceChange), lots, initial)


def scale_units(
    lots: Iterable[Lot], ratio: Decimal
) -> Tuple[List[Lot], Decimal, Decimal]:
    """Scale Lot units by the given ratio.  Cost basis is not affected.

    Args:
        lots: list of Lots; doesn't need to be sorted.
        ratio: scaling factor, i.e. new units per old units.

    Returns:
        2-tuple of:
            0) list of Lots with each units scaled.
            1) units total pre-scaling
            2) units total post-scaling
    """
    Accumulator = Tuple[List[Lot], Decimal, Decimal]

    def make_accum(ratio: Decimal) -> Callable[[Accumulator, Lot], Accumulator]:
        """Factory to produce accumulator function from scaling ratio"""

        def accum_scale(accum: Accumulator, lot: Lot) -> Accumulator:
            lots, fromunits, units = accum

            units_ = lot.units * ratio
            price = lot.price / ratio

            lots.append(lot._replace(units=units_, price=price))
            fromunits += lot.units
            units += units_

            return lots, fromunits, units

        return accum_scale

    # Need annotation to determine type of list elements
    initial: Accumulator = ([], Decimal(0), Decimal(0))
    scaledLots, fromunits, units = functools.reduce(make_accum(ratio), lots, initial)
    return scaledLots, fromunits, units
