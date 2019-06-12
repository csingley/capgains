# coding: utf-8
"""
Besides the fundamental requirement of keeping accurate tallies, the main purpose
of this module is to match opening and closing transactions in order to calculate
the amount and character of realized gains.

To use this module, create a Portfolio instance and call its book() method, passing in
instances of type capgains.inventory.models.TransactionType.

Alternatively, you can use any object that implements the
mapping protocol, and pass it to the module-level book() function.

Each Lot tracks the current state of a particular bunch of securities - (unit, cost).
Lots are collected in lists called "positions", which are the values of a
Portfolio mapping keyed by a (FI account, security) tuple called a "pocket".

Additionally, each Lot keeps a reference to its opening Transaction, i.e. the
Transaction which started its holding period for tax purposes (to determine whether
the character of realized Gain is long-term or short-term).

Each Lot also keeps a reference to its "creating" Transaction, i.e. the Transaction
which added the Lot to its current pocket.  A Lot's security can be sourced from
Lot.createtransaction.security, and the account where it's custodied can be sourced
from Lot.createtransaction.fiaccount.  In the case of an opening trade, the
opening Transaction and the creating Transaction will be the same.  For transfers,
spin-offs, mergers, etc., these will be different.

Gains link opening Transactions to realizing Transactions - which are usually closing
Transactions, but may also come from return of capital distributions that exceed
cost basis.  Return of capital Transactions generally don't provide per-share
distribution information, so Gains must keep state for the realizing price.

To compute realized capital gains from a Gain instance:
    * Proceeds - gain.lot.units * gain.price
    * Basis - gain.lot.units * gain.lot.price
    * Holding period start - gain.lot.opentransaction.datetime
    * Holding period end - gain.transaction.datetime

Lots and Transactions are immutable, so you can rely on the accuracy of references
to them (e.g. Gain.lot; Lot.createtransaction).  Everything about a Lot (except
opentransaction) can be changed by Transactions; the changes are reflected in a
newly-created Lot, leaving the old Lot undisturbed.

Nothing in this module changes a Transaction or a Gain, once created.
"""

__all__ = [
    "Inconsistent",
    "UNITS_RESOLUTION",
    "Portfolio",
    "book",
    "book_model",
    "book_trade",
    "book_returnofcapital",
    "book_split",
    "book_transfer",
    "book_spinoff",
    "book_transfer",
]


# stdlib imports
from collections import defaultdict
from decimal import Decimal
import itertools
import functools
from typing import Tuple, List, MutableMapping, Any, Optional, Union


# local imports
from capgains import models, utils
from capgains.inventory import functions
from .types import (
    Lot,
    Gain,
    TransactionType,
    Trade,
    ReturnOfCapital,
    Transfer,
    Split,
    Spinoff,
    Exercise,
)
from .predicates import openAsOf, longAsOf, closableBy
from .sortkeys import SortType, FIFO


class InventoryError(Exception):
    """ Base class for Exceptions defined in this module """


class Inconsistent(InventoryError):
    """Exception raised when a Position's state is inconsistent with Transaction.

    Args:
        transaction: the transaction instance that couldn't be applied.
        msg: Error message detailing the inconsistency.

    Attributes:
        transaction: the transaction instance that couldn't be applied.
        msg: Error message detailing the inconsistency.
    """

    def __init__(self, transaction: "TransactionType", msg: str) -> None:
        self.transaction = transaction
        self.msg = msg
        super(Inconsistent, self).__init__(f"{transaction} inconsistent: {msg}")


UNITS_RESOLUTION = Decimal("0.001")
"""Significance threshold for difference between predicted units and reported units.

For transactions that involve scaling units by a ratio (i.e. Split & Spinoff), if the
product of that ratio and the total position Lot.units affected by the transaction
differs from the reported transaction.units by more than UNITS_RESOLUTION, then
an Inconsistent error is raised.
"""


class Portfolio(defaultdict):
    """Mapping container for securities positions (i.e. lists of Lot instances).

    Keyed by (FI account, security) a/k/a "pocket".

    Note:
        Any object implementing the mapping protocol may be used with the functions in
        this module.  It's convenient to inherit from collections.defaultdict.
    """

    default_factory = list

    def __init__(self, *args, **kwargs):
        args = (self.default_factory,) + args
        defaultdict.__init__(self, *args, **kwargs)

    def book(
        self, transaction: TransactionType, sort: Optional[SortType] = None
    ) -> List[Gain]:
        """Convenience method to call inventory.book()

        Args:
            transaction: the transaction to apply to the Portfolio.
            sort: sort algorithm for gain recognition.

        Returns:
            A sequence of Gain instances, reflecting Lots closed by the transaction.
        """
        return book(transaction, self, sort=sort)


FiAccount = Any
Security = Any
PortfolioType = MutableMapping[Tuple[FiAccount, Security], List[Lot]]


@functools.singledispatch
def book(transaction, *args, **kwargs) -> List[Gain]:
    """Apply a Transaction to the appropriate position(s) in the Portfolio.

    Dispatch to handler function below based on type of transaction.

    Raises:
        ValueError: if functools.singledispatch doesn't have a handler registered
                    for the transaction type.
    """

    raise ValueError(f"Unknown transaction type {type(transaction)}")


@book.register
def book_model(
    transaction: models.Transaction,
    portfolio: PortfolioType,
    *,
    sort: Optional[SortType] = None,
    opentransaction: Optional[TransactionType] = None,
    createtransaction: Optional[TransactionType] = None,
) -> List[Gain]:
    """Apply a models.Transaction to the appropriate position(s) in the Portfolio.

    models.Transaction doesn't have subclasses, so dispatch based on the instance's
    type attribute.

    Args:
        transaction: the transaction to apply to the Portfolio.
        portfolio: map of (FI account, security) to list of Lots.
        sort: sort algorithm for gain recognition e.g. FIFO, used to order closed Lots.
        opentransaction: if present, overrides transaction in any Lots created to
                         preserve holding period.
        createtransaction: if present, overrides transaction in any Lots/Gains created.

    Returns:
        A sequence of Gain instances, reflecting Lots closed by the transaction.
    """

    handlers = {
        models.TransactionType.TRADE: book_trade,
        models.TransactionType.RETURNCAP: book_returnofcapital,
        models.TransactionType.SPLIT: book_split,
        models.TransactionType.TRANSFER: book_transfer,
        models.TransactionType.SPINOFF: book_spinoff,
        models.TransactionType.EXERCISE: book_exercise,
    }
    handler = handlers[transaction.type]
    gains = handler(  # type: ignore
        transaction, portfolio, sort, opentransaction, createtransaction
    )
    return gains  # type: ignore


@book.register(Trade)
def book_trade(
    transaction: Union[Trade, models.Transaction],
    portfolio: PortfolioType,
    *,
    sort: Optional[SortType] = None,
    opentransaction: Optional[TransactionType] = None,
    createtransaction: Optional[TransactionType] = None,
) -> List[Gain]:
    """Apply a Trade to the appropriate position(s) in the Portfolio.

    Note:
        `opentransaction` and `createtransaction` are only provided as hooks for
        `_transferBasis()` and should not normally be used.

    Args:
        transaction: the transaction to apply to the Portfolio.
        portfolio: map of (FI account, security) to list of Lots.
        sort: sort algorithm for gain recognition e.g. FIFO, used to order closed Lots.
        opentransaction: if present, overrides transaction in any Lots created to
                         preserve holding period.
        createtransaction: if present, overrides transaction in any Lots/Gains created.

    Returns:
        A sequence of Gain instances, reflecting Lots closed by the transaction.

    Raises:
        ValueError: if `Trade.units` is zero.
    """
    if transaction.units == 0:
        raise ValueError(f"units can't be zero: {transaction}")

    pocket = (transaction.fiaccount, transaction.security)
    position = portfolio.get(pocket, [])
    position.sort(**(sort or FIFO))

    # Remove closed lots from the position
    lots_closed, position = functions.part_units(
        position, closableBy(transaction), -transaction.units
    )

    units = transaction.units + sum([lot.units for lot in lots_closed])
    price = abs(transaction.cash / transaction.units)
    if units != 0:
        newLot = Lot(
            opentransaction=opentransaction or transaction,
            createtransaction=createtransaction or transaction,
            units=units,
            price=price,
            currency=transaction.currency,
        )
        position.append(newLot)

    portfolio[pocket] = position

    return [
        Gain(lot=lot, transaction=createtransaction or transaction, price=price)
        for lot in lots_closed
    ]


@book.register(ReturnOfCapital)
def book_returnofcapital(
    transaction: Union[ReturnOfCapital, models.Transaction],
    portfolio: PortfolioType,
    **_,
) -> List[Gain]:
    """Apply a ReturnOfCapital to the appropriate position(s) in the Portfolio.

    Args:
        transaction: the transaction to apply to the Portfolio.
        portfolio: map of (FI account, security) to list of Lots.

    Returns:
        A sequence of Gain instances, reflecting Lots closed by the transaction.

    Raises:
        Inconsistent: if no position in the `portfolio` as of `ReturnOfCapital.datetime`
                      is found to receive the distribution.
    """
    pocket = (transaction.fiaccount, transaction.security)
    position = portfolio.get(pocket, [])

    # First get a total of shares affected by return of capital,
    # in order to determine return of capital per share
    unaffected, affected = utils.partition(longAsOf(transaction.datetime), position)
    affected = list(affected)
    if not affected:
        msg = (
            f"Return of capital {transaction}:\n"
            f"FI account {transaction.fiaccount} has no long position in "
            f"{transaction.security} as of {transaction.datetime}"
        )
        raise Inconsistent(transaction, msg)

    unitROC = transaction.cash / sum([lot.units for lot in affected])

    def reduceBasis(lot: Lot, unitROC: Decimal) -> Tuple[Lot, Optional[Gain]]:
        gain = None
        newBasis = lot.price - unitROC
        if newBasis < 0:
            gain = Gain(lot=lot, transaction=transaction, price=unitROC)
            newBasis = Decimal("0")
        return (lot._replace(price=newBasis), gain)

    basisReduced, gains = zip(*(reduceBasis(lot, unitROC) for lot in affected))
    portfolio[pocket] = list(basisReduced) + list(unaffected)
    return [gain for gain in gains if gain is not None]


@book.register(Split)
def book_split(
    transaction: Union[Split, models.Transaction], portfolio: PortfolioType, **_
) -> List[Gain]:
    """Apply a Split to the appropriate position(s) in the Portfolio.

    Args:
        transaction: the transaction to apply to the Portfolio.
        portfolio: map of (FI account, security) to list of Lots.

    Returns:
        A sequence of Gain instances, reflecting Lots closed by the transaction.

    Raises:
        Inconsistent: if the relevant position in the `portfolio`, when adjusted for
                      the split ratio, wouldn't cause a share delta that matches
                      `Split.units`.
    """
    splitRatio = transaction.numerator / transaction.denominator

    pocket = (transaction.fiaccount, transaction.security)
    position = portfolio.get(pocket, [])

    if not position:
        msg = (
            f"Split {transaction.security} "
            f"{transaction.numerator}:{transaction.denominator} "
            f"on {transaction.datetime} -\n"
            f"No position in FI account {transaction.fiaccount}"
        )
        raise Inconsistent(transaction, msg)

    unaffected, affected = utils.partition(openAsOf(transaction.datetime), position)

    def _split(lot: Lot, ratio: Decimal) -> Tuple[Lot, Decimal]:
        """ Returns (post-split Lot, original Units) """
        units = lot.units * ratio
        price = lot.price / ratio
        return (lot._replace(units=units, price=price), lot.units)

    position_new, origUnits = zip(*(_split(lot, splitRatio) for lot in affected))
    newUnits = sum([lot.units for lot in position_new]) - sum(origUnits)
    if abs(newUnits - transaction.units) > UNITS_RESOLUTION:
        msg = (
            f"Split {transaction.security} "
            f"{transaction.numerator}:{transaction.denominator} -\n"
            f"To receive {transaction.units} units {transaction.security} "
            f"requires a position of {transaction.units / splitRatio} units of "
            f"{transaction.security} in FI account {transaction.fiaccount} "
            f"on {transaction.datetime}, not units={origUnits}"
        )
        raise Inconsistent(transaction, msg)

    portfolio[pocket] = list(position_new) + list(unaffected)

    # Stock splits don't realize Gains
    return []


@book.register(Transfer)
def book_transfer(
    transaction: Union[Transfer, models.Transaction],
    portfolio: PortfolioType,
    *,
    sort: Optional[SortType] = None,
) -> List[Gain]:
    """Apply a Transfer to the appropriate position(s) in the Portfolio.

    Args:
        transaction: the transaction to apply to the Portfolio.
        portfolio: map of (FI account, security) to list of Lots.
        sort: sort algorithm for gain recognition e.g. FIFO, used to order closed Lots.

    Returns:
        A sequence of Gain instances, reflecting Lots closed by the transaction.

    Raises:
        ValueError: if `Transfer.units` and `Transfer.unitsfrom` aren't
                    oppositely signed.
        Inconsistent: if the relevant position in `portfolio` is insufficient to
                      satisfy `Transfer.unitsfrom`.
    """

    if transaction.units * transaction.unitsfrom >= 0:
        msg = f"units and unitsfrom aren't oppositely signed in {transaction}"
        raise ValueError(msg)

    pocketFrom = (transaction.fiaccountfrom, transaction.securityfrom)
    positionFrom = portfolio.get(pocketFrom, [])
    if not positionFrom:
        raise Inconsistent(transaction, f"No position in {pocketFrom}")
    positionFrom.sort(**(sort or FIFO))

    # Remove the Lots from the source position
    lotsFrom, positionFrom = functions.part_units(
        positionFrom, openAsOf(transaction.datetime), -transaction.unitsfrom
    )

    openUnits = sum([lot.units for lot in lotsFrom])
    if abs(openUnits + transaction.unitsfrom) > UNITS_RESOLUTION:
        msg = (
            f"Position in {transaction.security} for FI account "
            f"{transaction.fiaccount} on {transaction.datetime} is only "
            f"{openUnits} units; can't transfer out {transaction.units} units."
        )
        raise Inconsistent(transaction, msg)

    portfolio[pocketFrom] = positionFrom

    transferRatio = -transaction.units / transaction.unitsfrom

    gains = (
        _transferBasis(portfolio, lotFrom, transaction, transferRatio, sort)
        for lotFrom in lotsFrom
    )
    return list(itertools.chain.from_iterable(gains))


@book.register(Spinoff)
def book_spinoff(
    transaction: Union[Spinoff, models.Transaction],
    portfolio: PortfolioType,
    *,
    sort: Optional[SortType] = None,
) -> List[Gain]:
    """Apply a Spinoff to the appropriate position(s) in the Portfolio.

    Args:
        transaction: the transaction to apply to the Portfolio.
        portfolio: map of (FI account, security) to list of Lots.
        sort: sort algorithm for gain recognition e.g. FIFO, used to order closed Lots.

    Returns:
        A sequence of Gain instances, reflecting Lots closed by the transaction.

    Raises:
        ValueError: if either `Spinoff.numerator` or `Spinoff.denominator` isn't a
                    positive number.
        Inconsistent: if the relevant position in `portfolio` as of `Spinoff.datetime`,
                      when adjusted for the spinoff ratio, wouldn't produce a change in
                      # units that matches `Spinoff.units`.
    """

    if transaction.numerator <= 0 or transaction.denominator <= 0:
        msg = f"numerator & denominator must be positive Decimals in {transaction}"
        raise ValueError(msg)

    pocketFrom = (transaction.fiaccount, transaction.securityfrom)
    positionFrom = portfolio.get(pocketFrom, [])
    if not positionFrom:
        raise Inconsistent(transaction, f"No position in {pocketFrom}")
    positionFrom.sort(**(sort or FIFO))

    spinRatio = Decimal(transaction.numerator) / Decimal(transaction.denominator)

    # costFraction is the fraction of original cost allocated to the spinoff,
    # with the balance allocated to the source position.
    if transaction.securityprice is None or transaction.securityfromprice is None:
        costFraction = Decimal("0")
    else:
        spinoffFMV = transaction.securityprice * transaction.units
        spunoffFMV = transaction.securityfromprice * transaction.units / spinRatio
        costFraction = spinoffFMV / (spinoffFMV + spunoffFMV)

    # Take the basis from the source Position
    lotsFrom, positionFrom = functions.part_basis(
        positionFrom, openAsOf(transaction.datetime), costFraction
    )

    openUnits = sum([lot.units for lot in lotsFrom])
    if abs(openUnits * spinRatio - transaction.units) > UNITS_RESOLUTION:
        msg = (
            f"Spinoff {transaction.numerator} units {transaction.security} "
            f"for {transaction.denominator} units {transaction.securityfrom}:\n"
            f"To receive {transaction.units} units {transaction.security} "
            f"requires a position of {transaction.units / spinRatio} units of "
            f"{transaction.securityfrom} in FI account {transaction.fiaccount} "
            f"on {transaction.datetime}, not units={openUnits}"
        )
        raise Inconsistent(transaction, msg)

    portfolio[pocketFrom] = positionFrom

    gains = (
        _transferBasis(portfolio, lotFrom, transaction, spinRatio, sort)
        for lotFrom in lotsFrom
    )
    return list(itertools.chain.from_iterable(gains))


@book.register(Exercise)
def book_exercise(
    transaction: Union[Exercise, models.Transaction],
    portfolio: PortfolioType,
    *,
    sort: Optional[SortType] = None,
) -> List[Gain]:
    """Apply an Exercise transaction to the appropriate position(s) in the Portfolio.

    Args:
        transaction: the transaction to apply to the Portfolio.
        portfolio: map of (FI account, security) to list of Lots.
        sort: sort algorithm for gain recognition e.g. FIFO, used to order closed Lots.

    Returns:
        A sequence of Gain instances, reflecting Lots closed by the transaction.

    Raises:
        Inconsistent: if the relevant position in `portfolio` as of `Exercise.datetime`
                      doesn't contain enough units of the option to satisfy the
                      `Exercise.units`.
    """

    unitsfrom = transaction.unitsfrom

    pocketFrom = (transaction.fiaccount, transaction.securityfrom)
    positionFrom = portfolio.get(pocketFrom, [])

    # Remove lots from the source position
    takenLots, remainingPosition = functions.part_units(
        positionFrom, openAsOf(transaction.datetime), -unitsfrom
    )

    takenUnits = sum([lot.units for lot in takenLots])
    if abs(takenUnits) - abs(unitsfrom) > UNITS_RESOLUTION:
        msg = f"Exercise Lot.units={takenUnits} (not {unitsfrom})"
        raise Inconsistent(transaction, msg)

    portfolio[pocketFrom] = remainingPosition

    multiplier = abs(transaction.units / transaction.unitsfrom)
    strikePrice = abs(transaction.cash / transaction.units)

    gains = (
        _transferBasis(
            portfolio,
            lot,
            transaction,
            multiplier,
            sort,
            extra_basis=lot.units * multiplier * strikePrice,
            preserve_holding_period=False,
        )
        for lot in takenLots
    )
    return list(itertools.chain.from_iterable(gains))


def _transferBasis(
    portfolio: PortfolioType,
    lot: Lot,
    transaction: TransactionType,
    ratio: Decimal,
    sort: Optional[SortType],
    extra_basis: Optional[Decimal] = None,
    preserve_holding_period: bool = True,
) -> List[Gain]:
    """Apply cost basis removed from one position to new units of another position.

    Apply as a trade in order to close Lots of destination position as needed,
    preserving opentransaction from source Lot to maintain holding period.

    Args:
        portfolio: map of (FI account, security) to list of Lots.
        lot: Lot instance recording the extracted cost basis and source units.
        transaction: transaction booking in the new position.
        ratio: # of new position units to create for each unit of source position.
        sort: sort algorithm for gain recognition e.g. FIFO, used to order closed Lots.
        extra_basis: PER SHARE basis to be added to the cost transferred from the
                     source position, e.g. payment of strike price for options exercise.
        preserve_holding_period: if False, sets opentransaction to the transaction
                                 booking in the new position rather than maintaining
                                 the opentransaction from the source Lot.

    Returns:
        A sequence of Gain instances, reflecting Lots closed by the transaction.
    """

    costBasis = lot.price * lot.units
    if extra_basis is None:
        extra_basis = Decimal("0")
    costBasis += extra_basis

    if preserve_holding_period:
        opentransaction = lot.opentransaction
    else:
        opentransaction = transaction

    # FIXME - We need a Transaction.id for book_trade() to set
    # Lot.createtxid, but "id=transaction.id" is problematic.
    trade_ = Trade(
        id=transaction.id,
        uniqueid=transaction.uniqueid,
        datetime=transaction.datetime,
        memo=transaction.memo,
        currency=lot.currency,
        cash=-costBasis,
        fiaccount=transaction.fiaccount,
        security=transaction.security,
        units=lot.units * ratio,
    )
    return book(
        trade_,
        portfolio,
        sort=sort,
        opentransaction=opentransaction,
        createtransaction=transaction,
    )
