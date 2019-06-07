# coding: utf-8
"""
Data structures and functions for tracking units/cost history of financial assets.
Besides the fundamental requirement of keeping accurate tallies, the main purpose
of this module is to match opening and closing transactions in order to calculate
the amount and character of realized gains.

To use this module, create a Portfolio instance and pass TransactionType instances
to its book() method.  Alternatively, you can use any object that implements the
mapping protocol, and pass it to the module-level book() function.

Each Lot tracks the current state of a particular bunch of securities - (unit, cost).
Lots are collected in sequences called "positions", which are the values of a
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
# stdlib imports
from collections import defaultdict
from decimal import Decimal
import datetime as _datetime
import itertools
import functools
from typing import (
    NamedTuple,
    Tuple,
    List,
    Mapping,
    MutableMapping,
    Callable,
    Any,
    Optional,
    Union,
)


# local imports
from capgains import models, utils


class InventoryError(Exception):
    """ Base class for Exceptions defined in this module """


class Inconsistent(InventoryError):
    """Exception raised when a Position's state is inconsistent with Transaction.

    Args:
        transaction:
        msg:

    Attributes:
        transaction:
        msg:
    """

    def __init__(self, transaction: "TransactionType", msg: str) -> None:
        self.transaction = transaction
        self.msg = msg
        super(Inconsistent, self).__init__(f"{transaction} inconsistent: {msg}")


UNITS_RESOLUTION = Decimal("0.001")


#######################################################################################
# DATA MODEL
#######################################################################################
class Trade(NamedTuple):
    """Transaction to buy or sell a security.

    Attributes:
        id:
        uniqueid:
        datetime;
        fiaccount:
        security:
        cash:
        currency:
        units:
        dtsettle:
        memo:
        sort:
    """

    id: int
    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    cash: Decimal
    currency: str
    units: Decimal
    dtsettle: Optional[_datetime.datetime] = None
    memo: Optional[str] = None
    # sort's type is actually Optional[SortType], but mypy chokes on recursion
    sort: Optional[Mapping[str, Union[bool, Callable[[Any], Tuple]]]] = None


Trade.cash.__doc__ = "Change in money amount"
Trade.currency.__doc__ = "Currency denomination of cash (ISO 4217)"
Trade.units.__doc__ = "Change in security quantity"
Trade.dtsettle.__doc__ = "Settlement date/time"
Trade.sort.__doc__ = "Sort algorithm for gain recognition"


class ReturnOfCapital(NamedTuple):
    id: int
    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    cash: Decimal
    currency: str
    memo: Optional[str] = None
    dtsettle: Optional[_datetime.datetime] = None


ReturnOfCapital.cash.__doc__ = "Total amount of distribution"
ReturnOfCapital.currency.__doc__ = "Currency denomination of distribution (ISO 4217)"
ReturnOfCapital.dtsettle.__doc__ = "Payment date for distribution"


class Split(NamedTuple):
    id: int
    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    numerator: Decimal
    denominator: Decimal
    units: Decimal
    memo: Optional[str] = None


Split.numerator.__doc__ = "Normalized units of post-split security"
Split.denominator.__doc__ = "Normalized units of pre-slit security"
Split.units.__doc__ = "Change in security quantity"


class Transfer(NamedTuple):
    id: int
    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    units: Decimal
    fiaccountFrom: Any
    securityFrom: Any
    unitsFrom: Decimal
    memo: Optional[str] = None


Transfer.units.__doc__ = "Change in destination security quantity"
Transfer.fiaccountFrom.__doc__ = "Source FI account"
Transfer.securityFrom.__doc__ = "Source security"
Transfer.unitsFrom.__doc__ = "Change in source security quantity"


class Spinoff(NamedTuple):
    id: int
    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    units: Decimal
    numerator: Decimal
    denominator: Decimal
    securityFrom: Any
    memo: Optional[str] = None
    securityPrice: Optional[Decimal] = None
    securityFromPrice: Optional[Decimal] = None


Spinoff.units.__doc__ = "Change in destination security quantity"
Spinoff.numerator.__doc__ = "Normalized units of destination security"
Spinoff.denominator.__doc__ = "Normalized units of source security"
Spinoff.securityFrom.__doc__ = "Source security"
Spinoff.securityPrice.__doc__ = "FMV of destination security post-spin"
Spinoff.securityFromPrice.__doc__ = "FMV of source security post-spin"


class Exercise(NamedTuple):
    id: int
    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    units: Decimal
    cash: Decimal
    currency: str
    securityFrom: Any
    unitsFrom: Decimal
    memo: Optional[str] = None


Exercise.units.__doc__ = "Change in destination security quantity"
Exercise.cash.__doc__ = "Cash paid for exercise"
Exercise.securityFrom.__doc__ = "Source security"
Exercise.unitsFrom.__doc__ = "Change in source security quantity"


#  Type alias involving Transaction must be defined after Transaction itself,
#  else mypy chokes on the recursive definition.
TransactionType = Union[
    Trade, ReturnOfCapital, Split, Transfer, Spinoff, Exercise, models.Transaction
]


class Lot(NamedTuple):
    """ Cost basis/holding data container for a securities position """

    opentransaction: TransactionType
    createtransaction: TransactionType
    units: Decimal
    price: Decimal
    currency: str


Lot.opentransaction.__doc__ = "Transaction that began holding period"
Lot.createtransaction.__doc__ = "Transaction that created the Lot for current position"
Lot.units.__doc__ = "(Nonzero)"
Lot.price.__doc__ = "Per-unit cost (positive or zero)"
Lot.currency.__doc__ = "Currency denomination of cost price"


class Gain(NamedTuple):
    """
    Binds realizing Transaction to a Lot (and indirectly its opening Transaction)
    """

    lot: Lot
    transaction: Any
    price: Decimal


Gain.lot.__doc__ = "Lot instance for which gain is realized"
Gain.transaction.__doc__ = "Transaction instance realizing gain"
Gain.price.__doc__ = "Per-unit proceeds (positive or zero)"


# This type alias needs to be defined before it's used, or functools.register
# freaks out if it's used in a function signature.
SortType = Mapping[str, Union[bool, Callable[[Lot], Tuple]]]


#######################################################################################
# API
#######################################################################################
PortfolioType = MutableMapping


class Portfolio(defaultdict):
    """
    Mapping container for positions (i.e. sequences of Lots),
    keyed by (FI account, security) a/k/a "pocket".
    """

    default_factory = list

    def __init__(self, *args, **kwargs):
        args = (self.default_factory,) + args
        defaultdict.__init__(self, *args, **kwargs)

    def book(
        self, transaction: TransactionType, sort: Optional["SortType"] = None
    ) -> List[Gain]:
        """ Convenience method to call inventory.book() """
        return book(transaction, self, sort=sort)


@functools.singledispatch
def book(
    transaction,
    portfolio: PortfolioType,
    sort: Optional["SortType"] = None,
    opentransaction: Optional[TransactionType] = None,
    createtransaction: Optional[TransactionType] = None,
) -> List[Gain]:
    """
    Apply a Transaction to the appropriate position(s) in the Portfolio.
    """
    raise ValueError(f"Unknown transaction type {type(transaction)}")


@book.register
def book_model(
    transaction: models.Transaction,
    portfolio: PortfolioType,
    sort: Optional["SortType"] = None,
    opentransaction: Optional[TransactionType] = None,
    createtransaction: Optional[TransactionType] = None,
) -> List[Gain]:
    """
    Dispatch to handler function on models.Transaction.type value.
    """
    handlers = {
        models.TransactionType.RETURNCAP: returnofcapital,
        models.TransactionType.SPLIT: split,
        models.TransactionType.SPINOFF: spinoff,
        models.TransactionType.TRANSFER: transfer,
        models.TransactionType.TRADE: trade,
        models.TransactionType.EXERCISE: exercise,
    }
    handler = handlers[transaction.type]
    gains = handler(  # type: ignore
        transaction, portfolio, sort, opentransaction, createtransaction
    )
    return gains  # type: ignore


@book.register(Trade)
def trade(
    transaction: Union[Trade, models.Transaction],
    portfolio: PortfolioType,
    sort: Optional["SortType"] = None,
    opentransaction: Optional[TransactionType] = None,
    createtransaction: Optional[TransactionType] = None,
) -> List[Gain]:
    """
    Normal buy or sell, closing open Lots and realizing Gains.

    If ``opentransaction`` is passed in, it overrides lot.opentransaction
    to preserve holding period.

    If ``createtransaction`` is passed in, it overrides lot.createtransaction
    and gain.transaction.

    ``sort`` is a mapping of (key func, reverse) such as FIFO/MINGAIN etc.
    used to order Lots when closing them.
    """
    if transaction.units == 0:
        raise ValueError(f"units can't be zero: {transaction}")

    pocket = (transaction.fiaccount, transaction.security)
    position = portfolio.get(pocket, [])
    position.sort(**(sort or FIFO))

    # Remove closed lots from the position
    lots_closed, position = take_lots(
        transaction, position, closableBy(transaction), -transaction.units
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
def returnofcapital(
    transaction: Union[ReturnOfCapital, models.Transaction],
    portfolio: PortfolioType,
    sort=None,
    opentransaction: Optional[TransactionType] = None,
    createtransaction: Optional[TransactionType] = None,
) -> List[Gain]:
    """
    Apply cash to reduce cost basis of long position as of Transaction.datetime;
    realize Gain on Lots where cash proceeds exceed cost basis.

    ``sort`` is in the argument signature for applyTransaction(), but unused.
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
def split(
    transaction: Union[Split, models.Transaction],
    portfolio: PortfolioType,
    sort=None,
    opentransaction: Optional[TransactionType] = None,
    createtransaction: Optional[TransactionType] = None,
) -> List[Gain]:
    """
    Increase/decrease Lot units without affecting basis or realizing Gain.

    ``sort`` is in the argument signature for applyTransaction(), but unused.
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
def transfer(
    transaction: Union[Transfer, models.Transaction],
    portfolio: PortfolioType,
    sort: Optional["SortType"] = None,
    opentransaction: Optional[TransactionType] = None,
    createtransaction: Optional[TransactionType] = None,
) -> List[Gain]:
    """
    Move Lots from one Position to another, maybe changing Security/units.

    ``sort`` is a mapping of (key func, reverse) such as FIFO/MINGAIN etc.
    used to order Lots when closing them.
    """
    if transaction.units * transaction.unitsFrom >= 0:
        msg = f"units and unitsFrom aren't oppositely signed in {transaction}"
        raise ValueError(msg)

    pocketFrom = (transaction.fiaccountFrom, transaction.securityFrom)
    positionFrom = portfolio.get(pocketFrom, [])
    if not positionFrom:
        raise Inconsistent(transaction, f"No position in {pocketFrom}")
    positionFrom.sort(**(sort or FIFO))

    # Remove the Lots from the source position
    lotsFrom, positionFrom = take_lots(
        transaction,
        positionFrom,
        openAsOf(transaction.datetime),
        -transaction.unitsFrom,
    )

    openUnits = sum([lot.units for lot in lotsFrom])
    if abs(openUnits + transaction.unitsFrom) > UNITS_RESOLUTION:
        msg = (
            f"Position in {transaction.security} for FI account "
            f"{transaction.fiaccount} on {transaction.datetime} is only "
            f"{openUnits} units; can't transfer out {transaction.units} units."
        )
        raise Inconsistent(transaction, msg)

    portfolio[pocketFrom] = positionFrom

    transferRatio = -transaction.units / transaction.unitsFrom

    gains = (
        _transferBasis(portfolio, lotFrom, transaction, transferRatio, sort)
        for lotFrom in lotsFrom
    )
    return list(itertools.chain.from_iterable(gains))


@book.register(Spinoff)
def spinoff(
    transaction: Union[Spinoff, models.Transaction],
    portfolio: PortfolioType,
    sort: Optional["SortType"] = None,
    opentransaction: Optional[TransactionType] = None,
    createtransaction: Optional[TransactionType] = None,
) -> List[Gain]:
    """
    Remove cost from position to create a new position, preserving
    the holding period through the spinoff and not removing units or
    closing Lots from the source position.

    ``sort`` is a mapping of (key func, reverse) such as FIFO/MINGAIN etc.
    used to order Lots when closing them.
    """
    assert isinstance(transaction.units, Decimal)
    assert isinstance(transaction.numerator, Decimal)
    assert isinstance(transaction.denominator, Decimal)

    if transaction.numerator <= 0 or transaction.denominator <= 0:
        msg = f"numerator & denominator must be positive Decimals in {transaction}"
        raise ValueError(msg)

    pocketFrom = (transaction.fiaccount, transaction.securityFrom)
    positionFrom = portfolio.get(pocketFrom, [])
    if not positionFrom:
        raise Inconsistent(transaction, f"No position in {pocketFrom}")
    positionFrom.sort(**(sort or FIFO))

    spinRatio = Decimal(transaction.numerator) / Decimal(transaction.denominator)

    # costFraction is the fraction of original cost allocated to the spinoff,
    # with the balance allocated to the source position.
    if transaction.securityPrice is None or transaction.securityFromPrice is None:
        costFraction = Decimal("0")
    else:
        spinoffFMV = transaction.securityPrice * transaction.units
        spunoffFMV = transaction.securityFromPrice * transaction.units / spinRatio
        costFraction = spinoffFMV / (spinoffFMV + spunoffFMV)

    # Take the basis from the source Position
    lotsFrom, positionFrom = take_basis(
        positionFrom, openAsOf(transaction.datetime), costFraction
    )

    openUnits = sum([lot.units for lot in lotsFrom])
    if abs(openUnits * spinRatio - transaction.units) > UNITS_RESOLUTION:
        msg = (
            f"Spinoff {transaction.numerator} units {transaction.security} "
            f"for {transaction.denominator} units {transaction.securityFrom}:\n"
            f"To receive {transaction.units} units {transaction.security} "
            f"requires a position of {transaction.units / spinRatio} units of "
            f"{transaction.securityFrom} in FI account {transaction.fiaccount} "
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
def exercise(
    transaction: Union[Exercise, models.Transaction],
    portfolio: PortfolioType,
    sort: Optional["SortType"] = None,
    opentransaction: Optional[TransactionType] = None,
    createtransaction: Optional[TransactionType] = None,
) -> List[Gain]:
    """
    Exercise an option.

    ``sort`` is a mapping of (key func, reverse) such as FIFO/MINGAIN etc.
    used to order Lots when closing them.
    """
    unitsFrom = transaction.unitsFrom

    pocketFrom = (transaction.fiaccount, transaction.securityFrom)
    positionFrom = portfolio.get(pocketFrom, [])

    # Remove lots from the source position
    takenLots, remainingPosition = take_lots(
        transaction, positionFrom, openAsOf(transaction.datetime), -unitsFrom
    )

    takenUnits = sum([lot.units for lot in takenLots])
    assert isinstance(takenUnits, Decimal)
    if abs(takenUnits) - abs(unitsFrom) > UNITS_RESOLUTION:
        msg = f"Exercise Lot.units={takenUnits} (not {unitsFrom})"
        raise Inconsistent(transaction, msg)

    portfolio[pocketFrom] = remainingPosition

    multiplier = abs(transaction.units / transaction.unitsFrom)
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
    sort: Optional["SortType"],
    extra_basis: Optional[Decimal] = None,
    preserve_holding_period: bool = True,
) -> List[Gain]:
    """
    Apply cost basis removed from a position to new units of another position.
    Apply as a trade in order to close Lots of destination position as needed,
    preserving opentransaction from source Lot to maintain holding period.

    ``lot``: Lot instance recording the extracted cost basis and source units.

    ``transaction``: Transaction instance booking in the new position.

    ``ratio``: # of new position units to create for each unit of source position.

    ``sort`` is a mapping of (key func, reverse) such as FIFO/MINGAIN etc.
    used to order Lots when closing them.

    ``extra_basis`` is PER SHARE basis to be added to the cost transferred from the
    source position, e.g. payment of strike price for options exercise.

    ``preserve_holding_period``, if False, sets opentransaction to the transaction
    booking in the new position rather than maintaining the opentransaction from
    the source Lot.
    """
    costBasis = lot.price * lot.units
    if extra_basis is None:
        extra_basis = Decimal("0")
    costBasis += extra_basis

    if preserve_holding_period:
        opentransaction = lot.opentransaction
    else:
        opentransaction = transaction

    # FIXME - We need a Transaction.id for trade() to set
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
        opentransaction=opentransaction,
        createtransaction=transaction,
        sort=sort,
    )


#######################################################################################
# FUNCTIONS OPERATING ON LOTS
#######################################################################################
def part_lot(lot: Lot, units: Decimal) -> Tuple[Lot, Lot]:
    """
    Partition Lot at specified # of units, adding new Lot of leftover units.
    """
    if not abs(units) < abs(lot.units):
        msg = f"units={units} must have smaller magnitude than lot.units={lot.units}"
        raise ValueError(msg)
    if not units * lot.units > 0:
        msg = f"units={units} and lot.units={lot.units} must have same sign (non-zero)"
        raise ValueError(msg)
    return (lot._replace(units=units), lot._replace(units=lot.units - units))


def take_lots(
    transaction: TransactionType,
    lots: List[Lot],
    criterion: Optional["CriterionType"] = None,
    max_units: Optional[Decimal] = None,
) -> Tuple[List[Lot], List[Lot]]:
    """
    Remove a selection of Lots from the Position in sequential order.

    ``lots`` must be presorted by caller.

    ``criterion`` is a filter function that accepts a Lot instance and returns bool
    e.g. openAsOf(datetime) or closableBy(transaction). By default, matches everything.

    ``max_units`` is the limit of units matching criterion to take.  Sign convention
    is SAME SIGN as position, i.e. units arg must be positive for long, negative for
    short. By default, take all units that match criterion without limit.

    Returns (Lots matching criterion/max_units, nonmatching Lots)
    """
    if not lots:
        return [], []

    if criterion is None:

        def _criterion(lot):
            return True

        criterion = _criterion

    def take(lots: List[Lot], criterion: CriterionType, max_units: Optional[Decimal]):
        units_remain = max_units
        for lot in lots:
            if not criterion(lot):
                yield (None, lot)
            elif units_remain is None:
                yield (lot, None)
            elif units_remain == 0:
                yield (None, lot)
            else:
                if not lot.units * units_remain > 0:
                    msg = (
                        f"units_remain={units_remain} and Lot.units={lot.units} "
                        "must have same sign (nonzero)"
                    )
                    raise Inconsistent(transaction, msg)

                if abs(lot.units) <= abs(units_remain):
                    units_remain -= lot.units
                    yield (lot, None)
                else:
                    taken, left = part_lot(lot, units_remain)
                    units_remain -= taken.units
                    yield (taken, left)

    lots_taken, lots_left = zip(*take(lots, criterion, max_units))
    return (
        [lot for lot in lots_taken if lot is not None],
        [lot for lot in lots_left if lot is not None],
    )


def take_basis(
    lots: List[Lot], criterion: Optional["CriterionType"], fraction: Decimal
) -> Tuple[List[Lot], List[Lot]]:
    """
    Remove a fraction of the cost from each Lot in the Position.

    ``lots`` must be presorted by caller.

    ``criterion`` is a filter function that accepts a Lot instance and returns bool
    e.g. openAsOf(datetime) or closableBy(transaction). By default, matches everything.

    ``fraction`` is the portion of cost to take from each Lot

    Returns: 2-tuple of
            0) list of Lots (copies of Lots meeting criterion, with each
               price updated to reflect the basis removed)
            1) list of Lots (original position, less basis removed)
    """
    if criterion is None:

        def _criterion(lot):
            return True

        criterion = _criterion

    if not (0 <= fraction <= 1):
        msg = f"fraction must be between 0 and 1 (inclusive), not '{fraction}'"
        raise ValueError(msg)

    def take(
        lot: Lot, criterion: CriterionType, fraction: Decimal
    ) -> Tuple[Optional[Lot], Optional[Lot]]:
        if not criterion(lot):
            return (None, lot)
        takenprice = lot.price * fraction
        return (
            lot._replace(price=takenprice),
            lot._replace(price=lot.price - takenprice),
        )

    lots_taken, lots_left = zip(*(take(lot, criterion, fraction) for lot in lots))
    return (
        [lot for lot in lots_taken if lot is not None],
        [lot for lot in lots_left if lot is not None],
    )


#######################################################################################
# FILTER CRITERIA
#######################################################################################
CriterionType = Callable[[Lot], bool]


def openAsOf(datetime: _datetime.datetime) -> CriterionType:
    """
    Filter function that chooses Lots created on or before datetime
    """

    def isOpen(lot):
        return lot.createtransaction.datetime <= datetime

    return isOpen


def longAsOf(datetime: _datetime.datetime) -> CriterionType:
    """
    Filter function that chooses long Lots (i.e. positive units) created
    on or before datetime
    """

    def isOpen(lot):
        lot_open = lot.createtransaction.datetime <= datetime
        lot_long = lot.units > 0
        return lot_open and lot_long

    return isOpen


def closableBy(transaction: TransactionType) -> CriterionType:
    """
    Filter function that chooses Lots created on or before the given
    transaction.datetime, with sign opposite to the given transaction.units
    """

    def closeMe(lot):
        lot_open = lot.createtransaction.datetime <= transaction.datetime
        opposite_sign = lot.units * transaction.units < 0
        return lot_open and opposite_sign

    return closeMe


#######################################################################################
# SORT FUNCTIONS
#######################################################################################
def sort_oldest(lot: Lot) -> Tuple:
    """
    Sort by holding period, then by opening Transaction.uniqueid
    """
    opentx = lot.opentransaction
    return (opentx.datetime, opentx.uniqueid or "")


def sort_cheapest(lot: Lot) -> Tuple:
    """
    Sort by price, then by opening Transaction.uniqueid
    """
    # FIXME this doesn't sort stably for identical prices but different lots
    price = lot.price
    assert isinstance(price, Decimal)
    return (price, lot.opentransaction.uniqueid or "")


def sort_dearest(lot: Lot) -> Tuple:
    """
    Sort by inverse price, then by opening Transaction.uniqueid
    """
    # FIXME this doesn't sort stably for identical prices but different lots
    price = lot.price
    assert isinstance(price, Decimal)
    return (-price, lot.opentransaction.uniqueid or "")


FIFO = {"key": sort_oldest, "reverse": False}
LIFO = {"key": sort_oldest, "reverse": True}
MINGAIN = {"key": sort_dearest, "reverse": False}
MAXGAIN = {"key": sort_cheapest, "reverse": False}
