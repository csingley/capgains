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
    Sequence,
    Mapping,
    MutableMapping,
    Callable,
    Generator,
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
differs from the reported transaction.units by at least UNITS_RESOLUTION, then
an Inconsistent error is raised.
"""


########################################################################################
#  DATA MODEL
########################################################################################
class Trade(NamedTuple):
    """Buy/sell a security, creating basis (if opening) or realizing gain (if closing).

    Attributes:
        id: database primary key of transaction.
        uniqueid: transaction unique identifier (e.g. FITID).
        datetime: trade date/time.
        fiaccount: brokerage account where the trade is executed.
        security: asset bought/sold.
        cash: change in money amount (+ increases cash, - decreases cash).
        currency: currency denomination of cash (ISO 4217 code).
        units: change in security amount.
        dtsettle: trade settlement date/time.
        memo: transaction notes.
        sort: sort algorithm for gain recognition.
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


class ReturnOfCapital(NamedTuple):
    """Cash distribution that reduces cost basis.

    Attributes:
        id: database primary key of transaction.
        uniqueid: transaction unique identifier (e.g. FITID).
        datetime: effective date/time of distribution (ex-date).
        fiaccount: brokerage account where the distribution is received.
        security: security making the distribution.
        cash: amount of distribution (+ increases cash, - decreases cash).
        currency: currency denomination of cash (ISO 4217 code).
        dtsettle: pay date of distribution.
        memo: transaction notes.
    """

    id: int
    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    cash: Decimal
    currency: str
    dtsettle: Optional[_datetime.datetime] = None
    memo: Optional[str] = None


class Transfer(NamedTuple):
    """Move assets between (fiaccount, security) pockets, retaining basis/ open date.

    Units can also be changed during the Transfer, so it's useful for corporate
    reorganizations (mergers, etc.)

    Attributes:
        id: database primary key of transaction.
        uniqueid: transaction unique identifier (e.g. FITID).
        datetime: transfer date/time.
        fiaccount: destination brokerage account.
        security: destination security.
        fiaccount: destination brokerage account.
        security: destination security.
        units: destination security amount.
        fiaccountFrom: source brokerage account.
        securityFrom: source security.
        unitsFrom: source security amount.
        memo: transaction notes.
    """

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


class Split(NamedTuple):
    """Change position units without affecting costs basis or holding period.

    Splits are declared in terms of new units : old units (the split ratio).

    Note:
        A Split is essentially a Transfer where fiaccount=fiaccountFrom and
        security=SecurityFrom, differing only in units/unitsFrom.

    Attributes:
        id: database primary key of transaction.
        uniqueid: transaction unique identifier (e.g. FITID).
        datetime: effective date/time of split (ex-date).
        fiaccount: brokerage account where the split happens.
        security: security being split.
        numerator: normalized units of post-split security.
        denominator: normalized units of pre-slit security.
        units: change in security amount resulting from the split.
        memo: transaction notes.
    """

    id: int
    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    numerator: Decimal
    denominator: Decimal
    units: Decimal
    memo: Optional[str] = None


class Spinoff(NamedTuple):
    """Turn one security into two, partitioning cost basis between them.

    Spinoffs are declared in terms of (units new security) : (units original security).
    Per the US tax code, cost basis must be divided between the two securities
    positions in proportion to their fair market value.  For exchange-traded securities
    this is normally derived from market prices immediately after the spinoff.  This
    pricing isn't known at the time of the spinoff; securityPrice and securityFromPrice
    must be edited in after market data becomes available.

    Attributes:
        id: database primary key of transaction.
        uniqueid: transaction unique identifier (e.g. FITID).
        datetime: effective date/time of spinoff (ex-date).
        fiaccount: brokerage account where the spinoff happens.
        security: destination security (i.e. new spinoff security).
        units: amount of destination security received.
        numerator: normalized units of destination security.
        denominator: normalized units of source security.
        securityFrom: source security (i.e. security subject to spinoff).
        memo: transaction notes.
        securityPrice: unit price used to fair-value source security.
        securityFromPrice: unit price used to fair-value destination security.
    """

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


class Exercise(NamedTuple):
    """Exercise a securities option, buying/selling the underlying security.

    Exercising an option removes its units from the FI account and rolls its cost
    basis into the underlying.  For tax purposes, the holding period for the
    underlying begins at exercise, not at purchase of the option.

    Attributes:
        id: database primary key of transaction.
        uniqueid: transaction unique identifier (e.g. FITID).
        datetime: date/time of option exercise.
        fiaccount: brokerage account where the option is exercised.
        security: destination security (i.e. underlying received via exercise).
        units: change in amount of destination security (i.e. the underlying).
        cash: net exercise payment (+ long put/short call; - long call/short put)
        currency: currency denomination of cash (ISO 4217 code).
        securityFrom: source security (i.e. the option).
        unitsFrom: change in mount of source security (i.e. the option).
        memo: transaction notes.
    """

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


TransactionType = Union[
    Trade, ReturnOfCapital, Split, Transfer, Spinoff, Exercise, models.Transaction
]
"""Type alias for classes implementing the Transaction interface.

Includes models.Transaction, as well as the NamedTuple subclasses defined above that
implement each relevant subset of Transaction attributes.

This type alias must be defined after the transaction NamedTuples to which it refers,
because mypy can't yet handle the recursive type definition.
"""


class Lot(NamedTuple):
    """Cost basis/holding data container for a securities position.

    Attributes:
        opentransaction: transaction creating basis, which began tax holding period.
        createtransaction: transaction booking Lot into current (account, security).
        units: amount of security comprising the Lot (must be nonzero).
        price: per-unit cost basis (must be positive or zero).
        currency: currency denomination of price (ISO 4217 code).
    """

    opentransaction: TransactionType
    createtransaction: TransactionType
    units: Decimal
    price: Decimal
    currency: str


class Gain(NamedTuple):
    """Binds realizing Transaction to a Lot (and indirectly its opening Transaction).

    Note:
        Realizing Transactions are usually closing Transactions, but may also come from
        return of capital distributions that exceed cost basis.  Return of capital
        Transactions generally don't provide per-share distribution information, so
        Gains must keep state for the realizing price.

    Attributes:
        lot: Lot instance for which gain is realized.
        transaction: Transaction instance realizing gain.
        price: per-unit cash amount of the realizing transaction.
    """

    lot: Lot
    transaction: Any
    price: Decimal


# This type alias needs to be defined before it's used, or functools.register
# freaks out if it's used in a function signature.
SortType = Mapping[str, Union[bool, Callable[[Lot], Tuple]]]


########################################################################################
#  API
########################################################################################
PortfolioType = MutableMapping


class Portfolio(defaultdict):
    """Mapping container for securities positions (i.e. sequences of Lot instances).

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
        self, transaction: TransactionType, sort: Optional["SortType"] = None
    ) -> List[Gain]:
        """Convenience method to call inventory.book()

        Args:
            transaction: the transaction to apply to the Portfolio.
            sort: sort algorithm for gain recognition.

        Returns:
            A sequence of Gain instances, reflecting Lots closed by the transaction.
        """
        return book(transaction, self, sort=sort)


@functools.singledispatch
def book(
    transaction,
    portfolio: PortfolioType,
    sort: Optional["SortType"] = None,
    opentransaction: Optional[TransactionType] = None,
    createtransaction: Optional[TransactionType] = None,
) -> List[Gain]:
    """Apply a Transaction to the appropriate position(s) in the Portfolio.

    Dispatch to handler function below based on type of transaction.

    Args:
        transaction: the transaction to apply to the Portfolio.
        portfolio: map of (FI account, security) to sequence of Lots.
        sort: sort algorithm for gain recognition e.g. FIFO, used to order closed Lots.
        opentransaction: if present, overrides transaction in any Lots created to
                         preserve holding period.
        createtransaction: if present, overrides transaction in any Lots/Gains created.

    Returns:
        A sequence of Gain instances, reflecting Lots closed by the transaction.

    Raises:
        ValueError: if functools.singledispatch doesn't have a handler registered
                    for the transaction type.
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
    """Apply a models.Transaction to the appropriate position(s) in the Portfolio.

    models.Transaction doesn't have subclasses, so dispatch based on the instance's
    type attribute.

    Args:
        transaction: the transaction to apply to the Portfolio.
        portfolio: map of (FI account, security) to sequence of Lots.
        sort: sort algorithm for gain recognition e.g. FIFO, used to order closed Lots.
        opentransaction: if present, overrides transaction in any Lots created to
                         preserve holding period.
        createtransaction: if present, overrides transaction in any Lots/Gains created.

    Returns:
        A sequence of Gain instances, reflecting Lots closed by the transaction.
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
    """Apply a Trade to the appropriate position(s) in the Portfolio.

    Args:
        transaction: the transaction to apply to the Portfolio.
        portfolio: map of (FI account, security) to sequence of Lots.
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
    """Apply a ReturnOfCapital to the appropriate position(s) in the Portfolio.

    Args:
        transaction: the transaction to apply to the Portfolio.
        portfolio: map of (FI account, security) to sequence of Lots.
        sort: sort algorithm for gain recognition e.g. FIFO, used to order closed Lots.
        opentransaction: if present, overrides transaction in any Lots created to
                         preserve holding period.
        createtransaction: if present, overrides transaction in any Lots/Gains created.

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
def split(
    transaction: Union[Split, models.Transaction],
    portfolio: PortfolioType,
    sort=None,
    opentransaction: Optional[TransactionType] = None,
    createtransaction: Optional[TransactionType] = None,
) -> List[Gain]:
    """Apply a Split to the appropriate position(s) in the Portfolio.

    Args:
        transaction: the transaction to apply to the Portfolio.
        portfolio: map of (FI account, security) to sequence of Lots.
        sort: sort algorithm for gain recognition e.g. FIFO, used to order closed Lots.
        opentransaction: if present, overrides transaction in any Lots created to
                         preserve holding period.
        createtransaction: if present, overrides transaction in any Lots/Gains created.

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
def transfer(
    transaction: Union[Transfer, models.Transaction],
    portfolio: PortfolioType,
    sort: Optional["SortType"] = None,
    opentransaction: Optional[TransactionType] = None,
    createtransaction: Optional[TransactionType] = None,
) -> List[Gain]:
    """Apply a Transfer to the appropriate position(s) in the Portfolio.

    Args:
        transaction: the transaction to apply to the Portfolio.
        portfolio: map of (FI account, security) to sequence of Lots.
        sort: sort algorithm for gain recognition e.g. FIFO, used to order closed Lots.
        opentransaction: if present, overrides transaction in any Lots created to
                         preserve holding period.
        createtransaction: if present, overrides transaction in any Lots/Gains created.

    Returns:
        A sequence of Gain instances, reflecting Lots closed by the transaction.

    Raises:
        ValueError: if `Transfer.units` and `Transfer.unitsFrom` aren't
                    oppositely signed.
        Inconsistent: if the relevant position in `portfolio` is insufficient to
                      satisfy `Transfer.unitsFrom`.
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
    """Apply a Spinoff to the appropriate position(s) in the Portfolio.

    Args:
        transaction: the transaction to apply to the Portfolio.
        portfolio: map of (FI account, security) to sequence of Lots.
        sort: sort algorithm for gain recognition e.g. FIFO, used to order closed Lots.
        opentransaction: if present, overrides transaction in any Lots created to
                         preserve holding period.
        createtransaction: if present, overrides transaction in any Lots/Gains created.

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
    """Apply an Exercise transaction to the appropriate position(s) in the Portfolio.

    Args:
        transaction: the transaction to apply to the Portfolio.
        portfolio: map of (FI account, security) to sequence of Lots.
        sort: sort algorithm for gain recognition e.g. FIFO, used to order closed Lots.
        opentransaction: if present, overrides transaction in any Lots created to
                         preserve holding period.
        createtransaction: if present, overrides transaction in any Lots/Gains created.

    Returns:
        A sequence of Gain instances, reflecting Lots closed by the transaction.

    Raises:
        Inconsistent: if the relevant position in `portfolio` as of `Exercise.datetime`
                      doesn't contain enough units of the option to satisfy the
                      `Exercise.units`.
    """

    unitsFrom = transaction.unitsFrom

    pocketFrom = (transaction.fiaccount, transaction.securityFrom)
    positionFrom = portfolio.get(pocketFrom, [])

    # Remove lots from the source position
    takenLots, remainingPosition = take_lots(
        transaction, positionFrom, openAsOf(transaction.datetime), -unitsFrom
    )

    takenUnits = sum([lot.units for lot in takenLots])
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
    """Apply cost basis removed from one position to new units of another position.

    Apply as a trade in order to close Lots of destination position as needed,
    preserving opentransaction from source Lot to maintain holding period.

    Args:
        portfolio: map of (FI account, security) to sequence of Lots.
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


########################################################################################
#  FUNCTIONS OPERATING ON LOTS
########################################################################################
def part_lot(lot: Lot, units: Decimal) -> Tuple[Lot, Lot]:
    """Partition Lot at specified # of units, adding new Lot of leftover units.

    The partitioned Lots differ only in units; all other attributes are the same.

    Args:
        lot: a Lot instance.
        units: # of units to carve out into separate Lot.

    Returns:
        (Lot matching requested # of units, Lot holding remaining units)

    Raises:
        ValueError: if `units` magnitude exceeds `lot.units`, or if `units` and
                    `lot.units` are oppositely signed / zero.
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
    """Remove a selection of Lots from the Position in sequential order.

    Args:
        transaction: source transaction (only used for error reporting).
        lots: sequence of Lots.  Must be presorted by caller.
        criterion: filter function that accepts a Lot instance and returns bool,
                   e.g. openAsOf(datetime) or closableBy(transaction).
                   By default, matches everything.
        max_units: limit of units matching criterion to take.  Sign convention
                   is SAME SIGN as position, i.e. units arg must be positive for long,
                   negative for short. By default, take all units that match criterion.

    Returns:
        (Lots matching criterion/max_units, nonmatching Lots)
    """

    if not lots:
        return [], []

    if criterion is None:

        def _criterion(lot):
            return True

        criterion = _criterion

    def take(
        lots: Sequence[Lot], criterion: CriterionType, max_units: Optional[Decimal]
    ) -> Generator[Tuple[Optional[Lot], Optional[Lot]], None, None]:
        """
        Args:
            lots:
            criterion:
            max_units:

        Yields:
            2-tuple of:
                0) If criterion doesn't match - None.
                   If criterion matches - newly created Lot (copy of original Lot
                   with units updated to reflect units removed from original).
                1) Original Lot, with units reduced by the amount removed.

        Raises:
            ValueError: if remaining units counter and `lot.units` are differently
                        signed / zero.

        """
        units_remain = max_units
        for lot in lots:
            if not criterion(lot):
                yield (None, lot)
            elif units_remain is None:
                yield (lot, None)
            elif units_remain == 0:
                yield (None, lot)
            else:
                if lot.units * units_remain <= 0:
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
    """Remove a fraction of the cost from each Lot in the Position.

    Args:
        lots: sequence of Lots.  Must be presorted by caller.
        criterion: filter function that accepts a Lot instance and returns bool,
                   e.g. openAsOf(datetime) or closableBy(transaction).
                   By default, matches everything.
        fraction: portion of cost to remove from each Lot matching criterion.
        max_units: limit of units matching criterion to take.  Sign convention
                   is SAME SIGN as position, i.e. units arg must be positive for long,
                   negative for short. By default, take all units that match criterion.

    Returns:
        2-tuple of:
            0) list of Lots (copies of Lots meeting criterion, with each
               price updated to reflect the basis removed from the original).
            1) list of Lots (original position, less basis removed).

    Raises:
        ValueError: if `fraction` isn't between 0 and 1.
    """

    if criterion is None:

        def _criterion(lot: Lot) -> bool:
            """Default criterion for take_basis() - matches everything.

            Args:
                lot - a Lot instance.

            Returns:
                True!
            """
            return True

        criterion = _criterion

    if not (0 <= fraction <= 1):
        msg = f"fraction must be between 0 and 1 (inclusive), not '{fraction}'"
        raise ValueError(msg)

    def take(
        lot: Lot, criterion: CriterionType, fraction: Decimal
    ) -> Tuple[Optional[Lot], Lot]:
        """Extract a fraction of a Lot's cost into a new Lot, if the criterion matches.

        Args:
            lot: a Lot instance.
            criterion: filter function that accepts a Lot instance and returns bool,
                       e.g. openAsOf(datetime) or closableBy(transaction).
                       By default, matches anything.
            fraction: portion of cost to remove from the Lot, if it matches criterion.

        Returns:
            2-tuple of:
                0) If criterion doesn't match - None.
                   If criterion matches - newly created Lot (copy of original Lot
                   with price updated to reflect the basis removed from the original).
                1) Original Lot, with price reduced by the basis removed.
        """

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


########################################################################################
#  FILTER CRITERIA
########################################################################################
CriterionType = Callable[[Lot], bool]


def openAsOf(datetime: _datetime.datetime) -> CriterionType:
    """Factory for functions that select open Lots created on or before datetime.

    Args:
        datetime: a datetime.datetime instance.

    Returns:
        Filter function accepting a Lot instance and returning bool.
    """

    def isOpen(lot: Lot) -> bool:
        return lot.createtransaction.datetime <= datetime

    return isOpen


def longAsOf(datetime: _datetime.datetime) -> CriterionType:
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


def closableBy(transaction: TransactionType) -> CriterionType:
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


########################################################################################
#  SORT FUNCTIONS
########################################################################################
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
