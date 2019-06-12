# coding: utf-8
"""
Data structures for tracking units/cost history of financial assets.

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
    "Trade",
    "ReturnOfCapital",
    "Transfer",
    "Split",
    "Spinoff",
    "Exercise",
    "TransactionType",
    "Lot",
    "Gain",
]


# stdlib imports
import operator
from decimal import Decimal
import datetime as _datetime
from typing import NamedTuple, Tuple, Mapping, Callable, Any, Optional, Union


# local imports
from capgains import models


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
        fiaccountfrom: source brokerage account.
        securityfrom: source security.
        unitsfrom: source security amount.
        memo: transaction notes.
    """

    id: int
    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    units: Decimal
    fiaccountfrom: Any
    securityfrom: Any
    unitsfrom: Decimal
    memo: Optional[str] = None


class Split(NamedTuple):
    """Change position units without affecting costs basis or holding period.

    Splits are declared in terms of new units : old units (the split ratio).

    Note:
        A Split is essentially a Transfer where fiaccount=fiaccountfrom and
        security=SecurityFrom, differing only in units/unitsfrom.

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
    pricing isn't known at the time of the spinoff; securityprice and securityfromprice
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
        securityfrom: source security (i.e. security subject to spinoff).
        memo: transaction notes.
        securityprice: unit price used to fair-value source security.
        securityfromprice: unit price used to fair-value destination security.
    """

    id: int
    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    units: Decimal
    numerator: Decimal
    denominator: Decimal
    securityfrom: Any
    memo: Optional[str] = None
    securityprice: Optional[Decimal] = None
    securityfromprice: Optional[Decimal] = None


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
        securityfrom: source security (i.e. the option).
        unitsfrom: change in mount of source security (i.e. the option).
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
    securityfrom: Any
    unitsfrom: Decimal
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


# Money type to replace (cash, currency) - currently unused
NumberType = Union[float, Decimal]


def bind_amount(func, cash: "Money"):
    return Money(amount=func(cash.amount), currency=cash.currency)


def bind_amount_scalar(func, cash: "Money", scalar: NumberType):
    return Money(amount=func(cash.amount, scalar), currency=cash.currency)


def bind_amounts(func, cash0: "Money", cash1: "Money"):
    if cash0.currency != cash1.currency:
        raise ValueError(f"{cash0} and {cash1} have different currencies")
    return func(cash0.amount, cash1.amount)


class Money(NamedTuple):
    amount: Decimal
    currency: str

    def __neg__(self) -> "Money":
        return bind_amount(operator.neg, self)

    def __pos__(self) -> "Money":
        return bind_amount(operator.pos, self)

    def __abs__(self) -> "Money":
        return bind_amount(operator.abs, self)

    # Comparison requires identical currencies
    def __lt__(self, other) -> bool:
        return bind_amounts(operator.lt, self, other)

    def __le__(self, other) -> bool:
        return bind_amounts(operator.le, self, other)

    def __eq__(self, other) -> bool:
        return bind_amounts(operator.eq, self, other)

    def __ne__(self, other) -> bool:
        return bind_amounts(operator.ne, self, other)

    def __ge__(self, other) -> bool:
        return bind_amounts(operator.ge, self, other)

    def __gt__(self, other) -> bool:
        return bind_amounts(operator.gt, self, other)

    # Addition requires identical currencies
    def __add__(self, other) -> "Money":
        return bind_amounts(operator.add, self, other)

    def __iadd__(self, other) -> "Money":
        return bind_amounts(operator.iadd, self, other)

    def __sub__(self, other) -> "Money":
        return bind_amounts(operator.sub, self, other)

    def __isub__(self, other) -> "Money":
        return bind_amounts(operator.isub, self, other)

    # Multiplication is scalar; multiplier must be unitless number
    def __mul__(self, other) -> "Money":
        return bind_amount_scalar(operator.mul, self, other)

    def __imul__(self, other) -> "Money":
        return bind_amount_scalar(operator.imul, self, other)

    def __truediv__(self, other) -> "Money":
        return bind_amount_scalar(operator.truediv, self, other)

    def __itruediv__(self, other) -> "Money":
        return bind_amount_scalar(operator.itruediv, self, other)
