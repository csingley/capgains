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
from decimal import Decimal
import datetime as _datetime
from typing import NamedTuple, Tuple, Mapping, Callable, Any, Optional, Union


# local imports
from capgains import models


class DummyTransaction(NamedTuple):
    """The models.Transaction interface.

    Will not be dispatched by inventory.api.book().  Instances are only created when
    translating currency of gains, or when deserializing Lots as a placeholder for
    the Lot.opentransaction that can't be serialized.

    Attributes:
        type: enum specifying a transaction type below (Trade, Split, etc.)
        uniqueid: transaction unique identifier (e.g. FITID).
        datetime: transaction date/time - accrual basis
                  (ex-date for spinoffs and return of capital).
        fiaccount: brokerage account where security and/or cash changes
                   (destination account for transfers).
        security: asset that changes
                  (destination security for transfers, spinoffs, and options exercise).
        units: change in (destination) security amount resulting from transaction.
        currency: currency denomination of cash (ISO 4217 code).
        cash: change in money amount (+ increases cash, - decreases cash).
        fromfiaccount: source brokerage account for transfers.
        fromsecurity: source security for transfers, spinoffs, options exercise.
        fromunits: change in source security amount for transfers, options exercise.
        numerator: normalized units of destination or post-split security.
        denominator: normalized units of source or pre-slit security.
        memo: transaction notes.
        dtsettle: transaction date/time - cash basis
                  (pay date for spinoffs and return of capital).
        sort: sort algorithm for gain recognition.
        securityprice: for spinoffs - price used to fair-value source security.
        fromsecurityprice: for spinoffs - price used to fair-value destination security.
    """

    type: models.TransactionType
    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    units: Optional[Decimal] = None
    currency: Optional[models.Currency] = None
    cash: Optional[Decimal] = None
    fromfiaccount: Any = None
    fromsecurity: Any = None
    fromunits: Optional[Decimal] = None
    numerator: Optional[Decimal] = None
    denominator: Optional[Decimal] = None
    memo: Optional[str] = None
    dtsettle: Optional[_datetime.datetime] = None
    # sort's type is Optional[SortType], but mypy chokes on recursion - redefine here.
    sort: Optional[Mapping[str, Union[bool, Callable[[Any], Tuple]]]] = None
    securityprice: Optional[Decimal] = None
    fromsecurityprice: Optional[Decimal] = None


class Trade(NamedTuple):
    """Buy/sell a security, creating basis (if opening) or realizing gain (if closing).

    Attributes:
        cf. DummyTransaction docstring
    """

    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    units: Decimal
    currency: models.Currency
    cash: Decimal
    memo: Optional[str] = None
    dtsettle: Optional[_datetime.datetime] = None
    # sort's type is Optional[SortType], but mypy chokes on recursion - redefine here.
    sort: Optional[Mapping[str, Union[bool, Callable[[Any], Tuple]]]] = None


class ReturnOfCapital(NamedTuple):
    """Cash distribution that reduces cost basis.

    Note: `datetime` is ex-date; `dtsettle` is pay date.

    Attributes:
        cf. DummyTransaction docstring
    """

    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    currency: models.Currency
    cash: Decimal
    memo: Optional[str] = None
    dtsettle: Optional[_datetime.datetime] = None


class Transfer(NamedTuple):
    """Move assets between (fiaccount, security) pockets, retaining basis/ open date.

    Units can also be changed during a Transfer, so this type can also represent
    corporate reorganizations (mergers, etc.)

    Attributes:
        cf. DummyTransaction docstring
    """

    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    units: Decimal
    fromfiaccount: Any
    fromsecurity: Any
    fromunits: Decimal
    memo: Optional[str] = None


class Split(NamedTuple):
    """Change position units without affecting costs basis or holding period.

    Splits are declared in terms of new units : old units (the split ratio).
    These normalized units are represented by `numerator`:`denominator`.

    Note:
        A Split is essentially a Transfer where fiaccount=fromfiaccount and
        security=SecurityFrom, differing only in units/unitsfrom.

    Attributes:
        cf. DummyTransaction docstring
    """

    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    units: Decimal
    numerator: Decimal
    denominator: Decimal
    memo: Optional[str] = None


#  FIXME - spinoff really should have `dtsettle` attribute
class Spinoff(NamedTuple):
    """Turn one security into two, partitioning cost basis between them.

    Spinoffs are declared in terms of (units new security) : (units original security).
    These normalized units are represented by `numerator`:`denominator`.

    Per the US tax code, cost basis must be divided between the two securities
    positions in proportion to their fair market value.  For exchange-traded securities
    this is normally derived from market prices immediately after the spinoff.  This
    pricing, represented by `securityprice` (for the spun-off security) and
    `securityfromprice` (for the spinning security), isn't generally known at the time
    of the spinoff.  Pricing data must be edited in after market data becomes available.

    Attributes:
        cf. DummyTransaction docstring
    """

    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    units: Decimal
    numerator: Decimal
    denominator: Decimal
    fromsecurity: Any
    memo: Optional[str] = None
    securityprice: Optional[Decimal] = None
    fromsecurityprice: Optional[Decimal] = None


class Exercise(NamedTuple):
    """Exercise a securities option, buying/selling the underlying security.

    Exercising an option removes its units from the FI account and rolls its cost
    basis into the underlying.  For tax purposes, the holding period for the
    underlying begins at exercise, not at purchase of the option.

    Note:
        Source security is the option; destination security is the underlying.
        `cash` represents the net exercise payment, which will have + sign for
        exercising long put/short call or - sign for exercising long call/short put.

    Attributes:
        cf. DummyTransaction docstring
    """

    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    units: Decimal
    currency: models.Currency
    cash: Decimal
    fromsecurity: Any
    fromunits: Decimal
    memo: Optional[str] = None


TransactionType = Union[
    models.Transaction,
    DummyTransaction,
    Trade,
    ReturnOfCapital,
    Split,
    Transfer,
    Spinoff,
    Exercise,
]
"""Type alias for classes implementing the Transaction interface.

Includes models.Transaction and its incarnations as a DummyTransaction, as well as
the NamedTuple subclasses defined above that implement each relevant subset of
Transaction attributes.

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
    currency: models.Currency


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
