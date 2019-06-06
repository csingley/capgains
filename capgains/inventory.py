# coding: utf-8
"""
Data structures and functions for tracking units/cost history of financial assets.
Besides the fundamental requirement of keeping accurate tallies, the main purpose
of this module is to match opening and closing transactions in order to calculate
the amount and character of realized gains.

The basic way to use this module is to create a Portfolio instance, then pass
Transaction instances to its processTransaction() method.

Each Lot tracks the current state of a particular bunch of securities - (unit, cost).

Additionally, each Lot keeps a reference to its opening Transaction, i.e. the
Transaction which started its holding period for tax purposes (to determine whether
the character of realized Gains are long-term or short-term).

Lots are collected in sequences called "positions", which are the values of a
Portfolio mapping keyed by a (FI account, security) called a "pocket".

Each Lot keeps a reference to its "creating" Transaction, i.e. the Transaction which
added the Lot to its current pocket.  In the case of an opening trade, the
opening Transaction and the creating Transaction will be the same.  In the case of
of a transfer, spin-off, reorg, etc., these will be different; the date of the tax
holding period can be sourced from the opening Transaction, while the current
pocket can be sourced from the creating Transaction.

Gains link opening Transactions to realizing Transactions - which are usually closing
Transactions, but may also come from return of capital distributions that exceed
cost basis.  Return of capital Transactions generally don't provide per-share
distribution information, so Gains must keep state for the realizing price.

Lots and Transactions are immutable, so you can rely on the accuracy of references
to them (e.g. Gain.lot & Gain.transaction).  Everything about a Lot (except
opentransaction) can be changed by Transactions; the changes are reflected in a
newly-created Lot, leaving the old Lot undisturbed.

Nothing in this module changes a Transaction or a Gain, once created.
"""
# stdlib imports
from collections import defaultdict
from decimal import Decimal
import datetime as _datetime
import itertools
from typing import NamedTuple, Tuple, List, Mapping, Callable, Any, Optional, Union


# local imports
from capgains.models import transactions
from capgains import utils


class InventoryError(Exception):
    """ Base class for Exceptions defined in this module """


class Inconsistent(InventoryError):
    """
    Exception raised when Position's state is inconsistent with Transaction
    """

    def __init__(self, transaction, msg):
        self.transaction = transaction
        self.msg = msg
        super(Inconsistent, self).__init__(f"{transaction} inconsistent: {msg}")


UNITS_RESOLUTION = Decimal("0.001")


#######################################################################################
# DATA MODEL
#######################################################################################

# FIXME - it would be better to decouple the Transaction model from the SQL version
# and implement transaction types via subclasses, rather than setting Transaction.type.
# That would let us get rid of the optional typing and default None values below.
class Transaction(NamedTuple):
    """
    A change to a securities position.

    Persistent SQL implementation of this model in capgains.models.transactions
    """

    id: int
    uniqueid: str
    datetime: _datetime.datetime
    type: transactions.TransactionType
    fiaccount: Any
    security: Any
    dtsettle: Optional[_datetime.datetime] = None
    memo: Optional[str] = None
    currency: Optional[str] = None
    cash: Optional[Decimal] = None
    units: Optional[Decimal] = None
    securityPrice: Optional[Decimal] = None
    fiaccountFrom: Optional[Any] = None
    securityFrom: Optional[Any] = None
    unitsFrom: Optional[Decimal] = None
    securityFromPrice: Optional[Decimal] = None
    numerator: Optional[Decimal] = None
    denominator: Optional[Decimal] = None
    # sort's type is actually Optional[SortType], but mypy chokes on recursion
    sort: Optional[Mapping[str, Union[bool, Callable[[Any], Tuple]]]] = None


Transaction.id.__doc__ = "Local transaction unique identifer (database PK)"
Transaction.uniqueid.__doc__ = "FI transaction unique identifier"
Transaction.datetime.__doc__ = "Effective date/time (ex-date for distributions)"
Transaction.dtsettle.__doc__ = "For cash distributions: payment date"
Transaction.type.__doc__ = (
    "A transactions.TransactionType enum, i.e. one of "
    "(RETURNCAP, SPLIT, SPINOFF, TRANSFER, TRADE, EXERCISE)"
)
Transaction.memo.__doc__ = "Transaction notes"
Transaction.currency.__doc__ = "Currency denomination of Transaction.cash (ISO 4217)"
Transaction.cash.__doc__ = "Change in money amount"
Transaction.fiaccount.__doc__ = "Financial institution (e.g. brokerage) account"
Transaction.security.__doc__ = "Security or other asset"
Transaction.units.__doc__ = "Change in Security quantity"
Transaction.fiaccountFrom.__doc__ = "For transfers: source FI account"
Transaction.securityFrom.__doc__ = "For transfers, spinoffs, exercise: source Security"
Transaction.unitsFrom.__doc__ = (
    "For splits, transfers, exercise: change in source Security quantity"
)
Transaction.securityPrice.__doc__ = (
    "For spinoffs: FMV of destination Security post-spin"
)
Transaction.securityFromPrice.__doc__ = "For spinoffs: FMV of source security post-spin"
Transaction.numerator.__doc__ = (
    "For splits, spinoffs: normalized units of destination Security"
)
Transaction.denominator.__doc__ = (
    "For splits, spinoff: normalized units of source Security"
)
Transaction.sort.__doc__ = "Sort algorithm for gain recognition"


#  Type alias involving Transaction must be defined after Transaction itself,
#  else mypy chokes on the recursive definition.
TransactionType = Union[Transaction, transactions.Transaction]


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
    Binds realizing Transaction to a Lot
    (and indirectly its opening Transaction)
    """

    lot: Lot
    transaction: Any
    price: Decimal


Gain.lot.__doc__ = "Lot instance for which gain is realized"
Gain.transaction.__doc__ = "Transaction instance realizing gain"
Gain.price.__doc__ = "Per-unit proceeds (positive or zero)"


#######################################################################################
# API
#######################################################################################
class Portfolio(defaultdict):
    """
    Mapping container for positions (i.e. sequences of Lots),
    keyed by (FI account, security) a/k/a "pocket".
    """

    default_factory = list

    def __init__(self, *args, **kwargs):
        args = (self.default_factory,) + args
        defaultdict.__init__(self, *args, **kwargs)

    def processTransaction(
        self, transaction: TransactionType, sort: Optional["SortType"] = None
    ) -> List[Gain]:
        """
        Apply a Transaction to the appropriate position(s).

        Main entry point for transaction processing.
        """

        handlers = {
            transactions.TransactionType.RETURNCAP: self.returnofcapital,
            transactions.TransactionType.SPLIT: self.split,
            transactions.TransactionType.SPINOFF: self.spinoff,
            transactions.TransactionType.TRANSFER: self.transfer,
            transactions.TransactionType.TRADE: self.trade,
            transactions.TransactionType.EXERCISE: self.exercise,
        }

        handler = handlers[transaction.type]
        gains = handler(transaction, sort=sort or transaction.sort)  # type: ignore
        return gains

    def trade(
        self,
        transaction: TransactionType,
        opentransaction: Optional[TransactionType] = None,
        createtransaction: Optional[TransactionType] = None,
        sort: Optional["SortType"] = None,
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
        assert isinstance(transaction.units, Decimal)
        assert isinstance(transaction.cash, Decimal)
        assert isinstance(transaction.currency, str)

        if transaction.units == 0:
            raise ValueError(f"units can't be zero: {transaction}")

        pocket = (transaction.fiaccount, transaction.security)
        position = self[pocket]
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

        self[pocket] = position

        return [
            Gain(lot=lot, transaction=createtransaction or transaction, price=price)
            for lot in lots_closed
        ]

    def returnofcapital(self, transaction: TransactionType, sort=None) -> List[Gain]:
        """
        Apply cash to reduce cost basis of long position as of Transaction.datetime;
        realize Gain on Lots where cash proceeds exceed cost basis.

        ``sort`` is in the argument signature for processTransaction(), but unused.
        """
        assert isinstance(transaction.cash, Decimal)

        pocket = (transaction.fiaccount, transaction.security)
        position = self[pocket]

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
        self[pocket] = list(basisReduced) + list(unaffected)
        return [gain for gain in gains if gain is not None]

    def split(self, transaction: TransactionType, sort=None) -> List[Gain]:
        """
        Increase/decrease Lot units without affecting basis or realizing Gain.

        ``sort`` is in the argument signature for processTransaction(), but unused.
        """
        assert isinstance(transaction.numerator, Decimal)
        assert isinstance(transaction.denominator, Decimal)
        assert isinstance(transaction.units, Decimal)

        splitRatio = transaction.numerator / transaction.denominator

        pocket = (transaction.fiaccount, transaction.security)
        position = self[pocket]

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

        self[pocket] = list(position_new) + list(unaffected)

        # Stock splits don't realize Gains
        return []

    def transfer(
        self, transaction: TransactionType, sort: Optional["SortType"] = None
    ) -> List[Gain]:
        """
        Move Lots from one Position to another, maybe changing Security/units.

        ``sort`` is a mapping of (key func, reverse) such as FIFO/MINGAIN etc.
        used to order Lots when closing them.
        """
        assert isinstance(transaction.units, Decimal)
        assert isinstance(transaction.unitsFrom, Decimal)

        if transaction.units * transaction.unitsFrom >= 0:
            msg = f"units and unitsFrom aren't oppositely signed in {transaction}"
            raise ValueError(msg)

        pocketFrom = (transaction.fiaccountFrom, transaction.securityFrom)
        positionFrom = self[pocketFrom]
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

        lotUnitsFrom = sum([lot.units for lot in lotsFrom])
        if abs(lotUnitsFrom + transaction.unitsFrom) > UNITS_RESOLUTION:
            msg = (
                f"Position in {transaction.security} for FI account "
                f"{transaction.fiaccount} on {transaction.datetime} is only "
                f"{lotUnitsFrom} units; can't transfer out {transaction.units} units."
            )
            raise Inconsistent(transaction, msg)

        self[pocketFrom] = positionFrom

        # Transform Lots to the destination Security/units and apply
        # as a Trade (to order closed Lots, if any) with opentxid & opendt
        # preserved from the source Lot
        gains = []
        for lotFrom in lotsFrom:
            # FIXME - We need a Trade.id for self.trade() to set
            # Lot.createtxid, but "id=transaction.id" is problematic.
            trade = Transaction(
                id=transaction.id,
                uniqueid=transaction.uniqueid,
                datetime=transaction.datetime,
                type=transactions.TransactionType.TRANSFER,
                memo=transaction.memo,
                currency=lotFrom.currency,
                cash=-lotFrom.price * lotFrom.units,
                fiaccount=transaction.fiaccount,
                security=transaction.security,
                units=lotFrom.units * -transaction.units / transaction.unitsFrom,
            )
            gs = self.trade(
                trade,
                opentransaction=lotFrom.opentransaction,
                createtransaction=transaction,
            )
            gains.extend(gs)

        return gains

    def spinoff(
        self, transaction: TransactionType, sort: Optional["SortType"] = None
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

        units = transaction.units

        spinRatio = Decimal(transaction.numerator) / Decimal(transaction.denominator)

        # costFraction is the fraction of original cost allocated to the spinoff,
        # with the balance allocated to the source position.
        if transaction.securityPrice is None or transaction.securityFromPrice is None:
            costFraction = Decimal("0")
        else:
            spinoffFMV = transaction.securityPrice * units
            spunoffFMV = transaction.securityFromPrice * units / spinRatio
            costFraction = spinoffFMV / (spinoffFMV + spunoffFMV)

        pocketFrom = (transaction.fiaccount, transaction.securityFrom)
        positionFrom = self[pocketFrom]

        # Take the basis from the source Position
        lotsFrom, positionFrom = take_basis(
            positionFrom, openAsOf(transaction.datetime), costFraction
        )

        openUnits = sum([lot.units for lot in lotsFrom])
        if abs(openUnits * spinRatio - units) > UNITS_RESOLUTION:
            msg = (
                f"Spinoff {transaction.numerator} units {transaction.security} "
                f"for {transaction.denominator} units {transaction.securityFrom}:\n"
                f"To receive {transaction.units} units {transaction.security} "
                f"requires a position of {units / spinRatio} units of "
                f"{transaction.securityFrom} in FI account {transaction.fiaccount} "
                f"on {transaction.datetime}, not units={openUnits}"
            )
            raise Inconsistent(transaction, msg)

        self[pocketFrom] = positionFrom

        # Apply spinoff units to the destination position as a trade (to close Lots
        # as necessary), preserving opentransaction from source Lot to maintain
        # holding period.

        def applyTrade(
            lotFrom: Lot,
            transaction: TransactionType,
            ratio: Decimal,
            sort: Optional[SortType],
        ) -> List[Gain]:
            # FIXME - We need a Trade.id for self.trade() to set
            # Lot.createtxid, but "id=transaction.id" is problematic.
            trade = Transaction(
                id=transaction.id,
                uniqueid=transaction.uniqueid,
                datetime=transaction.datetime,
                type=transactions.TransactionType.TRADE,
                memo=transaction.memo,
                currency=lotFrom.currency,
                cash=-lotFrom.price * lotFrom.units,
                fiaccount=transaction.fiaccount,
                security=transaction.security,
                units=lotFrom.units * ratio,
            )
            return self.trade(
                trade,
                opentransaction=lotFrom.opentransaction,
                createtransaction=transaction,
                sort=sort,
            )

        gains = (
            applyTrade(lotFrom, transaction, spinRatio, sort) for lotFrom in lotsFrom
        )
        return list(itertools.chain.from_iterable(gains))

    def exercise(
        self, transaction: TransactionType, sort: Optional["SortType"] = None
    ) -> List[Gain]:
        """
        Exercise an option.

        ``sort`` is a mapping of (key func, reverse) such as FIFO/MINGAIN etc.
        used to order Lots when closing them.
        """
        assert isinstance(transaction.units, Decimal)
        assert isinstance(transaction.unitsFrom, Decimal)
        assert isinstance(transaction.cash, Decimal)

        unitsFrom = transaction.unitsFrom

        pocketFrom = (transaction.fiaccount, transaction.securityFrom)
        positionFrom = self[pocketFrom]

        # Remove lots from the source position
        takenLots, remainingPosition = take_lots(
            transaction, positionFrom, openAsOf(transaction.datetime), -unitsFrom
        )

        takenUnits = sum([lot.units for lot in takenLots])
        assert isinstance(takenUnits, Decimal)
        if abs(takenUnits) - abs(unitsFrom) > UNITS_RESOLUTION:
            msg = f"Exercise Lot.units={takenUnits} (not {unitsFrom})"
            raise Inconsistent(transaction, msg)

        self[pocketFrom] = remainingPosition

        # Transform Lots to the destination Security/units, add additional
        # exercise cash as cost pro rata, and apply as a Trade (to order
        # closed Lots, if any)

        def applyTrade(lot, transaction, newUnits, sort):
            multiplier = abs(transaction.units / transaction.unitsFrom)
            adjusted_cash = lot.units * (transaction.cash / newUnits - lot.price)

            # FIXME - We need a Trade.id for self.trade() to set
            # Lot.createtxid, but "id=transaction.id" is problematic.
            trade = Transaction(
                type=transactions.TransactionType.TRADE,
                id=transaction.id,
                fiaccount=transaction.fiaccount,
                uniqueid=transaction.uniqueid,
                datetime=transaction.datetime,
                memo=transaction.memo,
                security=transaction.security,
                units=lot.units * multiplier,
                cash=adjusted_cash,
                currency=lot.currency,
            )
            return self.trade(trade, sort=sort)

        gains = (applyTrade(lot, transaction, takenUnits, sort) for lot in takenLots)
        return list(itertools.chain.from_iterable(gains))


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


SortType = Mapping[str, Union[bool, Callable[[Lot], Tuple]]]


FIFO = {"key": sort_oldest, "reverse": False}
LIFO = {"key": sort_oldest, "reverse": True}
MINGAIN = {"key": sort_dearest, "reverse": False}
MAXGAIN = {"key": sort_cheapest, "reverse": False}
