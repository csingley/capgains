# coding: utf-8
"""
Processes Transactions to track cost basis via Lots, and matches Transactions
to report amount and character of realized Gains.

The main things to remember about this data model:
    1.  Lots get split as needed by every Transaction affecting them; if you
        use the high-level interface of this module (trade(), split(), etc.)
        you should never need to deal with a partial Lot, only a whole Lot.

    2.  Everything significant about a Lot can be changed by Transactions
        (except opentransaction).  You can get a Lot's current FiAccount and
        Security by reference to its createtransaction.  A Lot must keep
        state for its current units and price (since these can be changed
        after its createtransaction by e.g. returns of capital and splits)
        and currency (since some createtransactions, e.g. transfers, don't
        provide currency information).

    3.  Lots may be loaded from CSV files, in which case the corresponding
        opentransactions and createtransactions may not be available in the DB.

    4.  Gains may be realized from return of capital Transactions, which
        generally don't provide per-share distribution information.  Therefore
        Gains must keep state for the realizing price.

    5.  Lots are immutable, and nothing in this module changes anything about
        Transactions, so you can rely on the accuracy of Gain.lot and
        Gain.transaction.  Subsequent changes to the same Lots won't affect
        the state of previously calculated Gains.
"""
# stdlib imports
from collections import (namedtuple, defaultdict)
from decimal import Decimal


###############################################################################
# BASIC DATA CONTAINERS
###############################################################################
Lot = namedtuple('Lot', ['opentransaction', 'createtransaction', 'units',
                         'price', 'currency'])
Lot.__doc__ += ': Cost basis/holding data container'
Lot.opentransaction.__doc__ = 'Transaction instance that began holding period'
Lot.createtransaction.__doc__ = ('Transaction instance that created the Lot ',
                                 'for the current Position')
Lot.units.__doc__ = '(type decimal.Decimal; nonzero)'
Lot.price.__doc__ = '(type decimal.Decimal; positive or zero)'
Lot.currency.__doc__ = 'Currency denomination of Lot.price'

Gain = namedtuple('Gain', ['lot', 'transaction', 'price'])
Gain.lot.__doc__ = 'Lot instance for which gain is realized'
Gain.transaction.__doc__ = 'The Transaction instance realizing gain'
Gain.price.__doc__ = '(type decimal.Decimal; positive or zero)'


###############################################################################
# TRANSACTION API
# Persistent SQL implementation of this API in capgains.models.transactions
###############################################################################
Transaction = namedtuple('Transaction', [
    'id', 'uniqueid', 'datetime', 'dtsettle', 'type', 'memo', 'currency',
    'cash', 'fiaccount', 'security', 'units', 'securityPrice', 'fiaccountFrom',
    'securityFrom', 'unitsFrom', 'securityFromPrice', 'numerator',
    'denominator', 'sort'])
Transaction.id.__doc__ = 'Local transaction unique identifer (database PK)'
Transaction.uniqueid.__doc__ = 'FI transaction unique identifier (type str)'
Transaction.datetime.__doc__ = 'Effective date/time (type datetime.datetime)'
Transaction.dtsettle.__doc__ = ('For cash distributions: payment date (dttrade'
                                ' is accrual date i.e. ex-date')
Transaction.type.__doc__ = ''
Transaction.memo.__doc__ = 'Transaction notes (type str)'
Transaction.currency.__doc__ = ('Currency denomination of Transaction.cash '
                                '(type str; ISO 4217)')
Transaction.cash.__doc__ = ('Change in money amount caused by Transaction '
                            '(type decimal.Decimal)')
Transaction.fiaccount.__doc__ = 'Financial institution acount'
Transaction.security.__doc__ = 'Security or other asset'
Transaction.units.__doc__ = ('Change in Security quantity caused by '
                             'Transaction (type decimal.Decimal)')
Transaction.fiaccountFrom.__doc__ = 'For transfers: source FI acount'
Transaction.securityFrom.__doc__ = ('For transfers, spinoffs, exercise: '
                                    'source Security')
Transaction.unitsFrom.__doc__ = ('For splits, transfers, exercise: change in '
                                 'quantity of source Security caused by '
                                 'Transaction (type decimal.Decimal)')
Transaction.securityPrice.__doc__ = ('For spinoffs: FMV of destination '
                                     'Security post-spin')
Transaction.securityFromPrice.__doc__ = ('For spinoffs: FMV of source '
                                         'security post-spin')
Transaction.numerator.__doc__ = ('For splits, spinoffs: normalized units of '
                                 'destination Security')
Transaction.denominator.__doc__ = ('For splits, spinoff: normalized units of '
                                   'source Security')
Transaction.sort.__doc__ = 'Sort algorithm for gain recognition'
Transaction.__new__.__defaults__ = (None, ) * 19


###############################################################################
# ERRORS
##############################################################################
class InventoryError(Exception):
    """ Base class for Exceptions defined in this module """
    pass


class Inconsistent(InventoryError):
    """
    Exception raised when Position's state is inconsistent with Transaction
    """
    def __init__(self, transaction, msg):
        self.transaction = transaction
        self.msg = msg
        new_msg = "{} inconsistent: {}"
        super(Inconsistent, self).__init__(new_msg.format(transaction, msg))


###############################################################################
# POSITION - list container for Lots
###############################################################################
class Position(list):
    """ Ordered sequence of Lots """
    @property
    def total(self):
        """
        Returns: (units, cost) tuple summed for entire position
        """
        totals = [(lot.units, lot.units * lot.price) for lot in self]
        return tuple(sum(t) for t in zip(*totals)) or (Decimal('0'),
                                                       Decimal('0'))

    def cut(self, index, units):
        """
        Cleave Lot in twain at the specified # of units, immediately thereafter
        inserting a new Lot holding the remaining units

        Args: index - sequence index of the Lot to cut
              units - # of units to partition

        Returns: the newly-inserted Lot holding the remaining units
        """
        if not isinstance(units, Decimal):
            msg = "units must be type Decimal, not {}"
            raise ValueError(msg.format(type(units).__name__))
        l = self[index]._asdict()
        u = l['units']
        assert isinstance(u, Decimal)

        assert abs(u) > abs(units)
        l['units'] = u - units
        self[index] = Lot(**l)
        l['units'] = units
        lot = Lot(**l)
        self.insert(index, lot)
        return lot

    def take(self, criterion, units=None):
        """
        Remove a selection of Lots from the Position in sequential order.

        Sign convention is SAME SIGN as position, i.e. units arg must be
        positive for long, negative for short

        Args: criterion - filter function that accepts ENUMERATED list items
                          i.e. (index, Lot) tuples
              units - max units to take.  If units=None, take all units that
                      match criterion.

        Returns: a list of Lot instances
        """
        if not isinstance(units, (type(None), Decimal)):
            msg = "units must be type None or Decimal, not {}"
            raise ValueError(msg.format(type(units).__name__))

        lots = filter(criterion, enumerate(self))

        if units is not None:
            lots_ = []

            for index, lot in lots:
                if units == 0:
                    break
                if units * lot.units < 0:
                    msg = "Lot(units={}) has the wrong sign".format(lot.units)
                    # Don't have access to the Transaction here
                    raise Inconsistent(None, msg)
                if abs(units) >= abs(lot.units):
                    # Mark the whole Lot to be taken.
                    lots_.append((index, lot))
                    units -= lot.units
                else:
                    # We've exhausted desired units and still have extra
                    # units left over in this Lot.
                    # Cut the Lot into desired units and leftover units;
                    # mark Lot with desired units to be taken.
                    lots_.append((index, self.cut(index, units)))
                    units = 0

            lots = lots_

        lots = list(lots)
        if not lots:
            return []
        indices, lots = zip(*lots)
        # We need to remove items from the list backwards from the end in order
        # to maintain stable ordering during iteration.
        for index in sorted(indices, reverse=True):
            del self[index]

        return lots

    def take_basis(self, criterion, fraction):
        """
        Remove a fraction of the cost from each Lot in the Position.

        Args: criterion - filter function that accepts ENUMERATED list items
                          i.e. (index, Lot) tuples
              fraction - portion of cost to take.

        Returns: a list of Lot instances (copies of the Lots meeting criterion,
                 with each price updated to reflect the basis removed).
        """
        if not (0 <= fraction <= 1):
            msg = "fraction must be between 0 and 1, not '{}'"
            raise ValueError(msg.format(fraction))

        lots = filter(criterion, enumerate(self))

        def take_cost(index, lot, fraction):
            price = lot.price
            takenprice = price * fraction
            taken = lot._replace(price=takenprice)
            left = lot._replace(price=price - takenprice)
            return taken, (index, left)

        results = [take_cost(index, lot, fraction) for index, lot in lots]
        if len(results) == 0:
            return []
        taken, left = zip(*results)
        for index, lot in left:
            self[index] = lot

        return taken


###############################################################################
# PORTFOLIO - dict container for Positions
###############################################################################
class Portfolio(defaultdict):
    """ """
    default_factory = Position

    def __init__(self, *args, **kwargs):
        args = (self.default_factory,) + args
        defaultdict.__init__(self, *args, **kwargs)

    def trade(self, transaction, opentransaction=None, createtransaction=None,
              sort=None):
        """
        Normal buy or sell, closing open Lots and realizing Gains.

        Args: transaction - a Transaction instance
              opentransaction - a Transaction instance; if present, overrides
                                lot.opentransaction to preserve holding period
              createtransaction - a Transaction instance; if present, overrides
                                  lot.createtransaction and gain.transaction
              sort - a duple of (key func, reverse) such as FIFO/MINGAIN etc.
                     defined above, used to order Lots when closing them.

        Returns: a list of Gain instances
        """
        self._validate_args(transaction,
                            units=Decimal, cash=(type(None), Decimal))
        if transaction.units == 0:
            msg = "units can't be zero: {}".format(transaction)
            raise ValueError(msg)

        sort = sort or FIFO
        position = self[(transaction.fiaccount, transaction.security)]
        position.sort(**sort)

        try:
            lots = position.take(closableBy(transaction), -transaction.units)
        except Inconsistent as err:
            # Lot.units opposite sign from Transaction.units
            raise Inconsistent(transaction, err.msg)
        units = transaction.units + sum([lot.units for lot in lots])
        price = abs(transaction.cash / transaction.units)
        if units != 0:
            position.append(
                Lot(opentransaction=opentransaction or transaction,
                    createtransaction=createtransaction or transaction,
                    units=units, price=price,
                    currency=transaction.currency))

        gains = [Gain(lot=lot, transaction=createtransaction or transaction,
                      price=price) for lot in lots]

        return gains

    def returnofcapital(self, transaction, sort=None):
        """
        Apply cash to reduce Lot cost basis; realize Gain once basis has been
        reducd to zero.
        """
        self._validate_args(transaction, cash=Decimal)

        position = self[(transaction.fiaccount, transaction.security)]
        lots = list(filter(longAsOf(transaction.datetime),
                           enumerate(position)))
        units = sum([lot.units for i, lot in lots])
        if units == 0:
            msg = "no long position for {} in {} as of {}".format(
                transaction.fiaccount, transaction.security,
                transaction.datetime)
            raise Inconsistent(transaction, msg)
        priceDelta = transaction.cash / units

        gains = []

        for index, lot in lots:
            netprice = lot.price - priceDelta
            if netprice < 0:
                gains.append(Gain(lot=lot, transaction=transaction,
                                  price=priceDelta))
                netprice = 0
            position[index] = lot._replace(price=netprice)

        return gains

    def split(self, transaction, sort=None):
        """
        Increase/decrease Lot units without affecting basis or realizing Gain.
        """
        self._validate_args(transaction, numerator=Decimal,
                            denominator=Decimal, units=Decimal)

        splitRatio = transaction.numerator / transaction.denominator

        position = self[(transaction.fiaccount, transaction.security)]
        lots = list(filter(openAsOf(transaction.datetime),
                           enumerate(position)))

        unitsTo = Decimal('0')
        unitsFrom = Decimal('0')

        for index, lot in lots:
            units = lot.units * splitRatio
            price = lot.price / splitRatio
            position[index] = lot._replace(units=units, price=price)
            unitsFrom += lot.units
            unitsTo += units

        calcUnits = unitsTo - unitsFrom
        if abs(calcUnits - transaction.units) > Decimal('0.001'):
            msg = ("For Lot.unitsFrom={}, split ratio {}:{} should yield "
                   "units={} not units={}").format(
                       unitsFrom, transaction.numerator,
                       transaction.denominator, calcUnits, transaction.units)
            raise Inconsistent(transaction, msg)

        # Stock splits don't realize Gains
        return []

    def transfer(self, transaction, sort=None):
        """
        Move Lots from one Position to another, maybe changing Security/units.
        """
        self._validate_args(transaction, units=Decimal, unitsFrom=Decimal)
        if transaction.units * transaction.unitsFrom >= 0:
            msg = "units and unitsFrom aren't oppositely signed in {}"
            raise ValueError(msg.format(transaction))

        ratio = -transaction.units / transaction.unitsFrom
        pocketFrom = (transaction.fiaccountFrom, transaction.securityFrom)
        positionFrom = self[pocketFrom]
        if not positionFrom:
            msg = "No position in {}".format(pocketFrom)
            raise Inconsistent(transaction, msg)
        sort = sort or FIFO
        positionFrom.sort(**sort)

        # Remove the Lots from the source Position
        try:
            lotsFrom = positionFrom.take(openAsOf(transaction.datetime),
                                         -transaction.unitsFrom)
        except Inconsistent as err:
            raise Inconsistent(transaction, err.msg)

        lunitsFrom = sum([l.units for l in lotsFrom])
        tunitsFrom = transaction.unitsFrom
        if abs(lunitsFrom + tunitsFrom) > 0.001:
            msg = ("Position in {} has units={}; can't satisfy "
                   "unitsFrom={}")
            raise Inconsistent(transaction,
                               msg.format(pocketFrom, lunitsFrom, tunitsFrom))

        # Transform Lots to the destination Security/units and apply
        # as a Trade (to order closed Lots, if any) with opentxid & opendt
        # preserved from the source Lot
        gains = []
        for lotFrom in lotsFrom:
            units = lotFrom.units * ratio
            # FIXME - We need a Trade.id for self.trade() to set
            # Lot.createtxid, but "id=transaction.id" is problematic.
            trade = Transaction(
                id=transaction.id,
                uniqueid=transaction.uniqueid,
                datetime=transaction.datetime,
                type='transfer',
                memo=transaction.memo,
                currency=lotFrom.currency,
                cash=-lotFrom.price * lotFrom.units,
                fiaccount=transaction.fiaccount,
                security=transaction.security,
                units=units
            )
            gs = self.trade(trade, opentransaction=lotFrom.opentransaction,
                            createtransaction=transaction)
            gains.extend(gs)

        return gains

    def spinoff(self, transaction, sort=None):
        """
        Remove cost from Position to create Lots in a new Security, preserving
        the holding period through the spinoff and not removing units or
        closing Lots from the source Position.
        """
        self._validate_args(transaction,
                            units=Decimal, numerator=Decimal,
                            denominator=Decimal,
                            securityPrice=(type(None), Decimal),
                            securityFromPrice=(type(None), Decimal))

        units = transaction.units
        securityFrom = transaction.securityFrom
        numerator = transaction.numerator
        denominator = transaction.denominator
        securityPrice = transaction.securityPrice
        securityFromPrice = transaction.securityFromPrice

        pocketFrom = (transaction.fiaccount, securityFrom)
        positionFrom = self[pocketFrom]

        splitRatio = numerator / denominator

        costFraction = 0
        if securityPrice and securityFromPrice:
            costFraction = Decimal(securityPrice * units) / (
                Decimal(securityPrice * units)
                + securityFromPrice * units / splitRatio)

        # Take the basis from the source Position
        lotsFrom = positionFrom.take_basis(openAsOf(transaction.datetime),
                                           costFraction)

        takenUnits = sum([lot.units for lot in lotsFrom])
        if abs(takenUnits * splitRatio - units) > 0.0001:
            msg = ("Spinoff {} for {} requires {} units={} (not units={}) "
                   "to yield {} units={}")
            raise Inconsistent(transaction,
                               msg.format(numerator, denominator, securityFrom,
                                          units / splitRatio, takenUnits,
                                          transaction.security, units))

        # Transform Lots to the destination Security/units and apply
        # as a Trade (to order closed Lots, if any) with opentxid & opendt
        # preserved from the source Lot
        gains = []
        for lotFrom in lotsFrom:
            units = lotFrom.units * splitRatio
            # FIXME - We need a Trade.id for self.trade() to set
            # Lot.createtxid, but "id=transaction.id" is problematic.
            trade = Transaction(
                id=transaction.id,
                uniqueid=transaction.uniqueid,
                datetime=transaction.datetime,
                type='trade',
                memo=transaction.memo,
                currency=lotFrom.currency,
                cash=-lotFrom.price * lotFrom.units,
                fiaccount=transaction.fiaccount,
                security=transaction.security,
                units=units
            )
            gs = self.trade(trade, opentransaction=lotFrom.opentransaction,
                            createtransaction=transaction, sort=sort)
            gains.extend(gs)

        return gains

    def exercise(self, transaction, sort=None):
        """
        Options exercise
        """
        self._validate_args(transaction, unitsFrom=Decimal, cash=Decimal)

        unitsFrom = transaction.unitsFrom
        cash = transaction.cash

        positionFrom = self[(transaction.fiaccount, transaction.securityFrom)]

        # Remove lots from the source Position
        try:
            lotsFrom = positionFrom.take(openAsOf(transaction.datetime),
                                         -unitsFrom)
        except Inconsistent as err:
            raise Inconsistent(transaction, err.msg)

        takenUnits = sum([lot.units for lot in lotsFrom])
        if abs(takenUnits) - abs(unitsFrom) > 0.0001:
            msg = "Exercise Lot.units={} (not {})"
            raise Inconsistent(transaction, msg.format(takenUnits, unitsFrom))

        multiplier = abs(transaction.units / unitsFrom)

        # Transform Lots to the destination Security/units, add additional
        # exercise cash as cost pro rata, and apply as a Trade (to order
        # closed Lots, if any)
        gains = []
        for lotFrom in lotsFrom:
            adjusted_price = -lotFrom.price * lotFrom.units \
                    + cash * lotFrom.units / takenUnits

            units = lotFrom.units * multiplier
            # FIXME - We need a Trade.id for self.trade() to set
            # Lot.createtxid, but "id=transaction.id" is problematic.
            trade = Transaction(
                type='trade', id=transaction.id,
                fiaccount=transaction.fiaccount, uniqueid=transaction.uniqueid,
                datetime=transaction.datetime, memo=transaction.memo,
                security=transaction.security, units=units,
                cash=adjusted_price, currency=transaction.currency)
            gs = self.trade(trade, sort=sort)
            gains.extend(gs)

        return gains

    @staticmethod
    def _validate_args(transaction, **kwargs):
        for arg, val in kwargs.items():
            attr = getattr(transaction, arg)
            if not isinstance(attr, val):
                # Unpack kwarg value sequences
                if hasattr(val, '__getitem__'):
                    val = tuple(v.__name__ for v in val)
                else:
                    val = val.__name__
                attrname = "{}.{}".format(transaction.__class__.__name__, arg)
                msg = "{} must be type {}, not {}: {}".format(
                    attrname, val, type(attr).__name__, transaction)
                raise ValueError(msg)

    def processTransaction(self, transaction, sort=None):
        sort = sort or globals().get(transaction.sort, None)
        handler = getattr(self, transaction.type)
        gains = handler(transaction, sort=sort)
        return gains


###############################################################################
# FILTER CRITERIA
###############################################################################
def openAsOf(datetime_):
    """
    Filter function that chooses Lots created on or before datetime
    """
    def isOpen(enum):
        index, lot = enum
        lot_open = lot.createtransaction.datetime <= datetime_
        return lot_open
    return isOpen


def longAsOf(datetime_):
    """
    Filter function that chooses long Lots (i.e. positive units) created
    on or before datetime
    """
    def isOpen(enum):
        index, lot = enum
        lot_open = lot.createtransaction.datetime <= datetime_
        lot_long = lot.units > 0
        return lot_open and lot_long
    return isOpen


def closableBy(transaction):
    """
    Filter function that chooses Lots created on or before the given
    transaction.datetime, with sign opposite to the given transaction.units
    """
    def closeMe(enum):
        index, lot = enum
        lot_open = lot.createtransaction.datetime <= transaction.datetime
        opposite_sign = lot.units * transaction.units < 0
        return lot_open and opposite_sign
    return closeMe


###############################################################################
# SORT FUNCTIONS
###############################################################################
def sort_oldest(lot):
    """
    Sort by holding period, then by opening Transaction.uniqueid
    """
    opendt = lot.opentransaction.datetime
    opentxid = lot.opentransaction.uniqueid
    assert opendt is not None
    return (str(opendt), opentxid or '')


def sort_cheapest(lot):
    """
    Sort by price, then by opening Transaction.uniqueid
    """
    # FIXME this doesn't sort stably for identical prices but different lots
    price = lot.price
    opentxid = lot.opentransaction.uniqueid
    assert price is not None
    return (price, opentxid or '')


def sort_dearest(lot):
    """
    Sort by price, then by opening Transaction.uniqueid
    """
    # FIXME this doesn't sort stably for identical prices but different lots
    price = lot.price
    opentxid = lot.opentransaction.uniqueid
    assert price is not None
    return (-price, opentxid or '')


FIFO = {'key': sort_oldest, 'reverse': False}
LIFO = {'key': sort_oldest, 'reverse': True}
MINGAIN = {'key': sort_dearest, 'reverse': False}
MAXGAIN = {'key': sort_cheapest, 'reverse': False}
