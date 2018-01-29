# coding: utf-8
"""
Creates model instances from OFX downloads.
"""
# stdlib imports
from collections import namedtuple
import itertools
import operator
import hashlib
import logging
import warnings


# 3rd party imports
from sqlalchemy import create_engine
from ofxtools.utils import cusip2isin


# Local imports
from capgains.models.transactions import (
    FiAccount, Security, Transaction,
)
from capgains.containers import GroupedList
from capgains.database import (Base, sessionmanager)


class OfxResponseReader(object):
    """
    Processor for ofxtools.models.ofx.OFX instance
    """
    def __init__(self, session, response):
        """
        Args: session - sqlalchemy.orm.session.Session instance
              response - ofxtools.models.ofx.OFX instance
        """
        self.session = session
        self.statements = [OfxStatementReader(session, stmt)
                           for stmt in response]

    def read(self):
        for stmt in self.statements:
            stmt.read()


class OfxStatementReader(object):
    """
    Processor for ofxtools.models.INVSTMTRS instance
    """
    def __init__(self, session, statement=None, seclist=None):
        """
        Args: session - sqlalchemy.orm.session.Session instance
              statement - ofxtools.models.investment.INVSTMTRS instance
              seclist - ofxtools.models.seclist.SECLIST instance
        """
        self.session = session
        self.statement = statement
        self.seclist = seclist
        self.account = None
        self.index = None
        self.securities = {}
        self.transactions = []

    def read(self, doTransactions=True):
        self.read_account()
        self.read_securities()
        if doTransactions:
            self.read_transactions()

    def read_account(self):
        account = self.statement.account
        self.account = FiAccount.merge(self.session, brokerid=account.brokerid,
                                       number=account.acctid)

    def read_securities(self):
        for sec in self.seclist:
            uniqueidtype = sec.uniqueidtype
            uniqueid = sec.uniqueid
            secname = sec.secname
            ticker = sec.ticker
            sec = Security.merge(self.session, uniqueidtype=uniqueidtype,
                                 uniqueid=uniqueid, name=secname,
                                 ticker=ticker)
            self.securities[(uniqueidtype, uniqueid)] = sec
            # Also do ISIN; why not?
            if uniqueidtype == 'CUSIP':
                try:
                    uniqueid = cusip2isin(uniqueid)
                    uniqueidtype = 'ISIN'
                    sec = Security.merge(
                        self.session, uniqueidtype=uniqueidtype,
                        uniqueid=uniqueid, name=secname, ticker=ticker)
                    self.securities[(uniqueidtype, uniqueid)] = sec
                except ValueError:
                    pass

    def read_transactions(self):
        """
        Group parsed statement transaction instances and dispatch groups to
        relevant handler functions
        """
        self.statement.transactions.sort(key=self.groupTransactions)
        for handler, transactions in itertools.groupby(
                self.statement.transactions, key=self.groupTransactions):
            if handler:
                handler = getattr(self, handler)
                handler(transactions)

    def groupTransactions(self, transaction):
        """ Group parsed statement transaction instances by class name """
        return self.transaction_handlers.get(
            transaction.__class__.__name__, '')

    transaction_handlers = {'BUYDEBT': 'doTrades',
                            'SELLDEBT': 'doTrades',
                            'BUYMF': 'doTrades',
                            'SELLMF': 'doTrades',
                            'BUYOPT': 'doTrades',
                            'SELLOPT': 'doTrades',
                            'BUYOTHER': 'doTrades',
                            'SELLOTHER': 'doTrades',
                            'BUYSTOCK': 'doTrades',
                            'SELLSTOCK': 'doTrades',
                            'INCOME': 'doCashTransactions',
                            'INVEXPENSE': 'doCashTransactions',
                            'TRANSFER': 'doTransfers',
                            }

    ###########################################################################
    # TRADES
    ###########################################################################
    def doTrades(self, transactions):
        """
        Preprocess trade transactions and send to merge_trade().

        The logic here eliminates unwanted trades (e.g. FX) and groups trades
        to net out canceled trades.

        Args: transactions - a sequence of instances implementing the interface
                             of ofxtools.models.investment.{BUY*, SELL*}
                             (as used by the methods below)
        """
        txs = GroupedList(transactions)\
                .filter(self.filterTrades)\
                .groupby(self.groupTradesForCancel)\
                .cancel(filterfunc=self.filterTradeCancels,
                        matchfunc=self.matchTradeWithCancel,
                        sortfunc=self.sortCanceledTrades)\
                .filter(operator.attrgetter('units'))\
                .map(self.merge_trade)

    @staticmethod
    def filterTrades(transaction):
        """
        Should this trade be processed?  Implement in subclass.

        Arg: an instance implementing the interface of
              ofxtools.models.investment.{BUY*, SELL*}
        """
        return True

    @staticmethod
    def groupTradesForCancel(transaction):
        """
        Transactions are grouped if they have the same security/datetime
        and matching units.  abs(units) is used so that trade cancellations
        are grouped together with the trades they cancel.

        Arg: an instance implementing the interface of
              ofxtools.models.investment.{BUY*, SELL*}
        """
        return (transaction.uniqueidtype, transaction.uniqueid,
                transaction.dttrade, abs(transaction.units))

    @staticmethod
    def filterTradeCancels(transaction):
        """
        Is this trade actually a trade cancellation?  Implement in subclass.

        Arg: an instance implementing the interface of
              ofxtools.models.investment.{BUY*, SELL*}
        Returns: boolean
        """
        return False

    @staticmethod
    def matchTradeWithCancel(transaction0, transaction1):
        """
        Does one of these trades cancel the other?

        Args: two instances implementing the interface of
              ofxtools.models.investment.{BUY*, SELL*}
        Returns: boolean
        """
        return transaction0.units == -1 * transaction1.units

    @staticmethod
    def sortCanceledTrades(transaction):
        """
        Determines order in which trades are canceled.

        Arg: transaction - instance implementing the interface of
                           ofxtools.models.investment.{BUY*, SELL*}
        """
        return transaction.fitid

    def merge_trade(self, tx, memo=None):
        """
        Process a trade into data fields to hand off to merge_transaction()
        to persist in the database.

        Args: transaction - instance implementing the interface of
                            ofxtools.models.investment.{BUY*, SELL*}
              memo - override transaction memo (type str)
        """
        security = self.securities[(tx.uniqueidtype, tx.uniqueid)]
        # Works with Flex currency attribute
        currency = tx.currency
        # Works with OFX Currency Aggregate
        if hasattr(currency, 'cursym'):
            currency = currency.cursym
        currency = currency or getattr(self.statement, 'curdef', None) or None

        sort = self.sortForTrade(tx)
        return self.merge_transaction(
            type='trade', fiaccount=self.account,
            uniqueid=tx.fitid, datetime=tx.dttrade, memo=memo or tx.memo,
            security=security, units=tx.units, currency=currency,
            cash=tx.total, sort=sort)

    @staticmethod
    def sortForTrade(transaction):
        """
        What flex.parser sort algorithm that applies to this transaction?

        Implement in subclass.

        Arg: transaction - instance implementing the interface of
                           ofxtools.models.investment.{BUY*, SELL*}
        """
        pass

    ###########################################################################
    # CASH TRANSACTIONS
    ###########################################################################
    def doCashTransactions(self, transactions):
        """
        Preprocess cash transactions and send to merge_retofcap().

        The logic here filters only for return of capital transactions;
        groups them to apply reversals; nets cash transactions remaining in
        each group; and persists to the database after applying any final
        preprocessing applied by fixCashTransactions().

        It's important to net cash transactions remaining in a group that
        aren't cancelled, since the cash totals of reversing transactions
        often don't match the totals of the transactions being reversed
        (e.g. partial reversals to recharacterize to/from payment in lieu).

        Args: transactions - a sequence of instances implementing the interface
                             of ofxtools.models.investment.INCOME
        """
        txs = GroupedList(transactions)\
                .filter(self.filterCashTransactions)\
                .groupby(self.groupCashTransactionsForCancel)\
                .cancel(filterfunc=self.filterCashTransactionCancels,
                        matchfunc=self.matchCashTransactionWithCancel,
                        sortfunc=self.sortCanceledCashTransactions)\
                .reduce(self.netCashTransactions)\
                .filter(operator.attrgetter('total'))\
                .map(self.fixCashTransactions)\
                .map(self.merge_retofcap)

    @staticmethod
    def filterCashTransactions(transaction):
        """
        Judge whether a transaction should be processed (i.e. it's a return
        of capital).

        Implement in subclass.

        Args: transaction - instance implementing the interface of
                            ofxtools.models.investment.INCOME
        Returns: boolean
        """
        return False

    @staticmethod
    def groupCashTransactionsForCancel(transaction):
        """
        Cash transactions are grouped together for cancellation/netting
        if they're for the same security at the same time with the same memo.

        Args: transaction - instance implementing the interface of
                            ofxtools.models.investment.INCOME
        """
        security = (transaction.uniqueidtype, transaction.uniqueid)
        memo = transaction.memo
        return transaction.dttrade, security, memo

    @staticmethod
    def filterCashTransactionCancels(transaction):
        """
        Is this cash transaction actually a reversal?  Implement in subclass.

        Arg: an instance implementing the interface of
              ofxtools.models.investment.INCOME
        Returns: boolean
        """
        return False

    @staticmethod
    def matchCashTransactionWithCancel(transaction0, transaction1):
        """
        Does one of these cash transactions reverse the other?

        Args: two instances implementing the interface of
              ofxtools.models.investment.INCOME
        Returns: boolean
        """
        return transaction0.total == -1 * transaction1.total

    @staticmethod
    def sortCanceledCashTransactions(transaction):
        """
        Determines order in which cash transactions are reversed.

        Implement in subclass.

        Arg: transaction - instance implementing the interface of
                           ofxtools.models.investment.{BUY*, SELL*}
        """
        return False

    def netCashTransactions(self, transaction0, transaction1):
        """
        Combine two cash transactions by summing their totals, and taking
        the earliest of their dates.

        Args: two instances implementing the interface of
              ofxtools.models.investment.INCOME
        Returns: a CashTransaction namedtuple instance (which implements the
                 key parts of the INCOME interface)
        """
        dttrade = self._minDateTime(transaction0.dttrade, transaction1.dttrade)
        dtsettle = self._minDateTime(transaction0.dtsettle,
                                     transaction1.dtsettle)
        total = transaction0.total + transaction1.total
        return CashTransaction(
            transaction0.fitid, dttrade, dtsettle, transaction0.memo,
            transaction0.uniqueidtype, transaction0.uniqueid,
            transaction0.incometype, transaction0.currency, total)

    @staticmethod
    def _minDateTime(datetime0, datetime1):
        """
        Handle None values when taking the min of two datetimes.
        """
        dt0 = datetime0 or 0
        dt1 = datetime1 or 0
        return min(dt0, dt1) or None

    @staticmethod
    def fixCashTransactions(transaction):
        """
        Any last processing of the transaction before it's persisted to the DB.

        Implement in subclass.

        Arg: transaction - instance implementing the interface of
                           ofxtools.models.investment.INCOME
        """
        return transaction

    def merge_retofcap(self, transaction, memo=None):
        """
        Process a return of capital cash transaction into data fields to
        hand off to merge_transaction() to persist in the database.

        Args: transaction - instance implementing the interface of
                            ofxtools.models.investment.INCOME
              memo - override transaction memo (type str)
        """
        security = self.securities[(transaction.uniqueidtype,
                                    transaction.uniqueid)]
        # Work with either Flex currency attribute or OFX Currency Aggregate
        currency = transaction.currency or None
        if hasattr(currency, 'cursym'):
            currency = currency.cursym
        dttrade = transaction.dttrade
        dtsettle = getattr(transaction, 'dtsettle', None) or dttrade
        return self.merge_transaction(
            type='returnofcapital', fiaccount=self.account,
            uniqueid=transaction.fitid, datetime=dttrade,
            dtsettle=dtsettle, memo=memo or transaction.memo,
            security=security, currency=currency, cash=transaction.total)

    ###########################################################################
    # ACCOUNT TRANSFERS
    ###########################################################################
    def doTransfers(self, transactions):
        """
        Preprocess transfer transactions - not handled here in the base class,
        since OFX data doesn't really give us enough information to match the
        two sides of a transfer unless we can parse the highly broker-specific
        formatting of transaction memos.

        Implement in subclass.
        """
        for tx in transactions:
            msg = "Skipping transfer {}".format(tx)
            warnings.warn(msg)

    def merge_transaction(self, **kwargs):
        """
        Persist a transaction to the database, using merge() logic i.e. insert
        if it doesn't already exist.
        """
        kwargs['uniqueid'] = kwargs['uniqueid'] or self.make_uid(**kwargs)
        tx = Transaction.merge(self.session, **kwargs)
        self.transactions.append(tx)
        return tx

    @staticmethod
    def make_uid(type, datetime, fiaccount, security, units=None,
                 currency=None, cash=None, fiaccountFrom=None,
                 securityFrom=None, unitsFrom=None, numerator=None,
                 denominator=None, **kwargs):
        """
        We require of a Transaction.uniqueid that it be *unique* (duh) and
        *deterministic*.  Transaction.merge() uses uniqueid as a signature,
        so the same inputs must always produce the same output.

        The requirement for determinism means that uniqueids aren't going to
        sort in the order they're created, or the order they appear in the
        data stream; we can't make uniqueids increase monotonically.

        This is too bad, because the inventory module uses Transaction.uniqueid
        as a sort key (after datetime).  However, it's not as bad as all that;
        sort order really only matters for trades with identical datetimes,
        and OFX or Flex XML data always has FI-generated unique transaction IDs
        so all that's needed is to jigger the inputs for CSV trade data so
        it sorts in the desired order.
        """
        dateTime = datetime.isoformat()
        msg = ('{} {}, fiaccount={}, security={}, units={}, currency={},  '
               'cash={}, fiaccountFrom={}, securityFrom={}, unitsFrom={}, '
               'numerator={}, denominator={}').format(
                   dateTime, type, fiaccount.id, security.id, units, currency,
                   cash, getattr(fiaccountFrom, 'id', None),
                   getattr(securityFrom, 'id', None), unitsFrom,
                   numerator, denominator)

        uid = hashlib.sha256(msg.encode('utf-8')).hexdigest()
        return uid


###############################################################################
# DATA CONTAINERS
###############################################################################
CashTransaction = namedtuple('CashTransaction',
                             ['fitid', 'dttrade', 'dtsettle', 'memo',
                              'uniqueidtype', 'uniqueid', 'incometype',
                              'currency', 'total'])


##############################################################################
# CLI SCRIPT
###############################################################################
def main():
    from argparse import ArgumentParser
    from capgains.ofx import read

    argparser = ArgumentParser(description='Parse OFX data')
    argparser.add_argument('file', nargs='+', help='OFX file(s)')
    argparser.add_argument('--database', '-d', default='sqlite://',
                           help='Database connection')
    argparser.add_argument('--verbose', '-v', action='count', default=0)
    args = argparser.parse_args()

    logLevel = (3 - min(args.verbose, 2)) * 10
    logging.basicConfig(level=logLevel)
    logging.captureWarnings(True)

    engine = create_engine(args.database)
    Base.metadata.create_all(bind=engine)

    for file in args.file:
        print(file)
        with sessionmanager(bind=engine) as session:
            transactions = read(session, file)
            session.add_all(transactions)

    engine.dispose()


if __name__ == '__main__':
    main()
