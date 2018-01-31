import csv
from datetime import date, datetime, timedelta
from decimal import Decimal
from collections import namedtuple
import logging


import sqlalchemy
# from sqlalchemy.orm.exc import NoResultFound


from capgains.models.transactions import (Fi, FiAccount, Security, SecurityId)
from capgains.ofx.reader import OfxStatementReader
from capgains.database import (Base, sessionmanager)


class CsvStatement(object):
    def __init__(self):
        self.transactions = []


class CsvTransactionReader(csv.DictReader, OfxStatementReader):
    BROKERID = 'etrade.com'

    def __init__(self, session, csvfile):
        self.session = session
        # Conform to ofx.OfxStatementReader interface
        self.statement = CsvStatement()
        self.securities = {}
        self.transactions = []
        row = next(csvfile)
        caption, acctid = row.split(',')
        assert caption == 'For Account:'
        acctid = acctid.strip()

        fi = Fi.merge(session, brokerid=self.BROKERID)

        self.account = FiAccount.merge(session, fi=fi, number=acctid)

        # Skip blank line before headers
        next(csvfile)
        super(CsvTransactionReader, self).__init__(csvfile)
        # Skip blank line after headers
        # next(self)

    def read(self):
        # E*Trade CSV files list most recent transactions first
        txs = enumerate(reversed(list(self)))
        self.statement.transactions = [self.parse(i, tx) for i, tx in txs]
        self.read_transactions()
        return self.transactions

    def parse(self, index, row):
        dt = datetime.strptime(row['TransactionDate'], '%m/%d/%y')
        # CSV file trade dates have a resolution of days, so it's easy
        # to get trades that have all fields identical.
        # To avoid collisions from make_uid(), we increment the datetime
        # by 1 min per row of the file.
        dt += timedelta(minutes=index)
        ticker = row['Symbol']
        self.merge_security(ticker)
        return CsvTransaction(
            fitid=None, dttrade=dt, dtsettle=dt, memo=row['Description'],
            uniqueidtype='TICKER', uniqueid=ticker,
            units=Decimal(row['Quantity']), currency='USD',
            total=Decimal(row['Amount']), type=row['TransactionType'],
        )

    def merge_security(self, ticker, name=None):
        """
        E*Trade CSV files don't include CUSIPS, only tickers, so we create a
        new uniqueidtype=TICKER and use that.

        CSV files also don't include a list of securities, so we create that
        here as we parse transactions.
        """
        if ('TICKER', ticker) in self.securities:
            return

        secid = self.session.query(SecurityId)\
                .filter_by(uniqueidtype='TICKER', uniqueid=ticker)\
                .one_or_none()
        if secid is None:
            security = Security.merge(self.session, ticker=ticker,
                                      uniqueidtype='TICKER', uniqueid=ticker)
        else:
            security = secid.security
        self.securities[('TICKER', ticker)] = security

    def groupTransactions(self, transaction):
        """
        Sort key for grouping transactions for dispatch (transaction_handlers)
        """
        return self.transaction_handlers.get(transaction.type, '')

    transaction_handlers = {'Bought': 'doTrades',
                            'Sold': 'doTrades',
                            'Cancel Bought': 'doTrades',
                            'Cancel Sold': 'doTrades',
                            'Dividend': 'doCashTransactions',
                            'Reorganization': ''}

    ###########################################################################
    # TRADES
    ###########################################################################
    @staticmethod
    def groupTrades(tx):
        """
        E*Trade CSV transaction dttrades only have a resolution of days
        """
        dttrade = tx.dttrade
        dttrade = date(dttrade.year, dttrade.month, dttrade.day)
        return (tx.uniqueidtype, tx.uniqueid, dttrade, abs(tx.units))

    @staticmethod
    def filterTradeCancels(transaction):
        return 'cancel' in transaction.memo.lower()

    ###########################################################################
    # CASH TRANSACTIONS
    ###########################################################################
    @staticmethod
    def filterCashTransactions(transaction):
        """
        """
        memo = transaction.memo.lower()
        return 'liqd' in memo or 'ret cap' in memo


def read(session, file_):
    with open(file_) as f:
        reader = CsvTransactionReader(session, f)
        reader.read()


###############################################################################
# DATA CONTAINERS
###############################################################################
CsvTransaction = namedtuple('CsvTransaction', [
    'fitid', 'dttrade', 'dtsettle', 'memo', 'uniqueidtype', 'uniqueid', 'units',
    'currency', 'total', 'type',
    # 'reportdate', 'code',
])


###############################################################################
# CLI SCRIPT
###############################################################################
def main():
    from argparse import ArgumentParser

    argparser = ArgumentParser(description='Parse Etrade CSV data')
    argparser.add_argument('file', nargs='+', help='CSV file(s)')
    argparser.add_argument('--database', '-d', default='sqlite://',
                           help='Database connection')
    argparser.add_argument('--verbose', '-v', action='count', default=0)
    args = argparser.parse_args()

    logLevel = (3 - min(args.verbose, 2)) * 10
    logging.basicConfig(level=logLevel)
    logging.captureWarnings(True)

    engine = sqlalchemy.create_engine(args.database)
    Base.metadata.create_all(bind=engine)

    for f in args.file:
        with sessionmanager(bind=engine) as session:
            print(f)
            read(session, f)


if __name__ == '__main__':
    main()
