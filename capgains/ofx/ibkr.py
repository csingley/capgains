# coding: utf-8
"""
"""
# stdlib imports
import logging


# 3rd party import
from sqlalchemy import create_engine


# Local imports
from capgains.ofx import reader
from capgains.flex.reader import (FlexStatementReader,)
from capgains.database import (Base, sessionmanager)


BROKERID = '4705'


class OfxStatementReader(FlexStatementReader):
    # Get back OfxStatementReader superclass methods that we need which were
    # overriden in FlexStatementReader class definition
    __init__ = reader.OfxStatementReader.__init__
    read = reader.OfxStatementReader.read
    read_securities = reader.OfxStatementReader.read_securities
    transaction_handlers = reader.OfxStatementReader.transaction_handlers
    doTransfers = reader.OfxStatementReader.doTransfers

    @staticmethod
    def filterTrades(transaction):
        isFx = False
        if transaction.memo and 'CASH TRADE' in transaction.memo:
            isFx = True
        return not isFx

    @staticmethod
    def filterTradeCancels(transaction):
        cancel = False
        memo = transaction.memo
        if memo and 'cancel' in memo.lower():
            cancel = True
        return cancel

    @staticmethod
    def sortCanceledTrades(transaction):
        return transaction.fitid

    @staticmethod
    def filterCashTransactions(transaction):
        memo = transaction.memo.lower()
        return 'return of capital' in memo or 'interimliquidation' in memo

    @staticmethod
    def groupCashTransactionsForCancel(transaction):
        """
        Cash transactions are grouped together for cancellation/netting
        if they're for the same security at the same time with the same memo.

        Args: transaction - instance implementing the interface of
                            ofxtools.models.investment.INCOME
        """
        security = (transaction.uniqueidtype, transaction.uniqueid)
        memo = transaction.memo.replace(' - REVERSAL', '')
        return transaction.dttrade, security, memo

    def fixCashTransactions(self, transaction):
        """
        Override the dttrade -> dtsettle jiggery pokery from superclass method
        """
        return transaction


###############################################################################
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
