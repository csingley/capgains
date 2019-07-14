# coding: utf-8
"""
"""
import logging
from typing import Any

from sqlalchemy import create_engine


from . import reader
from capgains.flex.reader import FlexStatementReader
from capgains.database import Base, sessionmanager


BROKERID = "4705"


class OfxStatementReader(FlexStatementReader):
    # Get back OfxStatementReader superclass methods that we need which were
    # overriden in FlexStatementReader class definition
    __init__ = reader.OfxStatementReader.__init__
    read = reader.OfxStatementReader.read
    read_default_currency = staticmethod(reader.OfxStatementReader.read_default_currency)  # type: ignore
    read_account = staticmethod(reader.OfxStatementReader.read_account)  # type: ignore
    read_securities = reader.OfxStatementReader.read_securities
    doTransfers = reader.OfxStatementReader.doTransfers
    TRANSACTION_HANDLERS = reader.OfxStatementReader.TRANSACTION_HANDLERS
    get_trade_sort_algo = staticmethod(reader.OfxStatementReader.get_trade_sort_algo)  # type: ignore
    cash_premerge_hook = reader.OfxStatementReader.cash_premerge_hook

    @staticmethod
    def is_security_trade(transaction: reader.Trade) -> bool:
        return "CASH TRADE" not in (transaction.memo or "")

    @staticmethod
    def is_trade_cancel(transaction: reader.Trade) -> bool:
        return "cancel" in (transaction.memo or "").lower()

    @staticmethod
    def sort_trades_to_cancel(transaction: reader.Trade) -> Any:
        return transaction.fitid

    @staticmethod
    def is_retofcap(transaction: reader.CashTransaction) -> bool:
        memo = (transaction.memo or "").lower()
        return "return of capital" in memo or "interimliquidation" in memo

    @staticmethod
    def groupCashTransactionsForCancel(transaction: reader.CashTransaction) -> Any:
        """Cash transactions are grouped together for cancellation/netting
        if they're for the same security at the same time with the same memo.
        """
        security = (transaction.uniqueidtype, transaction.uniqueid)
        memo = transaction.memo.replace(" - REVERSAL", "")
        return transaction.dttrade, security, memo


###############################################################################
# CLI SCRIPT
###############################################################################
def main():
    from argparse import ArgumentParser
    from capgains.ofx import read

    argparser = ArgumentParser(description="Parse OFX data")
    argparser.add_argument("file", nargs="+", help="OFX file(s)")
    argparser.add_argument(
        "--database", "-d", default="sqlite://", help="Database connection"
    )
    argparser.add_argument("--verbose", "-v", action="count", default=0)
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


if __name__ == "__main__":
    main()
