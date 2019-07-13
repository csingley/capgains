from __future__ import annotations

import csv
from datetime import date, datetime, timedelta
from decimal import Decimal
from collections import namedtuple
import logging
from typing import Tuple

import sqlalchemy

from capgains import models
from capgains import ofx, flex
from capgains.database import Base, sessionmanager


class CsvStatement(object):
    BROKERID = "etrade.com"

    def __init__(self):
        self.acctid = ""
        self.seclist = []
        self.transactions = []


class CsvTransactionReader(csv.DictReader, ofx.reader.OfxStatementReader):

    def __init__(self, csvfile):
        #  Set up a statement analogous to INVSTMTRS
        self.statement = CsvStatement()

        #  First row gives acct#
        firstrow = next(csvfile)
        caption, acctid = firstrow.split(",")
        assert caption == "For Account:"
        self.statement.acctid = acctid.strip()

        #  Skip blank line before headers.
        next(csvfile)
        #  Remaining rows are transaction data; read them.
        super().__init__(csvfile)

        # E*Trade CSV files list most recent transactions first.
        txs = enumerate(reversed(list(self)))
        seclist, transactions = zip(*[self.parse(i, tx) for i, tx in txs])

        # Conform to the rest of ofx.OfxStatementReader instance attributes.
        self.statement.transactions = list(transactions)
        self.seclist = list(seclist)

        #  Initialize reading results collections.
        self.securities = {}
        self.transactions = []

    @staticmethod
    def read_default_currency(statement: ofx.reader.Statement) -> str:
        return "USD"

    @staticmethod
    def read_account(
        statement: ofx.reader.Statement,
        session: sqlalchemy.orm.session.Session,
    ) -> models.FiAccount:
        assert isinstance(statement, CsvStatement)
        fi = models.Fi.merge(session, brokerid=statement.BROKERID)
        return models.FiAccount.merge(session, fi=fi, number=statement.acctid)

    def parse(
        self,
        index: int,
        row: dict
    ) -> Tuple[flex.Types.Security, CsvTransaction]:
        dt = datetime.strptime(row["TransactionDate"], "%m/%d/%y")
        # CSV file trade dates have a resolution of days, so it's easy
        # to get trades that have all fields identical.
        # To avoid collisions from make_uid(), we increment the datetime
        # by 1 min per row of the file.
        dt += timedelta(minutes=index)

        #  E*Trade CSV files don't include CUSIPS, only tickers, so we create a
        #  new uniqueidtype=TICKER and use that.
        uniqueidtype = "TICKER"
        ticker = row["Symbol"]
        security = flex.Types.Security(
            uniqueidtype=uniqueidtype,
            uniqueid=ticker,
            secname=None,
            ticker=ticker,
        )
        #  self.merge_security(ticker)
        transaction = CsvTransaction(
            fitid=None,
            dttrade=dt,
            dtsettle=dt,
            memo=row["Description"],
            uniqueidtype=uniqueidtype,
            uniqueid=ticker,
            units=Decimal(row["Quantity"]),
            currency="USD",
            total=Decimal(row["Amount"]),
            type=row["TransactionType"],
        )
        return security, transaction

    #  def merge_security(self, ticker, name=None):
        #  """
        #  E*Trade CSV files don't include CUSIPS, only tickers, so we create a
        #  new uniqueidtype=TICKER and use that.

        #  CSV files also don't include a list of securities, so we create that
        #  here as we parse transactions.
        #  """
        #  if ("TICKER", ticker) in self.securities:
            #  return

        #  secid = (
            #  self.session.query(models.SecurityId)
            #  .filter_by(uniqueidtype="TICKER", uniqueid=ticker)
            #  .one_or_none()
        #  )
        #  if secid is None:
            #  security = models.Security.merge(
                #  self.session, ticker=ticker, uniqueidtype="TICKER", uniqueid=ticker
            #  )
        #  else:
            #  security = secid.security
        #  self.securities[("TICKER", ticker)] = security

    def name_handler_for_tx(self, transaction: ofx.reader.Transaction) -> str:
        """Sort key to group transactions for dispatch (TRANSACTION_HANDLERS).
        """
        assert isinstance(transaction, CsvTransaction)
        TRANSACTION_HANDLERS = {
            "Bought": "doTrades",
            "Sold": "doTrades",
            "Cancel Bought": "doTrades",
            "Cancel Sold": "doTrades",
            "Dividend": "doCashTransactions",
            "Reorganization": "",
        }
        return TRANSACTION_HANDLERS.get(transaction.type, "")


    ###########################################################################
    # TRADES
    ###########################################################################
    @staticmethod
    def group_trades(tx):
        """E*Trade CSV transaction dttrades only have a resolution of days.
        """
        dttrade = tx.dttrade
        tradedate = date(dttrade.year, dttrade.month, dttrade.day)
        return (tx.uniqueidtype, tx.uniqueid, tradedate, abs(tx.units))

    @staticmethod
    def is_trade_cancel(transaction):
        return "cancel" in transaction.memo.lower()

    ###########################################################################
    # CASH TRANSACTIONS
    ###########################################################################
    @staticmethod
    def is_retofcap(transaction):
        """
        """
        memo = transaction.memo.lower()
        return "liqd" in memo or "ret cap" in memo


def read(session, file_):
    with open(file_) as f:
        reader = CsvTransactionReader(session, f)
        reader.read()


###############################################################################
# DATA CONTAINERS
###############################################################################
CsvTransaction = namedtuple(
    "CsvTransaction",
    [
        "fitid",
        "dttrade",
        "dtsettle",
        "memo",
        "uniqueidtype",
        "uniqueid",
        "units",
        "currency",
        "total",
        "type",
        # 'reportdate', 'code',
    ],
)


###############################################################################
# CLI SCRIPT
###############################################################################
def main():
    from argparse import ArgumentParser

    argparser = ArgumentParser(description="Parse Etrade CSV data")
    argparser.add_argument("file", nargs="+", help="CSV file(s)")
    argparser.add_argument(
        "--database", "-d", default="sqlite://", help="Database connection"
    )
    argparser.add_argument("--verbose", "-v", action="count", default=0)
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


if __name__ == "__main__":
    main()
