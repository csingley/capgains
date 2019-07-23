from __future__ import annotations

import csv
from datetime import date, datetime, timedelta
from decimal import Decimal
import logging
from typing import Tuple, NamedTuple, Any, Callable, Iterable, List, Optional

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
        try:
            caption, acctid = firstrow.split(",")
        except ValueError:
            raise ValueError(f"Bad first row '{firstrow}'")
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

    def dispatch_transaction(
        self,
        transaction: ofx.reader.Transaction,
    ) -> Optional[
        Callable[
            [
                Iterable[ofx.reader.Trade],
                sqlalchemy.orm.session.Session,
                ofx.reader.SecuritiesMap,
                models.FiAccount,
                str,
            ],
            List[models.Transaction]
        ]
    ]:
        """Sort key to group transactions for dispatch (TRANSACTION_DISPATCHER).
        """
        assert isinstance(transaction, CsvTransaction)
        key = transaction.type
        handler = self.TRANSACTION_DISPATCHER.get(key, None)
        if handler is None:
            return handler
        #  Bind class method to instance.
        #  Python functions use descriptor protocol to call types.MethodType
        #  to bind functions as methods; hook into that machinery here.
        #  https://docs.python.org/3/howto/descriptor.html#functions-and-methods
        return handler.__get__(self)

    TRANSACTION_DISPATCHER = {  # type: ignore
        "Bought": ofx.reader.OfxStatementReader.doTrades,
        "Sold": ofx.reader.OfxStatementReader.doTrades,
        "Cancel Bought": ofx.reader.OfxStatementReader.doTrades,
        "Cancel Sold": ofx.reader.OfxStatementReader.doTrades,
        "Dividend": ofx.reader.OfxStatementReader.doCashTransactions,
    }

    @staticmethod
    def sort_trades_to_cancel(transaction: ofx.reader.Trade) -> Any:
        """Use dttrade; CSV files don't provide transaction unique identifiers.
        """
        return transaction.dttrade

    ###########################################################################
    # TRADES
    ###########################################################################
    @staticmethod
    def fingerprint_trade(tx):
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
        memo = transaction.memo.lower()
        return "liqd" in memo or "ret cap" in memo


def read(session, file_):
    with open(file_) as f:
        reader = CsvTransactionReader(f)
        return reader.read(session)


###############################################################################
# DATA CONTAINERS
###############################################################################
class CsvTransaction(NamedTuple):
    fitid: str
    dttrade: datetime
    dtsettle: datetime
    memo: str
    uniqueidtype: str
    uniqueid: str
    units: Decimal
    currency: str
    total: Decimal
    type: str


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

    with sessionmanager(bind=engine) as session:
        for f in args.file:
            print(f)
            transactions = read(session, f)
            for transaction in transactions:
                print(transaction)


if __name__ == "__main__":
    main()
