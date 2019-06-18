# coding: utf-8
""" """
# stdlib imports
import datetime
import itertools


# Local imports
from capgains.ofx import reader
from capgains.flex.reader import FlexStatementReader
from capgains.containers import GroupedList


BROKERID = "ameritrade.com"


class OfxStatementReader(reader.OfxStatementReader):
    @staticmethod
    def filterTrades(transaction):
        """
        All the trade corrections we have in AMTD datastream are pure BS.

        Args: transaction - ofxtools.models.investment.{BUY*, SELL*} instance
        """
        return "TRADE CORRECTION" not in transaction.memo

    def doTransfers(self, transactions):
        for key, txs in itertools.groupby(transactions, key=self.groupTransfers):
            datetrade, memo = key
            handler = self.handler_for_transfer_memo(memo)
            txs = list(txs)
            handler(txs, memo)

    @staticmethod
    def groupTransfers(tx):
        """
        AMTD doesn't always book both legs of a reorg simultaneously;
        group by transaction date rather than datetime.
        """
        dttrade = tx.dttrade
        date = datetime.date(dttrade.year, dttrade.month, dttrade.day)
        return (date, tx.memo)

    def handler_for_transfer_memo(self, memo):
        if "EXCHANGE" in memo:
            handler = self.reorg
        elif "STOCK DIVIDEND" in memo:
            handler = self.stock_dividend
        elif "EXERCISE" in memo:
            handler = self.exercise
        else:
            handler = self.transfer

        return handler

    def transfer(self, transactions, memo):
        super(OfxStatementReader, self).doTransfers(transactions)

    def reorg(self, transactions, memo):
        """
        HACK based on assumption that txs is a pair with tfaction={"OUT", "IN"}
        """
        assert len(transactions) == 2
        transactions.sort(key=lambda x: x.tferaction)
        assert [tx.tferaction for tx in transactions] == ["IN", "OUT"]
        dest, src = transactions
        tx = FlexStatementReader.merge_security_transfer(self, src, dest, memo)

    def stock_dividend(self, transactions, memo):
        pass

    def exercise(self, transactions, memo):
        """
        HACK based on assumption that all transactions in list represent a
        single transaction
        """
        # Not ready for prime time
        return

        txs = (
            GroupedList(transactions)
            .groupby(self.groupExercises)
            .reduce(self.netExercises)
            .flatten()
            .sorted(self.sortExercises)
        )
        txs = txs.pop(None)
        assert len(txs) == 2
        dest, src = txs
        assert dest.tferaction == "IN"
        assert src.tferaction == "OUT"

        security = self.securities[(dest.uniqueidtype, dest.uniqueid)]
        fromsecurity = self.securities[(src.uniqueidtype, src.uniqueid)]

        # FIXME - exercise cash is sent as INVBANKTRAN; can't get it from
        # just the TRANSFERS which are dispatched to here.
        tx = self.merge_transaction(
            type="exercise",
            fiaccount=self.account,
            uniqueid=src.fitid,
            datetime=src.dttrade,
            memo=memo,
            security=security,
            units=dest.units,
            cash=0,
            fromfiaccount=None,
            fromsecurity=fromsecurity,
            fromunits=src.units,
        )
        return tx

    @staticmethod
    def groupExercises(tx):
        return ((tx.uniqueidtype, tx.uniqueid), tx.tferaction, tx.postype)

    @staticmethod
    def netExercises(tx0, tx1):
        units0 = tx0.units
        if tx0.postype == "SHORT":
            units0 *= -1
            tx0.postype = "LONG"
        units1 = tx1.units
        if tx1.postype == "SHORT":
            units1 *= -1
        tx0.units += tx1.units
        return tx0

    @staticmethod
    def sortExercises(tx):
        return tx.tferaction
