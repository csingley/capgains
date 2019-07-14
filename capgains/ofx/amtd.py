# coding: utf-8
""" """
import datetime
import itertools
from typing import List, Callable, Iterable, Any, cast

import sqlalchemy
import ofxtools

from capgains import ofx, flex, models
from capgains.containers import GroupedList


BROKERID = "ameritrade.com"


class OfxStatementReader(ofx.reader.OfxStatementReader):
    @staticmethod
    def is_security_trade(transaction):
        """All the trade corrections we have in AMTD datastream are pure BS.
        """
        return "TRADE CORRECTION" not in transaction.memo

    def doTransfers(
        self,
        transactions: Iterable[ofx.reader.Transaction],
        session: sqlalchemy.orm.session.Session,
        securities: ofx.reader.SecuritiesMap,
        account: models.FiAccount,
        default_currency: str,
    ) -> List[models.Transaction]:

        transactions_: List[models.Transaction] = []

        def group_key(transaction: ofx.reader.Transaction) -> Any:
            """
            AMTD doesn't always book both legs of a reorg simultaneously;
            group by transaction date rather than datetime.
            """
            assert isinstance(transaction.dttrade, datetime.datetime)
            return (transaction.dttrade.date(), transaction.memo)

        for key, txs in itertools.groupby(transactions, key=group_key):
            datetrade, memo = key
            handler = self.handler_for_transfer_memo(memo)
            transactions_.extend(handler(list(txs), memo))

        return transactions_

    def handler_for_transfer_memo(self, memo: str) -> Callable:
        if "EXCHANGE" in memo:
            handler = self.reorg
        elif "STOCK DIVIDEND" in memo:
            handler = self.stock_dividend
        elif "EXERCISE" in memo:
            handler = self.exercise
        else:
            handler = self.transfer

        return handler

    def transfer(
        self,
        transactions: List[ofxtools.models.TRANSFER],
        session: sqlalchemy.orm.session.Session,
        securities: ofx.reader.SecuritiesMap,
        account: models.FiAccount,
        default_currency: str,
        memo: str,
    ) -> List[models.Transaction]:
        return super().doTransfers(
            transactions,
            session,
            securities,
            account,
            default_currency,
        )

    @staticmethod
    def reorg(
        transactions: List[ofxtools.models.TRANSFER],
        session: sqlalchemy.orm.session.Session,
        securities: ofx.reader.SecuritiesMap,
        account: models.FiAccount,
        default_currency: str,
        memo: str,
    ) -> List[models.Transaction]:
        """
        HACK based on assumption that txs is a pair with tferaction={"OUT", "IN"}
        """
        assert len(transactions) == 2
        for tx in transactions:
            assert isinstance(tx, ofxtools.models.TRANSFER)
            assert tx.tferaction in ("OUT", "IN")

        transactions.sort(key=lambda x: x.tferaction)
        assert [tx.tferaction for tx in transactions] == ["IN", "OUT"]
        dest, src = transactions
        transaction = flex.reader.merge_security_transfer(
            session,
            securities,
            account,
            cast(flex.Types.CorporateAction, src),  # HACK FIXME
            cast(flex.Types.CorporateAction, dest),  # HACK FIXME
            memo,
        )
        return [transaction]

    @staticmethod
    def stock_dividend(
        transactions: List[ofxtools.models.TRANSFER],
        session: sqlalchemy.orm.session.Session,
        securities: ofx.reader.SecuritiesMap,
        account: models.FiAccount,
        default_currency: str,
        memo: str,
    ) -> List[models.Transaction]:
        return []

    @staticmethod
    def exercise(
        transactions: List[ofxtools.models.TRANSFER],
        session: sqlalchemy.orm.session.Session,
        securities: ofx.reader.SecuritiesMap,
        account: models.FiAccount,
        default_currency: str,
        memo: str,
    ) -> List[models.Transaction]:
        """
        HACK based on assumption that all transactions in list represent a
        single transaction
        """
        # Not ready for prime time
        return []

        def group_key(tx):
            return ((tx.uniqueidtype, tx.uniqueid), tx.tferaction, tx.postype)

        def net_exercises(tx0, tx1):
            units0 = tx0.units
            if tx0.postype == "SHORT":
                units0 *= -1
                tx0.postype = "LONG"
            units1 = tx1.units
            if tx1.postype == "SHORT":
                units1 *= -1
            tx0.units += tx1.units
            return tx0

        def sort_key(tx):
            return tx.tferaction

        txs = (
            GroupedList(transactions)
            .groupby(group_key)
            .reduce(net_exercises)
            .flatten()
            .sorted(sort_key)
        )
        txs = txs.pop(None)
        assert len(txs) == 2
        dest, src = txs
        assert dest.tferaction == "IN"
        assert src.tferaction == "OUT"

        security = securities[(dest.uniqueidtype, dest.uniqueid)]
        fromsecurity = securities[(src.uniqueidtype, src.uniqueid)]

        # FIXME - exercise cash is sent as INVBANKTRAN; can't get it from
        # just the TRANSFERS which are dispatched to here.
        tx = ofx.reader.merge_transaction(
            session,
            type="exercise",
            fiaccount=account,
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
        return [tx]
