# coding: utf-8
"""
"""
# stdlib imports
import unittest
from unittest.mock import patch, sentinel
from datetime import datetime
from decimal import Decimal


# 3rd party imports
from sqlalchemy import create_engine
import ofxtools


# local imports
from capgains.config import CONFIG
from capgains import ofx, flex, models
from capgains.models import (
    Fi,
    FiAccount,
    Security,
    SecurityId,
    Transaction,
    TransactionType,
)
from common import setUpModule, tearDownModule, RollbackMixin, OfxSnippetMixin


class OfxReaderMixin(RollbackMixin):
    def setUp(self):
        super(OfxReaderMixin, self).setUp()
        self.reader = ofx.reader.OfxStatementReader(self.session)


class ReadTestCase(OfxReaderMixin, unittest.TestCase):
    def setUp(self):
        super(ReadTestCase, self).setUp()
        acct = ofxtools.models.INVACCTFROM(acctid="12345", brokerid="foo.bar")
        self.reader.statement = ofxtools.models.INVSTMTRS(
            dtasof="20170101", curdef="USD", invacctfrom=acct
        )

        secid = ofxtools.models.SECID(uniqueidtype="CUSIP", uniqueid="284CNT995")
        secinfo = ofxtools.models.SECINFO(
            secname="Yoyodyne", ticker="YOYO", secid=secid
        )
        stockinfo = ofxtools.models.STOCKINFO(secinfo=secinfo)
        self.reader.seclist = ofxtools.models.SECLIST(stockinfo)
        self.reader.securities = self.reader.read_securities(self.session)

    @patch.object(ofx.reader.OfxStatementReader, "read_transactions")
    @patch.object(ofx.reader.OfxStatementReader, "read_securities")
    @patch.object(ofx.reader.OfxStatementReader, "read_account")
    def testRead(
        self,
        mock_read_account_method,
        mock_read_securities_method,
        mock_read_transactions_method,
    ):
        """
        OfxStatementReader.read() calls read_account(), read_securities(),
        and (by default) read_transactions()
        """
        self.reader.read(self.session)
        for method in (
            mock_read_account_method,
            mock_read_securities_method,
            mock_read_transactions_method,
        ):
            method.assert_called_once()

    @patch.object(ofx.reader.OfxStatementReader, "read_transactions")
    @patch.object(ofx.reader.OfxStatementReader, "read_securities")
    @patch.object(ofx.reader.OfxStatementReader, "read_account")
    def testReadNoTransactions(
        self,
        mock_read_account_method,
        mock_read_securities_method,
        mock_read_transactions_method,
    ):
        """
        OfxStatementReader.read() doesn't call read_transactions() when called
        with doTransactions=False
        """
        self.reader.read(self.session, doTransactions=False)
        for method in (mock_read_account_method, mock_read_securities_method):
            method.assert_called_once()
        mock_read_transactions_method.assert_not_called()

    def testReadAccount(self):
        acct = self.reader.read_account(self.reader.statement, self.session)
        self.assertIsInstance(acct, FiAccount)
        self.assertEqual(acct.number, "12345")
        fi = acct.fi
        self.assertEqual(fi.brokerid, "foo.bar")

    def testReadSecurities(self):
        """
        OfxStatementReader.read_securities() reads SECLIST into securities dict
        """
        self.reader.read_securities(self.session)
        securities = self.reader.securities
        self.assertIsInstance(securities, dict)
        # A valid CUSIP in SECLIST also gets you an ISIN for free
        self.assertEqual(len(securities), 2)
        self.assertIn(("CUSIP", "284CNT995"), securities)
        self.assertIn(("ISIN", "US284CNT9952"), securities)
        sec1 = securities[("CUSIP", "284CNT995")]
        sec2 = securities[("ISIN", "US284CNT9952")]
        self.assertIs(sec1, sec2)
        self.assertEqual(sec1.name, "Yoyodyne")
        self.assertEqual(sec1.ticker, "YOYO")

    @patch.object(
        ofx.reader.OfxStatementReader,
        "TRANSACTION_DISPATCHER",
        wraps=ofx.reader.OfxStatementReader.TRANSACTION_DISPATCHER,
    )
    @patch.object(ofx.reader.OfxStatementReader, "doTrades")
    @patch.object(ofx.reader.OfxStatementReader, "doCashTransactions")
    @patch.object(ofx.reader.OfxStatementReader, "doTransfers")
    def testReadTransactions(
        self,
        mock_do_transfers,
        mock_do_cash_transactions,
        mock_do_trades,
        mock_transaction_handlers,
    ):
        pass


class TradesTestCase(OfxReaderMixin, unittest.TestCase):
    def setUp(self):
        super(TradesTestCase, self).setUp()

        self.fi = Fi(brokerid="dch.com", name="Dewey Cheatham & Howe")
        self.account = FiAccount(fi=self.fi, number="5678", name="Test")
        self.security = Security(name="Yoyodyne", ticker="YOYO")
        self.securityId = SecurityId(
            security=self.security, uniqueidtype="CUSIP", uniqueid="284CNT995"
        )
        self.session.add_all([self.fi, self.account, self.security, self.securityId])

        secid = ofxtools.models.SECID(uniqueidtype="CUSIP", uniqueid="284CNT995")
        secinfo = ofxtools.models.SECINFO(secname="Frobozz", ticker="FBZZ", secid=secid)
        stockinfo = ofxtools.models.STOCKINFO(secinfo=secinfo)
        self.reader.seclist = ofxtools.models.SECLIST(stockinfo)
        self.reader.account = self.account
        self.reader.securities = self.reader.read_securities(self.session)
        self.reader.currency_default = "USD"

        invtran = ofxtools.models.INVTRAN(
            fitid="deadbeef", dttrade="20170203", memo="Fill up"
        )
        invbuy = ofxtools.models.INVBUY(
            invtran=invtran,
            secid=secid,
            units=Decimal("100"),
            unitprice=Decimal("1.2"),
            commission=Decimal("9.99"),
            total=Decimal("129.99"),
            subacctsec="CASH",
            subacctfund="CASH",
        )
        self.buystock = ofxtools.models.BUYSTOCK(invbuy=invbuy, buytype="BUY")

    def testDoTrades(self):
        pass

    def _mergeTradeTest(self, memo=None):
        tx = ofx.reader.merge_trade(
            self.buystock,
            session=self.session,
            securities=self.reader.securities,
            account=self.reader.account,
            default_currency="USD",
            get_trade_sort_algo=ofx.reader.OfxStatementReader.get_trade_sort_algo,
            memo=memo,
        )
        self.assertEqual(tx.type, TransactionType.TRADE)
        self.assertIs(tx.fiaccount, self.account)
        self.assertEqual(tx.uniqueid, "deadbeef")
        self.assertEqual(tx.datetime, datetime(2017, 2, 3, tzinfo=ofxtools.utils.UTC))
        if memo:
            self.assertEqual(tx.memo, memo)
        else:
            self.assertEqual(tx.memo, self.buystock.invbuy.invtran.memo)
        self.assertIs(tx.security, self.security)
        self.assertEqual(tx.units, 100)
        self.assertEqual(tx.currency, models.Currency.USD)  # self.reader.currency_default
        self.assertEqual(tx.cash, Decimal("129.99"))
        self.assertIsNone(tx.sort)

    def testMergeTrade(self):
        self._mergeTradeTest()

    def testMergeTradeOverrideMemo(self):
        """
        OfxStatementReader.merge_trade() can override the transaction memo
        """
        self._mergeTradeTest(memo="Load the boat")


class CashTransactionsTestCase(OfxReaderMixin, unittest.TestCase):
    def testFingerprintCash(self):
        """
        keyCashTransaction() extracts (incometype, (uniqueidtype, uniqueid), memo)
        """
        tx = flex.Types.CashTransaction(
            fitid="5279100113",
            dttrade=datetime(2015, 4, 24),
            dtsettle=None,
            memo="Something",
            uniqueidtype="CUSIP",
            uniqueid="abc123",
            incometype="DIV",
            currency="USD",
            total=Decimal("593517"),
        )
        key = ofx.reader.OfxStatementReader.fingerprint_cash(tx)
        self.assertIsInstance(key, tuple)
        self.assertEqual(len(key), 3)
        dttrade, security, memo = key
        self.assertIsInstance(dttrade, datetime)
        self.assertEqual(dttrade, datetime(2015, 4, 24))
        self.assertIsInstance(security, tuple)
        self.assertEqual(len(security), 2)
        uniqueidtype, uniqueid = security
        self.assertIsInstance(uniqueidtype, str)
        self.assertEqual(uniqueidtype, "CUSIP")
        self.assertIsInstance(uniqueid, str)
        self.assertEqual(uniqueid, "abc123")
        self.assertIsInstance(memo, str)
        self.assertEqual(memo, "Something")

    def testNetCash(self):
        """
        net_cash() returns CashTransaction with amount summed,
        earliest dttrade, and other fields from the first transaction.
        """
        tx0 = flex.Types.CashTransaction(
            fitid="5279100113",
            dttrade=datetime(2015, 4, 24),
            dtsettle=None,
            memo="Something",
            uniqueidtype="CUSIP",
            uniqueid="abc123",
            incometype="DIV",
            currency="USD",
            total=Decimal("593517"),
        )
        tx1 = flex.Types.CashTransaction(
            fitid="5279100115",
            dttrade=datetime(2015, 4, 23),
            dtsettle=None,
            memo="Something else",
            uniqueidtype="ISIN",
            uniqueid="abc123",
            incometype="INTEREST",
            currency="CHF",
            total=Decimal("150"),
        )
        net = ofx.reader.net_cash(tx0, tx1)
        self.assertIsInstance(net, flex.Types.CashTransaction)
        # net_cash() chooses the first transactionID
        self.assertEqual(net.fitid, "5279100113")
        self.assertEqual(net.dttrade, datetime(2015, 4, 23))
        self.assertEqual(net.memo, "Something")
        self.assertEqual(net.uniqueidtype, "CUSIP")
        self.assertEqual(net.uniqueid, "abc123")
        self.assertEqual(net.incometype, "DIV")
        self.assertEqual(net.currency, "USD")
        self.assertEqual(net.total, Decimal("593667"))

    def _mergeRetOfCapTest(self, memo=None):
        fi = Fi(brokerid="dch.com")
        acct = FiAccount(fi=fi, number="8675309")
        security = Security(name="Yoyodyne", ticker="YOYO")
        self.session.add_all([fi, acct, security])
        self.reader.account = acct
        self.reader.securities = {("ISIN", "9999"): security}
        tx = flex.Types.CashTransaction(
            fitid="deadbeef",
            dttrade=datetime(2015, 4, 1),
            dtsettle=None,
            memo="Jackpot",
            uniqueidtype="ISIN",
            uniqueid="9999",
            incometype="INTEREST",
            currency="USD",
            total=Decimal("123.45"),
        )
        trans = ofx.reader.merge_retofcap(
            tx,
            self.session,
            self.reader.securities,
            self.reader.account,
            default_currency="USD",
            memo=memo
        )
        self.assertIsInstance(trans, Transaction)
        self.assertEqual(trans.uniqueid, tx.fitid)
        self.assertEqual(trans.datetime, tx.dttrade)
        if memo:
            self.assertEqual(trans.memo, memo)
        else:
            self.assertEqual(trans.memo, tx.memo)
        self.assertEqual(trans.currency, models.Currency[tx.currency])
        self.assertEqual(trans.cash, tx.total)
        self.assertEqual(trans.fiaccount, acct)
        self.assertEqual(trans.security, security)
        self.assertEqual(trans.units, None)
        self.assertEqual(trans.securityprice, None)
        self.assertEqual(trans.fromfiaccount, None)

    def testMergeRetOfCap(self):
        self._mergeRetOfCapTest()

    def testMergeRetOfCapOverrideMemo(self):
        """
        OfxStatementReader.merge_retofcap() can override the transaction memo
        """
        self._mergeRetOfCapTest(memo="Bingo")


class TransactionTestCase(OfxReaderMixin, unittest.TestCase):
    @patch.object(Transaction, "merge", return_value=sentinel.Transaction)
    def testMergeTransaction(self, mock_merge_method):
        output = ofx.reader.merge_transaction(self.session, uniqueid="test")
        mock_merge_method.assert_called_with(self.session, uniqueid="test")
        self.assertEqual(output, sentinel.Transaction)

    @patch.object(Transaction, "merge", return_value=sentinel.Transaction)
    @patch("capgains.ofx.reader.make_uid", return_value="mock_uid")
    def testMergeTransactionNoUniqueId(self, mock_make_uid_method, mock_merge_method):
        tx = {
            "uniqueid": None,
            "fiaccount": "test account",
            "datetime": "test datetime",
        }
        output = ofx.reader.merge_transaction(self.session, **tx)
        mock_make_uid_method.assert_called_with(**tx)
        tx.update({"uniqueid": "mock_uid"})
        mock_merge_method.assert_called_with(self.session, **tx)
        self.assertEqual(output, sentinel.Transaction)

    def testMakeUid(self):
        pass


if __name__ == "__main__":
    unittest.main(verbosity=3)
