# coding: utf-8
"""
"""
# stdlib imports
import unittest
from unittest.mock import patch
import os
from datetime import datetime
from decimal import Decimal


# 3rd party imports
from sqlalchemy import create_engine
import ofxtools


# local imports
from capgains.ofx.reader import (
    OfxStatementReader, CashTransaction,
)
from capgains.models.transactions import (
    Fi, FiAccount, Security, SecurityId, Transaction
)
from capgains.database import Session, Base


DB_URI = os.getenv('DB', 'sqlite://')


def setUpModule():
    """
    Called by unittest.TestRunner before any other tests in this module.
    """
    global engine
    engine = create_engine(DB_URI)


def tearDownModule():
    engine.dispose()


class DatabaseTest(object):
    """ Mixin providing DB setup/teardown methods """
    def setUp(self):
        self.connection = engine.connect()
        self.transaction = self.connection.begin()
        self.session = Session(bind=self.connection)
        Base.metadata.create_all(bind=self.connection)
        self.reader = OfxStatementReader(self.session)

    def tearDown(self):
        self.session.close()
        self.transaction.rollback()
        self.connection.close()


class ReadTestCase(DatabaseTest, unittest.TestCase):
    def setUp(self):
        super(ReadTestCase, self).setUp()
        acct = ofxtools.models.INVACCTFROM(acctid='12345', brokerid='foo.bar')
        self.reader.statement = ofxtools.models.INVSTMTRS(
            dtasof='20170101', curdef='USD', invacctfrom=acct)

        secid = ofxtools.models.SECID(uniqueidtype='CUSIP',
                                      uniqueid='284CNT995')
        secinfo = ofxtools.models.SECINFO(secname='Yoyodyne',
                                          ticker='YOYO',
                                          secid=secid)
        stockinfo = ofxtools.models.STOCKINFO(secinfo=secinfo)
        self.reader.seclist = ofxtools.models.SECLIST(stockinfo)

    @patch.object(OfxStatementReader, 'read_transactions')
    @patch.object(OfxStatementReader, 'read_securities')
    @patch.object(OfxStatementReader, 'read_account')
    def testRead(self, mock_read_account_method, mock_read_securities_method,
                 mock_read_transactions_method):
        """
        OfxStatementReader.read() calls read_account(), read_securities(),
        and (by default) read_transactions()
        """
        self.reader.read()
        for method in (mock_read_account_method, mock_read_securities_method,
                       mock_read_transactions_method):
            method.assert_called_once()

    @patch.object(OfxStatementReader, 'read_transactions')
    @patch.object(OfxStatementReader, 'read_securities')
    @patch.object(OfxStatementReader, 'read_account')
    def testReadNoTransactions(self, mock_read_account_method,
                               mock_read_securities_method,
                               mock_read_transactions_method):
        """
        OfxStatementReader.read() doesn't call read_transactions() when called
        with doTransactions=False
        """
        self.reader.read(doTransactions=False)
        for method in (mock_read_account_method, mock_read_securities_method):
            method.assert_called_once()
        mock_read_transactions_method.assert_not_called()

    def testReadAccount(self):
        self.reader.read_account()
        acct = self.reader.account
        self.assertIsInstance(acct, FiAccount)
        self.assertEqual(acct.number, '12345')
        fi = acct.fi
        self.assertEqual(fi.brokerid, 'foo.bar')

    def testReadSecurities(self):
        """
        OfxStatementReader.read_securities() reads SECLIST into securities dict
        """
        self.reader.read_securities()
        securities = self.reader.securities
        self.assertIsInstance(securities, dict)
        # A valid CUSIP in SECLIST also gets you an ISIN for free
        self.assertEqual(len(securities), 2)
        self.assertIn(('CUSIP', '284CNT995'), securities)
        self.assertIn(('ISIN', 'US284CNT9952'), securities)
        sec1 = securities[('CUSIP', '284CNT995')]
        sec2 = securities[('ISIN', 'US284CNT9952')]
        self.assertIs(sec1, sec2)
        self.assertEqual(sec1.name, 'Yoyodyne')
        self.assertEqual(sec1.ticker, 'YOYO')

    @patch.object(OfxStatementReader, 'transaction_handlers', wraps=OfxStatementReader.transaction_handlers)
    @patch.object(OfxStatementReader, 'doTrades')
    @patch.object(OfxStatementReader, 'doCashTransactions')
    @patch.object(OfxStatementReader, 'doTransfers')
    def testReadTransactions(self, mock_do_transfers,
                             mock_do_cash_transactions, mock_do_trades,
                             mock_transaction_handlers):
        pass


class TradesTestCase(DatabaseTest, unittest.TestCase):
    def setUp(self):
        super(TradesTestCase, self).setUp()

        self.fi = Fi(brokerid='dch.com', name='Dewey Cheatham & Howe')
        self.account = FiAccount(fi=self.fi, number='5678', name='Test')
        self.security = Security(name='Yoyodyne', ticker='YOYO')
        self.securityId = SecurityId(
            security=self.security, uniqueidtype='CUSIP', uniqueid='284CNT995')
        self.session.add_all([self.fi, self.account, self.security,
                              self.securityId])

        secid = ofxtools.models.SECID(
            uniqueidtype='CUSIP', uniqueid='284CNT995')
        secinfo = ofxtools.models.SECINFO(secname='Frobozz', ticker='FBZZ',
                                          secid=secid)
        stockinfo = ofxtools.models.STOCKINFO(secinfo=secinfo)
        self.reader.seclist = ofxtools.models.SECLIST(stockinfo)
        self.reader.account = self.account
        self.reader.read_securities()
        self.reader.currency_default = 'USD'

        invtran = ofxtools.models.INVTRAN(fitid='deadbeef', dttrade='20170203',
                                          memo='Fill up')
        invbuy = ofxtools.models.INVBUY(invtran=invtran, secid=secid,
                                        units=Decimal('100'),
                                        unitprice=Decimal('1.2'),
                                        commission=Decimal('9.99'),
                                        total=Decimal('129.99'),
                                        subacctsec='CASH', subacctfund='CASH')
        self.buystock = ofxtools.models.BUYSTOCK(invbuy=invbuy, buytype='BUY')

    def testDoTrades(self):
        pass

    def _mergeTradeTest(self, memo=None):
        self.reader.merge_trade(self.buystock, memo=memo)
        self.assertEqual(len(self.reader.transactions), 1)
        tx = self.reader.transactions.pop()
        self.assertEqual(tx.type, 'trade')
        self.assertIs(tx.fiaccount, self.account)
        self.assertEqual(tx.uniqueid, 'deadbeef')
        self.assertEqual(tx.datetime,
                         datetime(2017, 2, 3, tzinfo=ofxtools.utils.UTC))
        if memo:
            self.assertEqual(tx.memo, memo)
        else:
            self.assertEqual(tx.memo, self.buystock.invbuy.invtran.memo)
        self.assertIs(tx.security, self.security)
        self.assertEqual(tx.units, 100)
        self.assertEqual(tx.currency, 'USD')  # self.reader.currency_default
        self.assertEqual(tx.cash, Decimal('129.99'))
        self.assertIsNone(tx.sort)

    def testMergeTrade(self):
        self._mergeTradeTest()

    def testMergeTradeOverrideMemo(self):
        """
        OfxStatementReader.merge_trade() can override the transaction memo
        """
        self._mergeTradeTest(memo='Load the boat')


class CashTransactionsTestCase(DatabaseTest, unittest.TestCase):
    def testGroupCashTransactionsForCancel(self):
        """
        keyCashTransaction() extracts (incometype, (uniqueidtype, uniqueid), memo)
        """
        tx = CashTransaction(fitid='5279100113', dttrade=datetime(2015, 4, 24),
                             dtsettle=None, memo='Something',
                             uniqueidtype='CUSIP', uniqueid='abc123',
                             incometype='DIV', currency='USD',
                             total=Decimal('593517'))
        key = OfxStatementReader.groupCashTransactionsForCancel(tx)
        self.assertIsInstance(key, tuple)
        self.assertEqual(len(key), 3)
        dttrade, security, memo = key
        self.assertIsInstance(dttrade, datetime)
        self.assertEqual(dttrade, datetime(2015, 4, 24))
        self.assertIsInstance(security, tuple)
        self.assertEqual(len(security), 2)
        uniqueidtype, uniqueid = security
        self.assertIsInstance(uniqueidtype, str)
        self.assertEqual(uniqueidtype, 'CUSIP')
        self.assertIsInstance(uniqueid, str)
        self.assertEqual(uniqueid, 'abc123')
        self.assertIsInstance(memo, str)
        self.assertEqual(memo, 'Something')

    def testNetCashTransactions(self):
        """
        _netCashTransactions() returns CashTransaction with amount summed,
        earliest dttrade, and other fields from the first transaction.
        """
        tx0 = CashTransaction(fitid='5279100113',
                              dttrade=datetime(2015, 4, 24), dtsettle=None,
                              memo='Something',
                              uniqueidtype='CUSIP', uniqueid='abc123',
                              incometype='DIV', currency='USD',
                              total=Decimal('593517'))
        tx1 = CashTransaction(fitid='5279100115',
                              dttrade=datetime(2015, 4, 23), dtsettle=None,
                              memo='Something else', uniqueidtype='ISIN',
                              uniqueid='abc123', incometype='INTEREST',
                              currency='CHF', total=Decimal('150'))
        net = OfxStatementReader(None).netCashTransactions(tx0, tx1)
        self.assertIsInstance(net, CashTransaction)
        # netCashTransactions() chooses the first transactionID
        self.assertEqual(net.fitid, '5279100113')
        self.assertEqual(net.dttrade, datetime(2015, 4, 23))
        self.assertEqual(net.memo, 'Something')
        self.assertEqual(net.uniqueidtype, 'CUSIP')
        self.assertEqual(net.uniqueid, 'abc123')
        self.assertEqual(net.incometype, 'DIV')
        self.assertEqual(net.currency, 'USD')
        self.assertEqual(net.total, Decimal('593667'))

    def _mergeRetOfCapTest(self, memo=None):
        fi = Fi(brokerid='dch.com')
        acct = FiAccount(fi=fi, number='8675309')
        security = Security(name='Yoyodyne', ticker='YOYO')
        self.session.add_all([fi, acct, security])
        self.reader.account = acct
        self.reader.securities = {('ISIN', '9999'): security}
        tx = CashTransaction(
            fitid='deadbeef', dttrade=datetime(2015, 4, 1), dtsettle=None,
            memo='Jackpot', uniqueidtype='ISIN', uniqueid='9999',
            incometype='INTEREST', currency='USD', total=Decimal('123.45'))
        self.reader.merge_retofcap(tx, memo=memo)
        self.assertEqual(len(self.reader.transactions), 1)
        trans = self.reader.transactions[0]
        self.assertIsInstance(trans, Transaction)
        self.assertEqual(trans.uniqueid, tx.fitid)
        self.assertEqual(trans.datetime, tx.dttrade)
        if memo:
            self.assertEqual(trans.memo, memo)
        else:
            self.assertEqual(trans.memo, tx.memo)
        self.assertEqual(trans.currency, tx.currency)
        self.assertEqual(trans.cash, tx.total)
        self.assertEqual(trans.fiaccount, acct)
        self.assertEqual(trans.security, security)
        self.assertEqual(trans.units, None)
        self.assertEqual(trans.securityPrice, None)
        self.assertEqual(trans.fiaccountFrom, None)

    def testMergeRetOfCap(self):
        self._mergeRetOfCapTest()

    def testMergeRetOfCapOverrideMemo(self):
        """
        OfxStatementReader.merge_retofcap() can override the transaction memo
        """
        self._mergeRetOfCapTest(memo='Bingo')


class TransactionTestCase(DatabaseTest, unittest.TestCase):
    @patch.object(Transaction, 'merge', return_value='mock_transaction')
    def testMergeTransaction(self, mock_merge_method):
        output = self.reader.merge_transaction(uniqueid='test')
        mock_merge_method.assert_called_with(self.reader.session,
                                             uniqueid='test')
        self.assertEqual(output, 'mock_transaction')

    @patch.object(Transaction, 'merge', return_value='mock_transaction')
    @patch.object(OfxStatementReader, 'make_uid', return_value='mock_uid')
    def testMergeTransactionNoUniqueId(self, mock_make_uid_method,
                                       mock_merge_method):
        tx = {'uniqueid': None, 'fiaccount': 'test account',
              'datetime': 'test datetime'}
        output = self.reader.merge_transaction(**tx)
        mock_make_uid_method.assert_called_with(**tx)
        tx.update({'uniqueid': 'mock_uid'})
        mock_merge_method.assert_called_with(self.reader.session, **tx)
        self.assertEqual(output, 'mock_transaction')

    def testMakeUid(self):
        pass


if __name__ == '__main__':
    unittest.main(verbosity=3)
