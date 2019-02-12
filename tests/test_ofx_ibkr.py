# coding: utf-8
"""
"""
# stdlib imports
import unittest
#  from unittest.mock import patch
import os
from datetime import datetime
from decimal import Decimal


# 3rd party imports
from sqlalchemy import create_engine
import ofxtools


# local imports
from capgains.ofx.ibkr import (
    OfxStatementReader,
)
from capgains.models.transactions import (
    Fi, FiAccount, Security, Transaction
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

class TradeTestCase(DatabaseTest, unittest.TestCase):
    def filterTradesTestCase(self):
        pass

    def filterTradeCancelsTestCase(self):
        pass

    def sortCanceledTradesTestCase(self):
        pass


class CashTransactionsTestCase(DatabaseTest, unittest.TestCase):
    def testDoCashTransactions(self):
        ofx = """
<INVTRANLIST> <DTSTART>20160331202000.000[-4:EDT]</DTSTART> <DTEND>20160429202000.000[-4:EDT]</DTEND> <INCOME> <INVTRAN> <FITID>20160413.U999999.e.USD.6352363694</FITID> <DTTRADE>20160413202000.000[-4:EDT]</DTTRADE> <MEMO>RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE (Return of Capital)</MEMO> </INVTRAN> <SECID> <UNIQUEID>ANN741081064</UNIQUEID> <UNIQUEIDTYPE>ISIN</UNIQUEIDTYPE> </SECID> <INCOMETYPE>DIV</INCOMETYPE> <TOTAL>138215</TOTAL> <SUBACCTSEC>CASH</SUBACCTSEC> <SUBACCTFUND>CASH</SUBACCTFUND> <CURRENCY> <CURRATE>1.0</CURRATE> <CURSYM>USD</CURSYM> </CURRENCY> </INCOME> <INVEXPENSE> <INVTRAN> <FITID>20160413.U999999.e.USD.6356130554</FITID> <DTTRADE>20160413202000.000[-4:EDT]</DTTRADE> <MEMO>RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE - REVERSAL (Return of Capital)</MEMO> </INVTRAN> <SECID> <UNIQUEID>ANN741081064</UNIQUEID> <UNIQUEIDTYPE>ISIN</UNIQUEIDTYPE> </SECID> <TOTAL>-138215</TOTAL> <SUBACCTSEC>CASH</SUBACCTSEC> <SUBACCTFUND>CASH</SUBACCTFUND> <CURRENCY> <CURRATE>1.0</CURRATE> <CURSYM>USD</CURSYM> </CURRENCY> </INVEXPENSE> <INCOME> <INVTRAN> <FITID>20160413.U999999.e.USD.6356130558</FITID> <DTTRADE>20160413202000.000[-4:EDT]</DTTRADE> <MEMO>RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE (Return of Capital)</MEMO> </INVTRAN> <SECID> <UNIQUEID>ANN741081064</UNIQUEID> <UNIQUEIDTYPE>ISIN</UNIQUEIDTYPE> </SECID> <INCOMETYPE>DIV</INCOMETYPE> <TOTAL>139000</TOTAL> <SUBACCTSEC>CASH</SUBACCTSEC> <SUBACCTFUND>CASH</SUBACCTFUND> <CURRENCY> <CURRATE>1.0</CURRATE> <CURSYM>USD</CURSYM> </CURRENCY> </INCOME> </INVTRANLIST>
        """

        #  element = ET.fromstring(xml)
        treebuilder = ofxtools.Parser.TreeBuilder()
        treebuilder.feed(ofx)
        invtranlist = ofxtools.models.base.Aggregate.from_etree(treebuilder.close())

        rhdgf = Security.merge(self.session, uniqueidtype='ISIN', uniqueid='ANN741081064')
        self.reader.securities[('ISIN', 'ANN741081064')] = rhdgf

        fi = Fi(brokerid='dch.com', name='Dewey Cheatham & Howe')
        self.reader.account = FiAccount(fi=fi, number='5678', name='Test')
        self.session.add(self.reader.account)

        self.reader.doCashTransactions(invtranlist)
        trans = self.reader.transactions

        # <INVEXPENSE> cancels first <INCOME>
        self.assertEqual(len(trans), 1)
        tran = trans.pop()
        self.assertIsInstance(tran, Transaction)
        self.assertEqual(tran.uniqueid, '20160413.U999999.e.USD.6356130558')
        # FIXME - timezone adjustment
        #  self.assertEqual(tran.datetime, datetime(2016, 4, 13, 20, 20))
        self.assertEqual(tran.type, 'returnofcapital')
        self.assertEqual(tran.memo, 'RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE (Return of Capital)')
        self.assertEqual(tran.currency, 'USD')
        self.assertEqual(tran.cash, Decimal('139000'))
        self.assertEqual(tran.fiaccount, self.reader.account)
        self.assertEqual(tran.security, rhdgf)
        self.assertEqual(tran.units, None)
        self.assertEqual(tran.securityPrice, None)
        self.assertEqual(tran.fiaccountFrom, None)
        self.assertEqual(tran.securityFrom, None)
        self.assertEqual(tran.unitsFrom, None)
        self.assertEqual(tran.securityFromPrice, None)
        self.assertEqual(tran.numerator, None)
        self.assertEqual(tran.denominator, None)
        self.assertEqual(tran.sort, None)


if __name__ == '__main__':
    unittest.main(verbosity=3)
