# coding: utf-8
"""
"""
# stdlib imports
import unittest
#  from unittest.mock import patch
from datetime import datetime
from decimal import Decimal


# local imports
from capgains.config import CONFIG
from capgains.ofx.ibkr import (
    OfxStatementReader,
)
from capgains.models.transactions import (
    Fi, FiAccount, Security, Transaction
)
from common import (
    setUpModule,
    tearDownModule,
    RollbackMixin,
    OfxSnippetMixin,
)


class TradeTestCase(RollbackMixin, unittest.TestCase):
    def filterTradesTestCase(self):
        pass

    def filterTradeCancelsTestCase(self):
        pass

    def sortCanceledTradesTestCase(self):
        pass


class CashTransactionsTestCase(OfxSnippetMixin, unittest.TestCase):
    readerclass = OfxStatementReader
    ofx = """
<INVTRANLIST> <DTSTART>20160331202000.000[-4:EDT]</DTSTART> <DTEND>20160429202000.000[-4:EDT]</DTEND> <INCOME> <INVTRAN> <FITID>20160413.U999999.e.USD.6352363694</FITID> <DTTRADE>20160413202000.000[-4:EDT]</DTTRADE> <MEMO>RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE (Return of Capital)</MEMO> </INVTRAN> <SECID> <UNIQUEID>ANN741081064</UNIQUEID> <UNIQUEIDTYPE>ISIN</UNIQUEIDTYPE> </SECID> <INCOMETYPE>DIV</INCOMETYPE> <TOTAL>138215</TOTAL> <SUBACCTSEC>CASH</SUBACCTSEC> <SUBACCTFUND>CASH</SUBACCTFUND> <CURRENCY> <CURRATE>1.0</CURRATE> <CURSYM>USD</CURSYM> </CURRENCY> </INCOME> <INVEXPENSE> <INVTRAN> <FITID>20160413.U999999.e.USD.6356130554</FITID> <DTTRADE>20160413202000.000[-4:EDT]</DTTRADE> <MEMO>RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE - REVERSAL (Return of Capital)</MEMO> </INVTRAN> <SECID> <UNIQUEID>ANN741081064</UNIQUEID> <UNIQUEIDTYPE>ISIN</UNIQUEIDTYPE> </SECID> <TOTAL>-138215</TOTAL> <SUBACCTSEC>CASH</SUBACCTSEC> <SUBACCTFUND>CASH</SUBACCTFUND> <CURRENCY> <CURRATE>1.0</CURRATE> <CURSYM>USD</CURSYM> </CURRENCY> </INVEXPENSE> <INCOME> <INVTRAN> <FITID>20160413.U999999.e.USD.6356130558</FITID> <DTTRADE>20160413202000.000[-4:EDT]</DTTRADE> <MEMO>RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE (Return of Capital)</MEMO> </INVTRAN> <SECID> <UNIQUEID>ANN741081064</UNIQUEID> <UNIQUEIDTYPE>ISIN</UNIQUEIDTYPE> </SECID> <INCOMETYPE>DIV</INCOMETYPE> <TOTAL>139000</TOTAL> <SUBACCTSEC>CASH</SUBACCTSEC> <SUBACCTFUND>CASH</SUBACCTFUND> <CURRENCY> <CURRATE>1.0</CURRATE> <CURSYM>USD</CURSYM> </CURRENCY> </INCOME> </INVTRANLIST>
    """

    def testDoCashTransactions(self):
        self.reader.doCashTransactions(self.parsed_txs)
        trans = self.reader.transactions

        # <INVEXPENSE> cancels first <INCOME>
        self.assertEqual(len(trans), 1)
        tran = trans.pop()
        self.assertEqual(len(self.securities), 1)
        rhdgf = self.securities[0]
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
