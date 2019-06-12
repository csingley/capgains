# coding: utf-8
"""
"""
# stdlib imports
import unittest
from unittest.mock import patch, call, sentinel
from datetime import datetime, date
from decimal import Decimal
import os


# local imports
from capgains.flex import reader
from capgains.flex.reader import FlexStatementReader, ParsedCorpAct

from capgains.config import CONFIG
from capgains.flex import parser
from capgains.flex.parser import CorporateAction
from capgains import models
from capgains.ofx.reader import OfxStatementReader
from capgains.inventory import ReturnOfCapital

from common import setUpModule, tearDownModule, RollbackMixin, XmlSnippetMixin


DB_URI = CONFIG.db_uri


class FlexStatementReaderMixin(RollbackMixin):
    @classmethod
    def setUpClass(cls):
        super(FlexStatementReaderMixin, cls).setUpClass()

        cls.reader = FlexStatementReader(cls.session)
        cls.securities = {}
        cls.reader.securities = cls.securities


class CashTransactionXmlSnippetMixin(XmlSnippetMixin):
    txs_entry_point = "doCashTransactions"


class ReadTestCase(unittest.TestCase):
    def setUp(self):
        self.reader = FlexStatementReader(None)

    @patch.object(OfxStatementReader, "read")
    @patch.object(FlexStatementReader, "read_dividends")
    @patch.object(FlexStatementReader, "read_currency_rates")
    def testRead(
        self,
        mock_flex_read_currency_rates_method,
        mock_flex_read_dividends_method,
        mock_ofx_read_method,
    ):
        self.reader.read()
        mock_flex_read_currency_rates_method.assert_called_once()
        mock_flex_read_dividends_method.assert_called_once()
        mock_ofx_read_method.assert_called_with(True)

    def testReadDividends(self):
        div0 = parser.Dividend(
            conid=sentinel.conid0,
            exDate=None,
            payDate=sentinel.payDate0,
            quantity=None,
            grossRate=None,
            taxesAndFees=None,
            total=None,
        )
        div1 = parser.Dividend(
            conid=sentinel.conid1,
            exDate=None,
            payDate=sentinel.payDate1,
            quantity=None,
            grossRate=None,
            taxesAndFees=None,
            total=None,
        )
        self.reader.statement = parser.FlexStatement(
            account=None,
            securities=None,
            dividends=[div0, div1],
            transactions=None,
            conversionrates=None,
        )
        self.reader.read_dividends()

        divs = self.reader.dividends
        self.assertIsInstance(divs, dict)
        self.assertEqual(len(divs), 2)
        self.assertEqual(divs[(sentinel.conid0, sentinel.payDate0)], div0)
        self.assertEqual(divs[(sentinel.conid1, sentinel.payDate1)], div1)

    def testReadCurrencyRates(self):
        pass

    @patch.object(models.Security, "merge", wraps=lambda session, **sec: sec)
    def testReadSecurities(self, mock_security_merge_method):
        sec0 = parser.Security(
            uniqueidtype=sentinel.cusip,
            uniqueid=sentinel.uniqueid0,
            secname=sentinel.secname0,
            ticker=sentinel.ticker0,
        )
        sec1 = parser.Security(
            uniqueidtype=sentinel.isin,
            uniqueid=sentinel.uniqueid1,
            secname=sentinel.secname1,
            ticker=sentinel.ticker1,
        )
        self.reader.statement = parser.FlexStatement(
            account=None,
            securities=[sec0, sec1],
            dividends=None,
            transactions=None,
            conversionrates=None,
        )
        self.reader.read_securities()
        self.assertEqual(
            mock_security_merge_method.mock_calls,
            [
                call(
                    None,
                    uniqueidtype=sec0.uniqueidtype,
                    uniqueid=sec0.uniqueid,
                    name=sec0.secname,
                    ticker=sec0.ticker,
                ),
                call(
                    None,
                    uniqueidtype=sec1.uniqueidtype,
                    uniqueid=sec1.uniqueid,
                    name=sec1.secname,
                    ticker=sec1.ticker,
                ),
            ],
        )

    def testTransactionHandlers(self):
        handlers = self.reader.transaction_handlers
        self.assertEqual(len(handlers), 5)
        self.assertEqual(handlers["Trade"], "doTrades")
        self.assertEqual(handlers["CashTransaction"], "doCashTransactions")
        self.assertEqual(handlers["Transfer"], "doTransfers")
        self.assertEqual(handlers["CorporateAction"], "doCorporateActions")
        self.assertEqual(handlers["Exercise"], "doOptionsExercises")


class TradesTestCase(FlexStatementReaderMixin, unittest.TestCase):
    def testFilterTrades(self):
        t0 = parser.Trade(
            memo="Something",
            fitid=None,
            dttrade=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            currency=None,
            total=None,
            reportdate=None,
            notes=None,
            orig_tradeid=None,
        )
        self.assertEqual(self.reader.filterTrades(t0), True)

        t1 = parser.Trade(
            memo="USD.CAD",
            fitid=None,
            dttrade=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            currency=None,
            total=None,
            reportdate=None,
            notes=None,
            orig_tradeid=None,
        )
        self.assertEqual(self.reader.filterTrades(t1), False)

        t2 = parser.Trade(
            memo="CAD.USD",
            fitid=None,
            dttrade=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            currency=None,
            total=None,
            reportdate=None,
            notes=None,
            orig_tradeid=None,
        )
        self.assertEqual(self.reader.filterTrades(t2), False)

        t3 = parser.Trade(
            memo="USD.EUR",
            fitid=None,
            dttrade=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            currency=None,
            total=None,
            reportdate=None,
            notes=None,
            orig_tradeid=None,
        )
        self.assertEqual(self.reader.filterTrades(t3), False)

        t4 = parser.Trade(
            memo="EUR.USD",
            fitid=None,
            dttrade=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            currency=None,
            total=None,
            reportdate=None,
            notes=None,
            orig_tradeid=None,
        )
        self.assertEqual(self.reader.filterTrades(t4), False)

    def testFilterTradeCancels(self):
        t0 = parser.Trade(
            notes=["Ca", "P"],
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            currency=None,
            total=None,
            reportdate=None,
            orig_tradeid=None,
        )
        self.assertEqual(self.reader.filterTradeCancels(t0), True)

        t1 = parser.Trade(
            notes=["O", "C"],
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            currency=None,
            total=None,
            reportdate=None,
            orig_tradeid=None,
        )
        self.assertEqual(self.reader.filterTradeCancels(t1), False)

    def testSortCanceledTrades(self):
        t0 = parser.Trade(
            reportdate="something",
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            currency=None,
            total=None,
            notes=None,
            orig_tradeid=None,
        )
        self.assertEqual(self.reader.sortCanceledTrades(t0), "something")

    def testSortForTrade(self):
        t0 = parser.Trade(
            notes=["ML", "C"],
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            currency=None,
            total=None,
            reportdate=None,
            orig_tradeid=None,
        )
        self.assertEqual(self.reader.sortForTrade(t0), "MINGAIN")

        t1 = parser.Trade(
            notes=["LI", "C"],
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            currency=None,
            total=None,
            reportdate=None,
            orig_tradeid=None,
        )
        self.assertEqual(self.reader.sortForTrade(t1), "LIFO")

        t2 = parser.Trade(
            notes=["Ca", "O"],
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            currency=None,
            total=None,
            reportdate=None,
            orig_tradeid=None,
        )
        self.assertEqual(self.reader.sortForTrade(t2), None)

        t3 = parser.Trade(
            notes=["ML", "LI"],
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            currency=None,
            total=None,
            reportdate=None,
            orig_tradeid=None,
        )
        with self.assertRaises(AssertionError):
            self.reader.sortForTrade(t3)


class CashTransactionsTestCase(FlexStatementReaderMixin, unittest.TestCase):
    def testFilterCashTransactions(self):
        t0 = parser.CashTransaction(
            incometype="Dividends",
            memo="foo ReTuRn Of CAPitAL bar",
            fitid=None,
            dttrade=None,
            dtsettle=None,
            uniqueidtype=None,
            uniqueid=None,
            currency=None,
            total=None,
        )
        self.assertEqual(self.reader.filterCashTransactions(t0), True)

        t1 = parser.CashTransaction(
            incometype="Dividends",
            memo="foo InTeRiMlIqUiDaTiOn bar",
            fitid=None,
            dttrade=None,
            dtsettle=None,
            uniqueidtype=None,
            uniqueid=None,
            currency=None,
            total=None,
        )
        self.assertEqual(self.reader.filterCashTransactions(t1), True)

        t2 = parser.CashTransaction(
            incometype="Dividends",
            memo="foo bar",
            fitid=None,
            dttrade=None,
            dtsettle=None,
            uniqueidtype=None,
            uniqueid=None,
            currency=None,
            total=None,
        )
        self.assertEqual(self.reader.filterCashTransactions(t2), False)

        t3 = parser.CashTransaction(
            incometype="Income",
            memo="foo interimliquidation bar",
            fitid=None,
            dttrade=None,
            dtsettle=None,
            uniqueidtype=None,
            uniqueid=None,
            currency=None,
            total=None,
        )
        self.assertEqual(self.reader.filterCashTransactions(t3), False)

    @patch.object(FlexStatementReader, "stripCashTransactionMemo", wraps=lambda m: m)
    def testGroupCashTransactionsForCancel(
        self, mock_strip_cash_memo_method, wraps=lambda memo: memo
    ):
        """
        FlexStatementReader.groupCashTransactionsForCancel() returns
        (tx.dtsettle, (tx.uniqueidtype, tx.uniqueid), tx.memo)
        """
        tx = parser.CashTransaction(
            memo=sentinel.memo,
            fitid=None,
            dttrade=None,
            dtsettle=sentinel.dtsettle,
            uniqueidtype=sentinel.uniqueidtype,
            uniqueid=sentinel.uniqueid,
            currency=None,
            total=None,
            incometype=None,
        )
        output = self.reader.groupCashTransactionsForCancel(tx)
        self.assertEqual(
            output,
            (
                sentinel.dtsettle,
                (sentinel.uniqueidtype, sentinel.uniqueid),
                sentinel.memo,
            ),
        )

    def testStripCashTransactionsMemo(self):
        memo = "foo - REVERSAL bar"
        self.assertEqual(self.reader.stripCashTransactionMemo(memo), "foo bar")

        memo = "foo CANCEL bar"
        self.assertEqual(self.reader.stripCashTransactionMemo(memo), "foo bar")

    def testFilterCashTransactionCancels(self):
        tx = parser.CashTransaction(
            memo="foobar",
            fitid=None,
            dttrade=None,
            dtsettle=None,
            uniqueidtype=None,
            uniqueid=None,
            currency=None,
            total=None,
            incometype=None,
        )
        output = self.reader.filterCashTransactionCancels(tx)
        self.assertEqual(output, False)

        tx = parser.CashTransaction(
            memo="fooREVERSALbar",
            fitid=None,
            dttrade=None,
            dtsettle=None,
            uniqueidtype=None,
            uniqueid=None,
            currency=None,
            total=None,
            incometype=None,
        )
        output = self.reader.filterCashTransactionCancels(tx)
        self.assertEqual(output, True)

        tx = parser.CashTransaction(
            memo="fooCANCELbar",
            fitid=None,
            dttrade=None,
            dtsettle=None,
            uniqueidtype=None,
            uniqueid=None,
            currency=None,
            total=None,
            incometype=None,
        )
        output = self.reader.filterCashTransactionCancels(tx)
        self.assertEqual(output, True)

    def testSortCanceledCashTransactions(self):
        """
        FlexStatementReader.sortCanceledCashTransactions() returns tx.fitid
        """
        tx = parser.CashTransaction(
            memo=None,
            fitid=sentinel.fitid,
            dttrade=None,
            dtsettle=None,
            uniqueidtype=None,
            uniqueid=None,
            currency=None,
            total=None,
            incometype=None,
        )
        output = self.reader.sortCanceledCashTransactions(tx)
        self.assertEqual(output, tx.fitid)

    def testFixCashTransactions(self):
        # N.B. flex.reader.FlexStatementReader.dividends is keyed by type
        # datetime.date, but flex.parser.CashTransaction.dtsettle is type
        # datetime.datetime
        div = parser.Dividend(
            conid=sentinel.conid,
            exDate=sentinel.exDate,
            payDate=sentinel.payDate,
            quantity=None,
            grossRate=None,
            taxesAndFees=None,
            total=None,
        )
        self.reader.dividends[(sentinel.conid, date(2012, 5, 3))] = div

        tx = parser.CashTransaction(
            memo=None,
            fitid=None,
            dttrade=None,
            dtsettle=datetime(2012, 5, 3),
            uniqueidtype=None,
            uniqueid=sentinel.conid,
            currency=None,
            total=None,
            incometype=None,
        )
        output = self.reader.fixCashTransactions(tx)
        self.assertIsInstance(output, parser.CashTransaction)
        self.assertEqual(output.memo, tx.memo)
        self.assertEqual(output.fitid, tx.fitid)
        self.assertEqual(output.dttrade, sentinel.exDate)
        self.assertEqual(output.dtsettle, tx.dtsettle)
        self.assertEqual(output.uniqueidtype, tx.uniqueidtype)
        self.assertEqual(output.uniqueid, tx.uniqueid)
        self.assertEqual(output.currency, tx.currency)
        self.assertEqual(output.total, tx.total)
        self.assertEqual(output.incometype, tx.incometype)


class CashTransactionWithFilterCancelTestCase(
    CashTransactionXmlSnippetMixin, unittest.TestCase
):
    xml = """
    <CashTransactions>
    <CashTransaction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="RHDGF"
    description="RHDGF(ANN741081064) CASH DIVIDEND 1.00000000 USD PER SHARE (Ordinary Dividend)" conid="24" securityID="ANN741081064" securityIDType="ISIN" cusip="" isin="ANN741081064" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" dateTime="2016-04-12" amount="27800" type="Dividends" tradeID="" code="" transactionID="6349123456" reportDate="2016-04-14" clientReference="" />
    <CashTransaction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="RHDGF" description="RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE (Return of Capital)" conid="24" securityID="ANN741081064" securityIDType="ISIN" cusip="" isin="ANN741081064" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" dateTime="2016-04-13" amount="138215" type="Dividends" tradeID="" code="" transactionID="6352363694" reportDate="2016-04-14" clientReference="" />
    <CashTransaction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="RHDGF" description="RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE - REVERSAL (Return of Capital)" conid="24" securityID="ANN741081064" securityIDType="ISIN" cusip="" isin="ANN741081064" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" dateTime="2016-04-13" amount="-138215" type="Dividends" tradeID="" code="" transactionID="6356130554" reportDate="2016-04-15" clientReference="" />
    <CashTransaction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="RHDGF" description="RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE (Return of Capital)" conid="24" securityID="ANN741081064" securityIDType="ISIN" cusip="" isin="ANN741081064" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" dateTime="2016-04-13" amount="139000" type="Dividends" tradeID="" code="" transactionID="6356130558" reportDate="2016-04-15" clientReference="" />
    </CashTransactions>
    """

    @property
    def persisted_txs(self):
        # First CashTransaction is not return of capital; filtered out.
        # 3rd CashTransaction cancels 2nd CashTransaction, leaving the last
        # CashTransaction to be persisted.
        return [
            ReturnOfCapital(
                id=None,
                uniqueid=None,
                datetime=datetime(2016, 4, 13),
                dtsettle=datetime(2016, 4, 13),
                memo="RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE (Return of Capital)",
                currency="USD",
                cash=Decimal("139000"),
                fiaccount=self.account,
                security=self.securities[0],
            )
        ]


class TransfersTestCase(FlexStatementReaderMixin, unittest.TestCase):
    @patch.object(FlexStatementReader, "merge_account_transfer")
    def testDoTransfers(self, mock_merge_acct_transfer_method):
        tx0 = parser.Transfer(
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=sentinel.conid0,
            units=None,
            tferaction=None,
            type=None,
            other_acctid=None,
        )
        tx1 = parser.Transfer(
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=sentinel.condid1,
            units=None,
            tferaction=None,
            type=None,
            other_acctid=None,
        )
        tx2 = parser.Transfer(
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            tferaction=None,
            type=None,
            other_acctid=None,
        )
        self.reader.doTransfers([tx0, tx1, tx2])

        self.assertEqual(
            mock_merge_acct_transfer_method.mock_calls, [call(tx0), call(tx1)]
        )


class CorporateActionsTestCase(FlexStatementReaderMixin, unittest.TestCase):
    def setUp(self):
        super(self.__class__, self).setUp()
        path = os.path.join(
            os.path.dirname(__file__), "data", "corpact_descriptions.txt"
        )
        with open(path) as file_:
            self.corpActMemos = file_.readlines()
        self.cmptypes = [
            "SD",
            "TO",
            "TO",
            "TC",
            "TC",
            "TO",
            "TO",
            "IC",
            "IC",
            "TC",
            "SO",
            "TC",
            "TC",
            "TC",
            "TC",
            "IC",
            "IC",
            "TC",
            "TC",
            "IC",
            "IC",
            "TO",
            "TO",
            "TC",
            "TO",
            "TO",
            "TC",
            "TO",
            "TO",
            "TC",
            "TC",
            "TO",
            "TC",
            "TO",
            "TC",
            "TC",
            "TC",
            "TC",
            "TC",
            "TO",
            "TO",
            "TO",
            "TO",
            "TC",
            "TC",
            "FS",
            "FS",
            "DW",
            "TO",
            "TO",
            "TC",
            "DW",
            "SO",
            "TC",
            "TC",
            "SO",
            "TC",
            "TC",
            "TC",
            "TC",
            "RI",
            "TC",
            "TC",
            "TC",
            "SO",
            "DW",
            "TC",
            "DW",
            "FS",
            "FS",
            "DW",
            "TC",
            "TC",
            "SO",
            "SO",
            "TO",
            "TO",
            "TC",
            "TC",
            "SD",
            "TO",
            "TO",
            "TO",
            "TO",
            "TC",
            "SO",
            "TO",
            "TO",
            "SR",
            "RI",
            "SR",
            "TC",
            "TC",
            "SO",
            "TC",
            "TC",
            "DW",
            "SD",
            "SD",
            "TC",
            "TC",
            "SR",
            "OR",
            "SR",
            "TC",
            "TC",
            "OR",
            "TC",
            "TC",
            "SD",
            "TO",
            "TO",
            "TC",
            "TC",
            "IC",
            "IC",
            "FS",
            "SO",
            "SO",
            "SO",
            "SO",
            "TC",
            "TO",
            "TO",
            "TC",
            "TO",
            "TC",
            "TC",
            "TC",
            "TC",
            "FS",
            "FS",
            "SD",
            "TC",
            "RI",
            "SO",
            "SO",
            "SR",
            "OR",
            "SR",
            "RI",
            "RI",
            "TO",
            "TO",
            "TC",
            "TC",
            "TC",
            "TC",
            "DW",
            "IC",
            "IC",
            "TO",
            "TO",
            "TC",
            "RI",
            "RI",
            "SR",
            "OR",
            "SR",
            "SR",
            "OR",
            "SR",
            "TC",
            "TC",
            "TC",
            "OR",
            "TC",
            "TC",
            "TC",
            "TC",
            "TC",
            "SO",
            "SO",
            "SO",
            "TC",
            "TC",
            "SO",
            "IC",
            "IC",
            "SO",
            "SO",
            "TO",
            "TO",
            "TC",
            "TC",
            "TC",
            "TC",
            "TC",
            "TC",
            "TC",
            "TC",
            "TC",
            "SO",
            "SO",
            "TC",
            "TC",
            "TC",
            "TO",
            "IC",
            "IC",
            "TC",
            "TO",
            "RI",
            "SR",
            "OR",
            "SR",
            "TC",
            "SD",
            "TC",
            "TC",
            "TC",
            "TC",
            "TO",
            "TO",
            "TC",
            "TC",
            "TC",
            "TO",
            "TO",
            "TO",
            "TO",
            "TO",
            "TO",
            "FS",
            "FS",
            "TO",
            "TO",
            "TO",
            "TO",
            "TO",
            "TO",
            "FS",
            "FS",
            "BM",
            "TO",
            "TO",
            "FS",
            "FS",
            "BM",
            "TO",
            "TO",
            "TC",
            "TO",
            "RI",
            "TO",
            "SR",
            "SR",
            "TC",
            "TC",
            "TC",
        ]
        self.assertEqual(len(self.corpActMemos), len(self.cmptypes))

    def testGroupCorporateAcionsForCancel(self):
        corpAct = parser.CorporateAction(
            fitid=None,
            dttrade=sentinel.dttrade,
            memo=sentinel.memo,
            uniqueidtype=sentinel.uniqueidtype,
            uniqueid=sentinel.uniqueid,
            units=None,
            currency=None,
            total=None,
            type=sentinel.type,
            reportdate=None,
            code=None,
        )
        result = self.reader.groupCorporateActionsForCancel(corpAct)
        self.assertEqual(
            result,
            (
                (corpAct.uniqueidtype, corpAct.uniqueid),
                corpAct.dttrade,
                corpAct.type,
                corpAct.memo,
            ),
        )

    def testFilterCorporateActionCancels(self):
        corpAct0 = parser.CorporateAction(
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            currency=None,
            total=None,
            type=None,
            reportdate=None,
            code=["Ca", "P"],
        )
        self.assertIs(self.reader.filterCorporateActionCancels(corpAct0), True)

        corpAct0 = parser.CorporateAction(
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            currency=None,
            total=None,
            type=None,
            reportdate=None,
            code=["O", "P"],
        )
        self.assertIs(self.reader.filterCorporateActionCancels(corpAct0), False)

    def testMatchCorporateActionWithCancel(self):
        corpAct0 = parser.CorporateAction(
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=None,
            units=5,
            currency=None,
            total=None,
            type=None,
            reportdate=None,
            code=None,
        )
        corpAct1 = parser.CorporateAction(
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=None,
            units=5,
            currency=None,
            total=None,
            type=None,
            reportdate=None,
            code=None,
        )
        corpAct2 = parser.CorporateAction(
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=None,
            units=-5,
            currency=None,
            total=None,
            type=None,
            reportdate=None,
            code=None,
        )

        self.assertIs(
            self.reader.matchCorporateActionWithCancel(corpAct0, corpAct1), False
        )
        self.assertIs(
            self.reader.matchCorporateActionWithCancel(corpAct0, corpAct2), True
        )

    def testSortCanceledCorporateActions(self):
        corpAct = parser.CorporateAction(
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            currency=None,
            total=None,
            type=None,
            reportdate=sentinel.reportdate,
            code=None,
        )

        self.assertIs(
            self.reader.sortCanceledCorporateActions(corpAct), sentinel.reportdate
        )

    def testNetCorporateActions(self):
        corpAct0 = parser.CorporateAction(
            fitid=sentinel.fitid0,
            dttrade=sentinel.dttrade0,
            memo=sentinel.memo0,
            uniqueidtype=sentinel.uniqueidtype0,
            uniqueid=sentinel.uniqueid0,
            units=2,
            currency=sentinel.currency0,
            total=20,
            type=sentinel.type0,
            reportdate=sentinel.reportdate0,
            code=sentinel.code0,
        )
        corpAct1 = parser.CorporateAction(
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=None,
            units=3,
            currency=sentinel.currency0,
            total=30,
            type=None,
            reportdate=None,
            code=None,
        )

        result = self.reader.netCorporateActions(corpAct0, corpAct1)
        self.assertIsInstance(result, parser.CorporateAction)
        self.assertIs(result.fitid, sentinel.fitid0)
        self.assertIs(result.dttrade, sentinel.dttrade0)
        self.assertIs(result.memo, sentinel.memo0)
        self.assertIs(result.uniqueidtype, sentinel.uniqueidtype0)
        self.assertIs(result.uniqueid, sentinel.uniqueid0)
        self.assertEqual(result.units, 5)
        self.assertIs(result.currency, sentinel.currency0)
        self.assertEqual(result.total, 50)
        self.assertIs(result.type, sentinel.type0)
        self.assertIs(result.reportdate, sentinel.reportdate0)
        self.assertIs(result.code, sentinel.code0)

    def testParseCorporateActionMemo(self):
        for i, memo in enumerate(self.corpActMemos):
            demarc = memo.rfind("(")
            mem = memo[:demarc].strip()
            payload = memo[demarc:].strip().strip("()").split(", ")
            cusip = payload.pop()
            ticker = payload.pop(0)
            # If ticker has timestamp prepended, strip it
            if ticker.startswith("20"):
                ticker = ticker[14:]
            # Sometimes secname contains commas, so gets split
            # If so, stitch it back together
            secname = ", ".join(payload)

            typ = self.cmptypes[i]
            ca = CorporateAction(
                None, None, memo, None, None, None, None, None, typ, None, None
            )
            pca = self.reader.parseCorporateActionMemo(ca)
            self.assertIsInstance(pca, ParsedCorpAct)
            self.assertIs(pca.raw, ca)
            self.assertEqual(pca.type, typ)
            self.assertEqual(pca.ticker, ticker)
            self.assertEqual(pca.cusip, cusip)
            self.assertEqual(pca.secname, secname)
            self.assertEqual(pca.memo, mem)

    def testInferCorporateActionType(self):
        for i, memo in enumerate(self.corpActMemos):
            inferredType = self.reader.inferCorporateActionType(memo)
            self.assertEqual(inferredType, self.cmptypes[i])

    def testGroupParsedCorporateActions(self):
        corpAct = parser.CorporateAction(
            fitid=None,
            dttrade=sentinel.dttrade,
            memo=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            currency=None,
            total=None,
            type=None,
            reportdate=sentinel.reportdate,
            code=None,
        )
        pca = reader.ParsedCorpAct(
            raw=corpAct,
            type=sentinel.type,
            ticker=None,
            cusip=None,
            secname=None,
            memo=sentinel.memo,
        )
        self.assertEqual(
            self.reader.groupParsedCorporateActions(pca),
            (sentinel.dttrade, sentinel.type, sentinel.memo),
        )

    def testSortParsedCorporateActions(self):
        corpAct = parser.CorporateAction(
            fitid=None,
            dttrade=None,
            memo=None,
            uniqueidtype=None,
            uniqueid=None,
            units=None,
            currency=None,
            total=None,
            type=None,
            reportdate=sentinel.reportdate,
            code=None,
        )
        pca = reader.ParsedCorpAct(
            raw=corpAct, type=None, ticker=None, cusip=None, secname=None, memo=None
        )
        self.assertEqual(
            self.reader.sortParsedCorporateActions(pca), sentinel.reportdate
        )

    def testMergeReorg(self):
        pass

    @patch.object(
        FlexStatementReader, "merge_transaction", return_value=sentinel.transaction
    )
    def testMergeSecurityTransfer(self, mock_merge_transaction_method):
        self.securities[
            (sentinel.uniqueidtype0, sentinel.uniqueid0)
        ] = sentinel.security0
        self.securities[
            (sentinel.uniqueidtype1, sentinel.uniqueid1)
        ] = sentinel.security1
        src = parser.CorporateAction(
            fitid=None,
            dttrade=None,
            memo=sentinel.memo0,
            uniqueidtype=sentinel.uniqueidtype0,
            uniqueid=sentinel.uniqueid0,
            units=sentinel.units0,
            currency=None,
            total=None,
            type=None,
            reportdate=None,
            code=None,
        )
        dest = parser.CorporateAction(
            fitid=sentinel.fitid1,
            dttrade=sentinel.dttrade1,
            memo=sentinel.memo1,
            uniqueidtype=sentinel.uniqueidtype1,
            uniqueid=sentinel.uniqueid1,
            units=sentinel.units1,
            currency=None,
            total=None,
            type=None,
            reportdate=None,
            code=None,
        )
        output = self.reader.merge_security_transfer(
            src=src, dest=dest, memo=sentinel.memo
        )

        self.assertEqual(
            mock_merge_transaction_method.mock_calls,
            [
                call(
                    type=models.TransactionType.TRANSFER,
                    fiaccount=self.reader.account,
                    uniqueid=sentinel.fitid1,
                    datetime=sentinel.dttrade1,
                    memo=sentinel.memo,
                    security=sentinel.security1,
                    units=sentinel.units1,
                    fiaccountfrom=self.reader.account,
                    securityfrom=sentinel.security0,
                    unitsfrom=sentinel.units0,
                )
            ],
        )

        self.assertIsInstance(output, list)
        self.assertEqual(len(output), 1)
        self.assertIs(output[0], sentinel.transaction)

    def testGuessSecurity(self):
        pass

    def testMergeAccountTransfer(self):
        pass


class SplitTestCase(FlexStatementReaderMixin, unittest.TestCase):
    @patch.object(
        FlexStatementReader, "merge_transaction", return_value=sentinel.transaction
    )
    def testMergeSplit(self, mock_merge_transaction_method):
        self.securities[(sentinel.uniqueidtype, sentinel.uniqueid)] = sentinel.security
        corpAct = parser.CorporateAction(
            fitid=sentinel.fitid,
            dttrade=sentinel.dttrade,
            memo=sentinel.memo,
            uniqueidtype=sentinel.uniqueidtype,
            uniqueid=sentinel.uniqueid,
            units=sentinel.units,
            currency=None,
            total=None,
            type=None,
            reportdate=None,
            code=None,
        )
        output = self.reader.merge_split(
            corpAct,
            numerator=sentinel.numerator,
            denominator=sentinel.denominator,
            memo=None,
        )
        self.assertEqual(
            mock_merge_transaction_method.mock_calls,
            [
                call(
                    type=models.TransactionType.SPLIT,
                    fiaccount=self.reader.account,
                    uniqueid=sentinel.fitid,
                    datetime=sentinel.dttrade,
                    memo=sentinel.memo,
                    security=sentinel.security,
                    numerator=sentinel.numerator,
                    denominator=sentinel.denominator,
                    units=sentinel.units,
                )
            ],
        )
        self.assertIsInstance(output, list)
        self.assertEqual(len(output), 1)
        self.assertIs(output[0], sentinel.transaction)

    @patch.object(
        FlexStatementReader, "merge_transaction", return_value=sentinel.transaction
    )
    def testMergeSplitOverrideMemo(self, mock_merge_transaction_method):
        self.securities[(sentinel.uniqueidtype, sentinel.uniqueid)] = sentinel.security
        corpAct = parser.CorporateAction(
            fitid=sentinel.fitid,
            dttrade=sentinel.dttrade,
            memo=sentinel.memo,
            uniqueidtype=sentinel.uniqueidtype,
            uniqueid=sentinel.uniqueid,
            units=sentinel.units,
            currency=None,
            total=None,
            type=None,
            reportdate=None,
            code=None,
        )
        output = self.reader.merge_split(
            corpAct,
            numerator=sentinel.numerator,
            denominator=sentinel.denominator,
            memo=sentinel.override_memo,
        )
        self.assertEqual(
            mock_merge_transaction_method.mock_calls,
            [
                call(
                    type=models.TransactionType.SPLIT,
                    fiaccount=self.reader.account,
                    uniqueid=sentinel.fitid,
                    datetime=sentinel.dttrade,
                    memo=sentinel.override_memo,
                    security=sentinel.security,
                    numerator=sentinel.numerator,
                    denominator=sentinel.denominator,
                    units=sentinel.units,
                )
            ],
        )
        self.assertIsInstance(output, list)
        self.assertEqual(len(output), 1)
        self.assertIs(output[0], sentinel.transaction)


class SpinoffTestCase(FlexStatementReaderMixin, unittest.TestCase):
    @patch.object(
        FlexStatementReader, "merge_transaction", return_value=sentinel.transaction
    )
    def testSpinoff(self, mock_merge_transaction_method):
        self.securities[(sentinel.uniqueidtype, sentinel.uniqueid)] = sentinel.security
        corpAct = parser.CorporateAction(
            fitid=sentinel.fitid,
            dttrade=sentinel.dttrade,
            memo=sentinel.memo,
            uniqueidtype=sentinel.uniqueidtype,
            uniqueid=sentinel.uniqueid,
            units=sentinel.units,
            currency=None,
            total=None,
            type=None,
            reportdate=None,
            code=None,
        )
        output = self.reader.merge_spinoff(
            corpAct,
            securityfrom=sentinel.securityfrom,
            numerator=sentinel.numerator,
            denominator=sentinel.denominator,
            memo=None,
        )
        self.assertEqual(
            mock_merge_transaction_method.mock_calls,
            [
                call(
                    type=models.TransactionType.SPINOFF,
                    fiaccount=self.reader.account,
                    uniqueid=sentinel.fitid,
                    datetime=sentinel.dttrade,
                    memo=sentinel.memo,
                    security=sentinel.security,
                    numerator=sentinel.numerator,
                    denominator=sentinel.denominator,
                    units=sentinel.units,
                    securityfrom=sentinel.securityfrom,
                )
            ],
        )
        self.assertIsInstance(output, list)
        self.assertEqual(len(output), 1)
        self.assertIs(output[0], sentinel.transaction)


class OptionsExercisTestCase(FlexStatementReaderMixin, unittest.TestCase):
    def testDoOptionsExercises(self):
        pass


if __name__ == "__main__":
    unittest.main(verbosity=3)
