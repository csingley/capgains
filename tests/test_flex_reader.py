# coding: utf-8
"""Unit tests for capgains.flex.reader.
"""
import unittest
from unittest.mock import patch, call, sentinel
from datetime import datetime, date
from decimal import Decimal
import os

import ibflex

from capgains import ofx, flex, models, inventory
from capgains.config import CONFIG

from common import (
    setUpModule,
    tearDownModule,
    RollbackMixin,
    ReadXmlSnippetMixin,
    DB_STATE,
)


DB_URI = CONFIG.db_uri


class FlexStatementReaderMixin(RollbackMixin):
    @classmethod
    def setUpClass(cls):
        super(FlexStatementReaderMixin, cls).setUpClass()

        cls.reader = flex.reader.FlexStatementReader(cls.session)
        cls.securities = {}
        cls.reader.securities = cls.securities
        cls.reader.account = None


class ReadTestCase(RollbackMixin, unittest.TestCase):
    def setUp(self):
        # Need a default currency
        account = flex.Types.Account(
            brokerid="",
            acctid="",
            name=None,
            currency="USD",
        )
        statement = flex.Types.FlexStatement(
            account=account,
            securities=(),
            transactions=(),
            changeInDividendAccruals=(),
            conversionRates=(),
        )

        self.reader = flex.reader.FlexStatementReader(statement)
        super().setUp()

    @patch.object(ofx.reader.OfxStatementReader, "read")
    @patch.object(flex.reader.FlexStatementReader, "read_change_in_dividend_accruals")
    @patch.object(flex.reader.FlexStatementReader, "read_currency_rates")
    def testRead(
        self,
        mock_flex_read_currency_rates_method,
        mock_flex_read_change_in_dividends_method,
        mock_ofx_read_method,
    ):
        self.reader.read(self.session)
        mock_flex_read_currency_rates_method.assert_called_once()
        mock_flex_read_change_in_dividends_method.assert_called_once()
        mock_ofx_read_method.assert_called_with(self.session, True)

    def testReadChangeInDividendAccruals(self):
        """read_change_in_dividend_accruals()
        """
        div0 = ibflex.Types.ChangeInDividendAccrual(
            date=date(2010, 6, 1),
            conid=sentinel.conid0,
            payDate=sentinel.payDate0,
            code=[ibflex.enums.Code.REVERSE],
        )
        div1 = ibflex.Types.ChangeInDividendAccrual(
            date=date(2010, 7, 1),
            conid=sentinel.conid1,
            payDate=sentinel.payDate1,
            code=[ibflex.enums.Code.REVERSE],
        )
        div2 = ibflex.Types.ChangeInDividendAccrual(
            date=date(2010, 8, 1),
            conid=sentinel.conid2,
            payDate=sentinel.payDate2,
            code=[],
        )
        self.reader.statement = flex.Types.FlexStatement(
            account=None,
            securities=None,
            transactions=None,
            changeInDividendAccruals=[div0, div1, div2],
            conversionRates=None,
        )
        divs = self.reader.read_change_in_dividend_accruals(self.reader.statement)
        self.assertIsInstance(divs, dict)
        self.assertEqual(len(divs), 2)
        self.assertEqual(divs[(sentinel.conid0, sentinel.payDate0)], div0)
        self.assertEqual(divs[(sentinel.conid1, sentinel.payDate1)], div1)

    def testReadCurrencyRates(self):
        pass

    @patch.object(models.Security, "merge", wraps=lambda session, **sec: sec)
    def testReadSecurities(self, mock_security_merge_method):
        sec0 = flex.Types.Security(
            uniqueidtype=sentinel.cusip,
            uniqueid=sentinel.uniqueid0,
            secname=sentinel.secname0,
            ticker=sentinel.ticker0,
        )
        sec1 = flex.Types.Security(
            uniqueidtype=sentinel.isin,
            uniqueid=sentinel.uniqueid1,
            secname=sentinel.secname1,
            ticker=sentinel.ticker1,
        )
        self.reader.statement = flex.Types.FlexStatement(
            account=None,
            securities=[sec0, sec1],
            transactions=None,
            changeInDividendAccruals=None,
            conversionRates=None,
        )
        self.reader.read_securities(self.session)
        self.assertEqual(
            mock_security_merge_method.mock_calls,
            [
                call(
                    self.session,
                    uniqueidtype=sec0.uniqueidtype,
                    uniqueid=sec0.uniqueid,
                    name=sec0.secname,
                    ticker=sec0.ticker,
                ),
                call(
                    self.session,
                    uniqueidtype=sec1.uniqueidtype,
                    uniqueid=sec1.uniqueid,
                    name=sec1.secname,
                    ticker=sec1.ticker,
                ),
            ],
        )

    def testTransactionHandlers(self):
        handlers = flex.reader.FlexStatementReader.TRANSACTION_HANDLERS
        self.assertEqual(handlers["Trade"], "doTrades")
        self.assertEqual(handlers["CashTransaction"], "doCashTransactions")
        self.assertEqual(handlers["Transfer"], "doTransfers")
        self.assertEqual(handlers["CorporateAction"], "doCorporateActions")
        self.assertEqual(handlers["Exercise"], "doOptionsExercises")


class TradesTestCase(FlexStatementReaderMixin, unittest.TestCase):
    def testFilterTrades(self):
        t0 = flex.Types.Trade(
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

        t1 = flex.Types.Trade(
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

        t2 = flex.Types.Trade(
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

        t3 = flex.Types.Trade(
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

        t4 = flex.Types.Trade(
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

    def testIsTradeCancel(self):
        t0 = flex.Types.Trade(
            notes=[ibflex.enums.Code.CANCEL, ibflex.enums.Code.PARTIAL],
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
        self.assertEqual(self.reader.is_trade_cancel(t0), True)

        t1 = flex.Types.Trade(
            notes=[ibflex.enums.Code.OPENING, ibflex.enums.Code.CLOSING],
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
        self.assertEqual(self.reader.is_trade_cancel(t1), False)

    def testSortTradesToCancel(self):
        t0 = flex.Types.Trade(
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
        self.assertEqual(self.reader.sort_trades_to_cancel(t0), "something")

    def testSortForTrade(self):
        t0 = flex.Types.Trade(
            notes=[ibflex.enums.Code.MAXLOSS, ibflex.enums.Code.CLOSING],
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
        self.assertEqual(flex.reader.sortForTrade(t0), models.TransactionSort.MINGAIN)

        t1 = flex.Types.Trade(
            notes=[ibflex.enums.Code.LIFO, ibflex.enums.Code.CLOSING],
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
        self.assertEqual(flex.reader.sortForTrade(t1), models.TransactionSort.LIFO)

        t2 = flex.Types.Trade(
            notes=[ibflex.enums.Code.CANCEL, ibflex.enums.Code.OPENING],
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
        self.assertEqual(flex.reader.sortForTrade(t2), None)

        t3 = flex.Types.Trade(
            notes=[ibflex.enums.Code.MAXLOSS, ibflex.enums.Code.LIFO],
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
            flex.reader.sortForTrade(t3)


class CashTransactionsTestCase(FlexStatementReaderMixin, unittest.TestCase):
    def testIsRetOfCap(self):
        """is_retofcap() searches memo for text
        `return of capital` / `interimliquidation`.
        """
        t0 = flex.Types.CashTransaction(
            incometype="DIV",
            memo="foo ReTuRn Of CAPitAL bar",
            fitid=None,
            dttrade=None,
            dtsettle=None,
            uniqueidtype=None,
            uniqueid=None,
            currency=None,
            total=None,
        )
        self.assertEqual(self.reader.is_retofcap(t0), True)

        t1 = flex.Types.CashTransaction(
            incometype="DIV",
            memo="foo InTeRiMlIqUiDaTiOn bar",
            fitid=None,
            dttrade=None,
            dtsettle=None,
            uniqueidtype=None,
            uniqueid=None,
            currency=None,
            total=None,
        )
        self.assertEqual(self.reader.is_retofcap(t1), True)

        t2 = flex.Types.CashTransaction(
            incometype="DIV",
            memo="foo bar",
            fitid=None,
            dttrade=None,
            dtsettle=None,
            uniqueidtype=None,
            uniqueid=None,
            currency=None,
            total=None,
        )
        self.assertEqual(self.reader.is_retofcap(t2), False)

    @patch.object(flex.reader.FlexStatementReader, "stripCashTransactionMemo", wraps=lambda m: m)
    def testFingerprintCash(
        self, mock_strip_cash_memo_method, wraps=lambda memo: memo
    ):
        """
        FlexStatementReader.fingerprint_cash() returns
        (tx.dtsettle, (tx.uniqueidtype, tx.uniqueid), tx.memo)
        """
        tx = flex.Types.CashTransaction(
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
        output = self.reader.fingerprint_cash(tx)
        (tx)
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

    def testIsCashCancel(self):
        tx = flex.Types.CashTransaction(
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
        output = self.reader.is_cash_cancel(tx)
        self.assertEqual(output, False)

        tx = flex.Types.CashTransaction(
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
        output = self.reader.is_cash_cancel(tx)
        self.assertEqual(output, True)

        tx = flex.Types.CashTransaction(
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
        output = self.reader.is_cash_cancel(tx)
        self.assertEqual(output, True)

    def testSortCashForCancel(self):
        """
        FlexStatementReader.sort_cash_for_cancel() returns tx.fitid
        """
        tx = flex.Types.CashTransaction(
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
        output = self.reader.sort_cash_for_cancel(tx)
        self.assertEqual(output, tx.fitid)

    def testFixCashTransactions(self):
        """FlexStatement.fixCashTransaction() takes dttrade from dividendsPaid
        """
        # N.B. flex.reader.FlexStatementReader.dividends is keyed by type
        # datetime.date, but flex.parser.CashTransaction.dtsettle is type
        # datetime.datetime
        div = ibflex.Types.ChangeInDividendAccrual(
            date=sentinel.date,
            conid="UniQUeId",
            exDate=date(2010, 6, 1),
            payDate=sentinel.payDate,
        )

        self.reader.dividendsPaid[(div.conid, date(2012, 5, 3))] = div

        #  (uniqueid, dtsettle) is FlexStatementReader.dividendsPaid lookup key.
        #  Must use correct typs for these, not sentinel objects.
        tx = flex.Types.CashTransaction(
            memo=None,
            fitid=None,
            dttrade=None,
            dtsettle=datetime(2012, 5, 3),
            uniqueidtype=None,
            uniqueid=div.conid,
            currency=None,
            total=None,
            incometype=None,
        )
        output = self.reader.fixCashTransaction(tx)
        self.assertIsInstance(output, flex.Types.CashTransaction)
        self.assertEqual(output.memo, tx.memo)
        self.assertEqual(output.fitid, tx.fitid)
        self.assertEqual(output.dttrade, datetime(2010, 6, 1))  # datetime not date
        self.assertEqual(output.dtsettle, tx.dtsettle)
        self.assertEqual(output.uniqueidtype, tx.uniqueidtype)
        self.assertEqual(output.uniqueid, tx.uniqueid)
        self.assertEqual(output.currency, tx.currency)
        self.assertEqual(output.total, tx.total)
        self.assertEqual(output.incometype, tx.incometype)


class CashTransactionWithFilterCancelTestCase(
    ReadXmlSnippetMixin, unittest.TestCase
):
    stmt_sections = [
        """
        <CashTransactions>
        <CashTransaction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="RHDGF" description="RHDGF(ANN741081064) CASH DIVIDEND 1.00000000 USD PER SHARE (Ordinary Dividend)" conid="24" securityID="ANN741081064" securityIDType="ISIN" cusip="" isin="ANN741081064" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" dateTime="2016-04-12" amount="27800" type="Dividends" tradeID="" code="" transactionID="6349123456" reportDate="2016-04-14" clientReference="" />
        <CashTransaction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="RHDGF" description="RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE (Return of Capital)" conid="24" securityID="ANN741081064" securityIDType="ISIN" cusip="" isin="ANN741081064" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" dateTime="2016-04-13" amount="138215" type="Dividends" tradeID="" code="" transactionID="6352363694" reportDate="2016-04-14" clientReference="" />
        <CashTransaction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="RHDGF" description="RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE - REVERSAL (Return of Capital)" conid="24" securityID="ANN741081064" securityIDType="ISIN" cusip="" isin="ANN741081064" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" dateTime="2016-04-13" amount="-138215" type="Dividends" tradeID="" code="" transactionID="6356130554" reportDate="2016-04-15" clientReference="" />
        <CashTransaction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="RHDGF" description="RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE (Return of Capital)" conid="24" securityID="ANN741081064" securityIDType="ISIN" cusip="" isin="ANN741081064" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" dateTime="2016-04-13" amount="139000" type="Dividends" tradeID="" code="" transactionID="6356130558" reportDate="2016-04-15" clientReference="" />
        </CashTransactions>
        """
    ]

    @property
    def persisted_txs(self):
        # First CashTransaction is not return of capital; filtered out.
        # 3rd CashTransaction cancels 2nd CashTransaction, leaving the last
        # CashTransaction to be persisted.
        return [
            inventory.types.ReturnOfCapital(
                uniqueid=None,
                datetime=datetime(2016, 4, 13),
                dtsettle=datetime(2016, 4, 13),
                memo="RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE (Return of Capital)",
                currency=models.Currency.USD,
                cash=Decimal("139000"),
                fiaccount=self.account,
                security=self.securities[0],
            )
        ]


class TransfersTestCase(FlexStatementReaderMixin, unittest.TestCase):
    @patch("capgains.flex.reader.merge_account_transfer")
    def testDoTransfers(self, mock_merge_acct_transfer_method):
        tx0 = flex.Types.Transfer(
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
        tx1 = flex.Types.Transfer(
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
        tx2 = flex.Types.Transfer(
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
        self.reader.doTransfers(
            [tx0, tx1, tx2],
            self.session,
            self.reader.securities,
            self.reader.account,
            "USD",
        )

        self.assertEqual(
            mock_merge_acct_transfer_method.mock_calls,
            [
                call(
                    tx0,
                    session=self.session,
                    securities={},
                    account=None,
                ),
                call(
                    tx1,
                    session=self.session,
                    securities={},
                    account=None,
                ),
            ]
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
            "SD", "TO", "TO", "TC", "TC", "TO", "TO", "IC", "IC", "TC", "SO",
            "TC", "TC", "TC", "TC", "IC", "IC", "TC", "TC", "IC", "IC", "TO",
            "TO", "TC", "TO", "TO", "TC", "TO", "TO", "TC", "TC", "TO", "TC",
            "TO", "TC", "TC", "TC", "TC", "TC", "TO", "TO", "TO", "TO", "TC",
            "TC", "FS", "FS", "DW", "TO", "TO", "TC", "DW", "SO", "TC", "TC",
            "SO", "TC", "TC", "TC", "TC", "RI", "TC", "TC", "TC", "SO", "DW",
            "TC", "DW", "FS", "FS", "DW", "TC", "TC", "SO", "SO", "TO", "TO",
            "TC", "TC", "SD", "TO", "TO", "TO", "TO", "TC", "SO", "TO", "TO",
            "SR", "RI", "SR", "TC", "TC", "SO", "TC", "TC", "DW", "SD", "SD",
            "TC", "TC", "SR", "OR", "SR", "TC", "TC", "OR", "TC", "TC", "SD",
            "TO", "TO", "TC", "TC", "IC", "IC", "FS", "SO", "SO", "SO", "SO",
            "TC", "TO", "TO", "TC", "TO", "TC", "TC", "TC", "TC", "FS", "FS",
            "SD", "TC", "RI", "SO", "SO", "SR", "OR", "SR", "RI", "RI", "TO",
            "TO", "TC", "TC", "TC", "TC", "DW", "IC", "IC", "TO", "TO", "TC",
            "RI", "RI", "SR", "OR", "SR", "SR", "OR", "SR", "TC", "TC", "TC",
            "OR", "TC", "TC", "TC", "TC", "TC", "SO", "SO", "SO", "TC", "TC",
            "SO", "IC", "IC", "SO", "SO", "TO", "TO", "TC", "TC", "TC", "TC",
            "TC", "TC", "TC", "TC", "TC", "SO", "SO", "TC", "TC", "TC", "TO",
            "IC", "IC", "TC", "TO", "RI", "SR", "OR", "SR", "TC", "SD", "TC",
            "TC", "TC", "TC", "TO", "TO", "TC", "TC", "TC", "TO", "TO", "TO",
            "TO", "TO", "TO", "FS", "FS", "TO", "TO", "TO", "TO", "TO", "TO",
            "FS", "FS", "BM", "TO", "TO", "FS", "FS", "BM", "TO", "TO", "TC",
            "TO", "RI", "TO", "SR", "SR", "TC", "TC", "TC",
        ]
        self.assertEqual(len(self.corpActMemos), len(self.cmptypes))

    def testFingerprintCorpAct(self):
        """FlexStatementReader.fingerprint_corpact() uses the type name, not type
        """
        corpAct = flex.Types.CorporateAction(
            fitid=None,
            dttrade=sentinel.dttrade,
            memo=sentinel.memo,
            uniqueidtype=sentinel.uniqueidtype,
            uniqueid=sentinel.uniqueid,
            units=None,
            currency=None,
            total=None,
            type=ibflex.enums.Reorg.DELISTWORTHLESS,
            reportdate=None,
            code=None,
        )
        result = flex.reader.fingerprint_corpact(corpAct)
        self.assertEqual(
            result,
            (
                (corpAct.uniqueidtype, corpAct.uniqueid),
                corpAct.dttrade,
                "DELISTWORTHLESS",
                corpAct.memo,
            ),
        )

    def testIsCorpactCancel(self):
        corpAct0 = flex.Types.CorporateAction(
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
            code=[ibflex.enums.Code.CANCEL, ibflex.enums.Code.PARTIAL],
        )
        self.assertIs(flex.reader.is_corpact_cancel(corpAct0), True)

        corpAct0 = flex.Types.CorporateAction(
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
            code=[ibflex.enums.Code.OPENING, ibflex.enums.Code.PARTIAL],
        )
        self.assertIs(flex.reader.is_corpact_cancel(corpAct0), False)

    def testAreCorpActCancelPair(self):
        corpAct0 = flex.Types.CorporateAction(
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
        corpAct1 = flex.Types.CorporateAction(
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
        corpAct2 = flex.Types.CorporateAction(
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
            flex.reader.are_corpact_cancel_pair(corpAct0, corpAct1), False
        )
        self.assertIs(
            flex.reader.are_corpact_cancel_pair(corpAct0, corpAct2), True
        )

    def testNetCorpActs(self):
        corpAct0 = flex.Types.CorporateAction(
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
        corpAct1 = flex.Types.CorporateAction(
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

        result = flex.reader.net_corpacts(corpAct0, corpAct1)
        self.assertIsInstance(result, flex.Types.CorporateAction)
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

            type_ = self.cmptypes[i]
            ca = flex.Types.CorporateAction(
                None, None, memo, None, None, None, None, None, type_, None, None
            )
            pca = flex.reader.parseCorporateActionMemo(
                self.session,
                self.securities,
                ca
            )
            self.assertIsInstance(pca, flex.reader.ParsedCorpAct)
            self.assertIs(pca.raw, ca)
            self.assertEqual(pca.type, type_)
            self.assertEqual(pca.ticker, ticker)
            self.assertEqual(pca.cusip, cusip)
            self.assertEqual(pca.secname, secname)
            self.assertEqual(pca.memo, mem)

    def testFingerprintParsedCorpAct(self):
        corpAct = flex.Types.CorporateAction(
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
        pca = flex.reader.ParsedCorpAct(
            raw=corpAct,
            type=ibflex.enums.Reorg.DELISTWORTHLESS,
            ticker=None,
            cusip=None,
            secname=None,
            memo=sentinel.memo,
        )
        self.assertEqual(
            flex.reader.fingerprint_parsed_corpact(pca),
            (sentinel.dttrade, "DELISTWORTHLESS", sentinel.memo),
        )

    def testSortParsedCorpActs(self):
        corpAct = flex.Types.CorporateAction(
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
        pca = flex.reader.ParsedCorpAct(
            raw=corpAct, type=None, ticker=None, cusip=None, secname=None, memo=None
        )
        self.assertEqual(
            flex.reader.sort_parsed_corpacts(pca), sentinel.reportdate
        )

    def testMergeReorg(self):
        pass

    @patch("capgains.ofx.reader.merge_transaction", return_value=sentinel.transaction)
    def testMergeSecurityTransfer(self, mock_merge_transaction):
        self.securities[
            (sentinel.uniqueidtype0, sentinel.uniqueid0)
        ] = sentinel.security0
        self.securities[
            (sentinel.uniqueidtype1, sentinel.uniqueid1)
        ] = sentinel.security1
        src = flex.Types.CorporateAction(
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
        dest = flex.Types.CorporateAction(
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
        output = flex.reader.merge_security_transfer(
            self.session,
            self.securities,
            self.reader.account,
            src=src,
            dest=dest,
            memo=sentinel.memo,
        )

        self.assertEqual(
            mock_merge_transaction.mock_calls,
            [
                call(
                    self.session,
                    type=models.TransactionType.TRANSFER,
                    fiaccount=self.reader.account,
                    uniqueid=sentinel.fitid1,
                    datetime=sentinel.dttrade1,
                    memo=sentinel.memo,
                    security=sentinel.security1,
                    units=sentinel.units1,
                    fromfiaccount=self.reader.account,
                    fromsecurity=sentinel.security0,
                    fromunits=sentinel.units0,
                )
            ],
        )

        self.assertIs(output, sentinel.transaction)

    def testGuessSecurity(self):
        pass

    def testMergeAccountTransfer(self):
        pass


class SplitTestCase(FlexStatementReaderMixin, unittest.TestCase):
    @patch("capgains.ofx.reader.merge_transaction", return_value=sentinel.transaction)
    def testMergeSplit(self, mock_merge_transaction):
        self.securities[(sentinel.uniqueidtype, sentinel.uniqueid)] = sentinel.security
        corpAct = flex.Types.CorporateAction(
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
        output = flex.reader.merge_split(
            self.session,
            self.securities,
            self.reader.account,
            corpAct,
            numerator=sentinel.numerator,
            denominator=sentinel.denominator,
            memo=None,
        )
        self.assertEqual(
            mock_merge_transaction.mock_calls,
            [
                call(
                    self.session,
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
        self.assertIs(output, sentinel.transaction)

    @patch("capgains.ofx.reader.merge_transaction", return_value=sentinel.transaction)
    def testMergeSplitOverrideMemo(self, mock_merge_transaction):
        self.securities[(sentinel.uniqueidtype, sentinel.uniqueid)] = sentinel.security
        corpAct = flex.Types.CorporateAction(
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
        output = flex.reader.merge_split(
            self.session,
            self.securities,
            self.reader.account,
            corpAct,
            numerator=sentinel.numerator,
            denominator=sentinel.denominator,
            memo=sentinel.override_memo,
        )
        self.assertEqual(
            mock_merge_transaction.mock_calls,
            [
                call(
                    self.session,
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
        self.assertIs(output, sentinel.transaction)


class SpinoffTestCase(FlexStatementReaderMixin, unittest.TestCase):
    @patch("capgains.ofx.reader.merge_transaction", return_value=sentinel.transaction)
    def testSpinoff(self, mock_merge_transaction):
        self.securities[(sentinel.uniqueidtype, sentinel.uniqueid)] = sentinel.security
        corpAct = flex.Types.CorporateAction(
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
        output = flex.reader.merge_spinoff(
            self.session,
            self.securities,
            self.reader.account,
            corpAct,
            fromsecurity=sentinel.fromsecurity,
            numerator=sentinel.numerator,
            denominator=sentinel.denominator,
            memo=None,
        )
        self.assertEqual(
            mock_merge_transaction.mock_calls,
            [
                call(
                    self.session,
                    type=models.TransactionType.SPINOFF,
                    fiaccount=self.reader.account,
                    uniqueid=sentinel.fitid,
                    datetime=sentinel.dttrade,
                    memo=sentinel.memo,
                    security=sentinel.security,
                    numerator=sentinel.numerator,
                    denominator=sentinel.denominator,
                    units=sentinel.units,
                    fromsecurity=sentinel.fromsecurity,
                )
            ],
        )
        self.assertIs(output, sentinel.transaction)


class OptionsExercisTestCase(FlexStatementReaderMixin, unittest.TestCase):
    xml = (
        'OptionEAE>'

        '<OptionEAE accountId="U12345" acctAlias="Test Alias" model="" '
        'currency="USD" fxRateToBase="1" assetCategory="OPT" '
        'symbol="VXX   110805C00020000" description="VXX 05AUG11 20.0 C" '
        'conid="91900358" securityID="" securityIDType="" cusip="" isin="" '
        'underlyingConid="80789235" underlyingSymbol="VXX" issuer="" '
        'multiplier="100" strike="20" expiry="2011-08-05" putCall="C" '
        'principalAdjustFactor="" date="2011-08-05" '
        'transactionType="Assignment" quantity="20" tradePrice="0.0000" '
        'markPrice="0.0000" proceeds="0.00" commisionsAndTax="0.00" '
        'costBasis="21,792.73" realizedPnl="0.00" fxPnl="0.00" '
        'mtmPnl="20,620.00" tradeID="590365479" />'

        '<OptionEAE accountId="U12345" acctAlias="Test Alias" model="" '
        'currency="USD" fxRateToBase="1" assetCategory="STK" symbol="VXX" '
        'description="IPATH S&amp;P 500 VIX S/T FU ETN" conid="80789235" '
        'securityID="" securityIDType="" cusip="" isin="" underlyingConid="" '
        'underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" '
        'putCall="" principalAdjustFactor="" date="2011-08-05" '
        'transactionType="Sell" quantity="-2,000" tradePrice="20.0000" '
        'markPrice="34.7800" proceeds="40,000.00" commisionsAndTax="-0.77" '
        'costBasis="-39,999.23" realizedPnl="0.00" fxPnl="0.00" '
        'mtmPnl="-29,560.00" tradeID="590365480" />'

        '</OptionEAE>'
    )


if __name__ == "__main__":
    unittest.main(verbosity=3)
