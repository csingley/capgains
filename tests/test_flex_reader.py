# coding: utf-8
"""
"""
# stdlib imports
import unittest
from unittest.mock import patch, call, sentinel
from datetime import datetime
from decimal import Decimal
import xml.etree.ElementTree as ET
import os
import re
from collections import namedtuple


# 3rd party imports
import ibflex
from sqlalchemy import create_engine


# local imports
from capgains.flex import reader
from capgains.flex.reader import (
    FlexStatementReader, ParsedCorpAct,
)

from capgains.flex import parser
from capgains.flex.parser import (
    CorporateAction, CashTransaction,
)
from capgains.flex.regexes import corpActRE, whitespace
from capgains.models.transactions import (
    Fi, FiAccount, Security, SecurityId,
    Transaction,
)
from capgains.ofx.reader import OfxStatementReader
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

        self.fi = self.session.query(Fi).filter_by(brokerid='4705').one_or_none()
        if self.fi is None:
            self.fi = Fi(brokerid='4705', name='Dewey Cheatham & Howe')
        self.account = FiAccount(fi=self.fi, number='5678', name='Test')
        self.security = Security(name='Yoyodyne', ticker='YOYO')
        self.securityId = SecurityId(security=self.security,
                                     uniqueidtype='CONID', uniqueid='23')

        self.session.add_all([self.fi, self.account, self.security,
                              self.securityId])
        self.session.flush()
        self.securities = {('CONID', '23'): self.security, }
        self.reader = FlexStatementReader(self.session)
        self.reader.account = self.account
        self.reader.securities = self.securities

    def tearDown(self):
        self.session.close()
        self.transaction.rollback()
        self.connection.close()


class XmlSnippetTestCase(DatabaseTest):
    re = NotImplemented

    def parseXML(self, xml):
        self.securities = []
        elem = ET.fromstring(xml)
        tag, items = ibflex.parser.parse_list(elem)
        for tx in items:
            conid = tx['conid']
            ticker = tx['symbol']
            sec = Security.merge(self.session, ticker=ticker,
                                 uniqueidtype='CONID', uniqueid=conid)
            secId = self.session.query(SecurityId)\
                    .filter_by(security=sec, uniqueidtype='CONID',
                               uniqueid=conid).one()
            self.session.add(sec)
            self.reader.securities[('CONID', conid)] = sec
            self.securities.append((sec, secId))
            memo = tx['description']
        self.transactions = parser.SUBPARSERS[tag](items)

        match = corpActRE.match(memo)
        if match:
            memo = match.group('memo')
        return memo


class TradeXmlSnippetTestCase(DatabaseTest):
    def parseXML(self, xml):
        self.securities = []
        elem = ET.fromstring(xml)
        tag, items = ibflex.parser.parse_list(elem)
        assert tag == 'Trades'
        for tx in items:
            conid = tx['conid']
            ticker = tx['symbol']
            name = tx['description']
            sec = Security.merge(self.session, ticker=ticker, name=name,
                                 uniqueidtype='CONID', uniqueid=conid)
            secId = self.session.query(SecurityId).filter_by(security=sec, uniqueidtype='CONID', uniqueid=conid).one()
            self.session.add(sec)
            self.reader.securities[('CONID', conid)] = sec
            if (sec, secId) not in self.securities:
                self.securities.append((sec, secId))

        self.trades = parser.parse_trades(items)


class CorpActXmlSnippetTestCase(XmlSnippetTestCase):
    re = corpActRE


class CashTransactionXmlSnippetTestCase(XmlSnippetTestCase):
    re = re.compile(
        r"""
        (?P<ticker>[^(]+)
        \(
        (?P<isin>[^)]+)
        \)\s+
        (?P<memo>.+)
        """,
        re.VERBOSE | re.IGNORECASE
    )


class ReadTestCase(unittest.TestCase):
    def setUp(self):
        self.reader = FlexStatementReader(None)

    @patch.object(OfxStatementReader, 'read')
    @patch.object(FlexStatementReader, 'read_dividends')
    def testRead(self, mock_flex_read_dividends_method, mock_ofx_read_method):
        self.reader.read()
        mock_flex_read_dividends_method.assert_called_once()
        mock_ofx_read_method.assert_called_with(True)

    def testReadDividends(self):
        div0 = parser.Dividend(conid=sentinel.conid0, exDate=None,
                               payDate=sentinel.payDate0,
                               quantity=None, grossRate=None,
                               taxesAndFees=None, total=None)
        div1 = parser.Dividend(conid=sentinel.conid1, exDate=None,
                               payDate=sentinel.payDate1,
                               quantity=None, grossRate=None,
                               taxesAndFees=None, total=None)
        self.reader.statement = parser.FlexStatement(account=None,
                                                     securities=None,
                                                     dividends=[div0, div1],
                                                     transactions=None)
        self.reader.read_dividends()

        divs = self.reader.dividends
        self.assertIsInstance(divs, dict)
        self.assertEqual(len(divs), 2)
        self.assertEqual(divs[(sentinel.conid0, sentinel.payDate0)], div0)
        self.assertEqual(divs[(sentinel.conid1, sentinel.payDate1)], div1)

    @patch.object(Security, 'merge', wraps=lambda session, **sec: sec)
    def testReadSecurities(self, mock_security_merge_method):
        sec0 = parser.Security(uniqueidtype=sentinel.cusip,
                               uniqueid=sentinel.uniqueid0,
                               secname=sentinel.secname0,
                               ticker=sentinel.ticker0)
        sec1 = parser.Security(uniqueidtype=sentinel.isin,
                               uniqueid=sentinel.uniqueid1,
                               secname=sentinel.secname1,
                               ticker=sentinel.ticker1)
        self.reader.statement = parser.FlexStatement(account=None,
                                                     securities=[sec0, sec1],
                                                     dividends=None,
                                                     transactions=None)
        self.reader.read_securities()
        self.assertEqual(mock_security_merge_method.mock_calls, [
            call(None, uniqueidtype=sec0.uniqueidtype, uniqueid=sec0.uniqueid,
                 name=sec0.secname, ticker=sec0.ticker),
            call(None, uniqueidtype=sec1.uniqueidtype, uniqueid=sec1.uniqueid,
                 name=sec1.secname, ticker=sec1.ticker)])

    def testTransactionHandlers(self):
        handlers = self.reader.transaction_handlers
        self.assertEqual(len(handlers), 5)
        self.assertEqual(handlers['Trade'], 'doTrades')
        self.assertEqual(handlers['CashTransaction'], 'doCashTransactions')
        self.assertEqual(handlers['Transfer'], 'doTransfers')
        self.assertEqual(handlers['CorporateAction'], 'doCorporateActions')
        self.assertEqual(handlers['Exercise'], 'doOptionsExercises')


class TradesTestCase(TradeXmlSnippetTestCase, unittest.TestCase):
    def testFilterTrades(self):
        t0 = parser.Trade(memo='Something', fitid=None, dttrade=None,
                          uniqueidtype=None, uniqueid=None, units=None,
                          currency=None, total=None, reportdate=None,
                          tradeID=None, notes=None)
        self.assertEqual(self.reader.filterTrades(t0), True)

        t1 = parser.Trade(memo='USD.CAD', fitid=None, dttrade=None,
                          uniqueidtype=None, uniqueid=None, units=None,
                          currency=None, total=None, reportdate=None,
                          tradeID=None, notes=None)
        self.assertEqual(self.reader.filterTrades(t1), False)

        t2 = parser.Trade(memo='CAD.USD', fitid=None, dttrade=None,
                          uniqueidtype=None, uniqueid=None, units=None,
                          currency=None, total=None, reportdate=None,
                          tradeID=None, notes=None)
        self.assertEqual(self.reader.filterTrades(t2), False)

        t3 = parser.Trade(memo='USD.EUR', fitid=None, dttrade=None,
                          uniqueidtype=None, uniqueid=None, units=None,
                          currency=None, total=None, reportdate=None,
                          tradeID=None, notes=None)
        self.assertEqual(self.reader.filterTrades(t3), False)

        t4 = parser.Trade(memo='EUR.USD', fitid=None, dttrade=None,
                          uniqueidtype=None, uniqueid=None, units=None,
                          currency=None, total=None, reportdate=None,
                          tradeID=None, notes=None)
        self.assertEqual(self.reader.filterTrades(t4), False)

    def testFilterTradeCancels(self):
        t0 = parser.Trade(notes=['Ca', 'P'],
                          fitid=None, dttrade=None, memo=None,
                          uniqueidtype=None, uniqueid=None, units=None,
                          currency=None, total=None, reportdate=None,
                          tradeID=None)
        self.assertEqual(self.reader.filterTradeCancels(t0), True)

        t1 = parser.Trade(notes=['O', 'C'],
                          fitid=None, dttrade=None, memo=None,
                          uniqueidtype=None, uniqueid=None, units=None,
                          currency=None, total=None, reportdate=None,
                          tradeID=None)
        self.assertEqual(self.reader.filterTradeCancels(t1), False)

    def testSortCanceledTrades(self):
        t0 = parser.Trade(reportdate='something',
                          fitid=None, dttrade=None, memo=None,
                          uniqueidtype=None, uniqueid=None, units=None,
                          currency=None, total=None, tradeID=None, notes=None)
        self.assertEqual(self.reader.sortCanceledTrades(t0), 'something')

    def testSortForTrade(self):
        t0 = parser.Trade(notes=['ML', 'C'],
                          fitid=None, dttrade=None, memo=None,
                          uniqueidtype=None, uniqueid=None, units=None,
                          currency=None, total=None, reportdate=None,
                          tradeID=None)
        self.assertEqual(self.reader.sortForTrade(t0), 'MINGAIN')

        t1 = parser.Trade(notes=['LI', 'C'],
                          fitid=None, dttrade=None, memo=None,
                          uniqueidtype=None, uniqueid=None, units=None,
                          currency=None, total=None, reportdate=None,
                          tradeID=None)
        self.assertEqual(self.reader.sortForTrade(t1), 'LIFO')

        t2 = parser.Trade(notes=['Ca', 'O'],
                          fitid=None, dttrade=None, memo=None,
                          uniqueidtype=None, uniqueid=None, units=None,
                          currency=None, total=None, reportdate=None,
                          tradeID=None)
        self.assertEqual(self.reader.sortForTrade(t2), None)

        t3 = parser.Trade(notes=['ML', 'LI'],
                          fitid=None, dttrade=None, memo=None,
                          uniqueidtype=None, uniqueid=None, units=None,
                          currency=None, total=None, reportdate=None,
                          tradeID=None)
        with self.assertRaises(AssertionError):
            self.reader.sortForTrade(t3)

    def testDoTradesWithCancel(self):
        """
        Test Trades full stack, canceling trades.
        """
        xml = """
        <Trades>
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="CNVR.SPO" description="CONVERA CORPORATION - SPINOFF" conid="132118505" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="" reportDate="2013-08-01" tradeDate="2011-05-09" tradeTime="000000" settleDateTarget="" transactionType="FracShare" exchange="--" quantity="-0.276942" tradePrice="0.00001" tradeMoney="-0.000002769" proceeds="0.000002769" taxes="0" ibCommission="0" ibCommissionCurrency="USD" netCash="0.000002769" closePrice="0.00001" openCloseIndicator="C" notes="" cost="-449.712074" fifoPnlRealized="-449.712071" fxPnl="0" mtmPnl="-0" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="3723709320" buySell="SELL" ibOrderID="3723709320" ibExecID="" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="N/A" orderTime="" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="" traderID="" isAPIOrder="N" />
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="CNVR.SPO" description="CONVERA CORPORATION - SPINOFF" conid="132118505" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="" reportDate="2013-09-20" tradeDate="2011-05-09" tradeTime="000000" settleDateTarget="" transactionType="FracShareCancel" exchange="--" quantity="0.276942" tradePrice="0.00001" tradeMoney="0.000002769" proceeds="-0.000002769" taxes="0" ibCommission="0" ibCommissionCurrency="USD" netCash="-0.000002769" closePrice="0.00001" openCloseIndicator="" notes="Ca" cost="0.000002769" fifoPnlRealized="0" fxPnl="0" mtmPnl="0" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="3831648707" buySell="SELL (Ca.)" ibOrderID="3831648707" ibExecID="" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="N/A" orderTime="" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="" traderID="" isAPIOrder="N" />
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="CNVR.SPO" description="CONVERA CORPORATION - SPINOFF" conid="132118505" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="" reportDate="2013-09-20" tradeDate="2011-05-09" tradeTime="000000" settleDateTarget="" transactionType="FracShare" exchange="--" quantity="-0.276942" tradePrice="203.698646" tradeMoney="-56.412710421" proceeds="56.412710421" taxes="0" ibCommission="0" ibCommissionCurrency="USD" netCash="56.412710421" closePrice="0.00001" openCloseIndicator="C" notes="" cost="-0.000003" fifoPnlRealized="56.412708" fxPnl="0" mtmPnl="56.4127" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="3831652905" buySell="SELL" ibOrderID="3831652905" ibExecID="" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="N/A" orderTime="" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="" traderID="" isAPIOrder="N" />
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="CNVR.SPO" description="CONVERA CORPORATION - SPINOFF" conid="132118505" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="" reportDate="2013-11-18" tradeDate="2011-05-09" tradeTime="000000" settleDateTarget="" transactionType="FracShareCancel" exchange="--" quantity="0.276942" tradePrice="203.698646" tradeMoney="56.412710421" proceeds="-56.412710421" taxes="0" ibCommission="0" ibCommissionCurrency="USD" netCash="-56.412710421" closePrice="0.00001" openCloseIndicator="" notes="Ca" cost="56.412710421" fifoPnlRealized="0" fxPnl="0" mtmPnl="-56.4127" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="3964505548" buySell="SELL (Ca.)" ibOrderID="3964505548" ibExecID="" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="N/A" orderTime="" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="" traderID="" isAPIOrder="N" />
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="CNVR.SPO" description="CONVERA CORPORATION - SPINOFF" conid="132118505" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="" reportDate="2013-11-18" tradeDate="2011-05-09" tradeTime="000000" settleDateTarget="" transactionType="FracShare" exchange="--" quantity="-0.276942" tradePrice="5334.4" tradeMoney="-1477.3194048" proceeds="1477.3194048" taxes="0" ibCommission="0" ibCommissionCurrency="USD" netCash="1477.3194048" closePrice="0.00001" openCloseIndicator="C" notes="" cost="-56.41271" fifoPnlRealized="1420.906694" fxPnl="0" mtmPnl="1477.3194" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="3964508206" buySell="SELL" ibOrderID="3964508206" ibExecID="" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="N/A" orderTime="" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="" traderID="" isAPIOrder="N" />
        </Trades>
        """
        self.parseXML(xml)
        self.assertEqual(len(self.securities), 1)
        (security0, securityId0) = self.securities[0]
        self.assertIsInstance(security0, Security)
        self.assertEqual(security0.ticker, 'CNVR.SPO')
        self.assertEqual(security0.name, 'CONVERA CORPORATION - SPINOFF')
        self.assertIsInstance(securityId0, SecurityId)
        self.assertEqual(securityId0.uniqueidtype, 'CONID')
        self.assertEqual(securityId0.uniqueid, '132118505')
        self.assertIs(securityId0.security, security0)


        # 2nd trade cancels the 1st; 4th trade cancels the 3rd
        # Leaving the last trade as the only one
        self.reader.doTrades(self.trades)
        trans = self.reader.transactions
        self.assertEqual(len(trans), 1)
        tx0 = trans[0]

        self.assertIsInstance(tx0, Transaction)
        self.assertEqual(tx0.uniqueid, '3964508206')
        self.assertEqual(tx0.datetime, datetime(2011, 5, 9))
        self.assertEqual(tx0.type, 'trade')
        self.assertEqual(tx0.memo, 'CONVERA CORPORATION - SPINOFF')
        self.assertEqual(tx0.currency, 'USD')
        self.assertEqual(tx0.cash, Decimal('1477.3194048'))
        self.assertEqual(tx0.fiaccount, self.account)
        self.assertEqual(tx0.security, security0)
        self.assertEqual(tx0.units, Decimal('-0.276942'))
        self.assertEqual(tx0.securityPrice, None)
        self.assertEqual(tx0.fiaccountFrom, None)
        self.assertEqual(tx0.securityFrom, None)
        self.assertEqual(tx0.unitsFrom, None)
        self.assertEqual(tx0.securityFromPrice, None)
        self.assertEqual(tx0.numerator, None)
        self.assertEqual(tx0.denominator, None)
        self.assertEqual(tx0.sort, None)

    def testDoTradesIgnoreFX(self):
        """
        Test Trades full stack, ignoring FX and canceling trades.
        """
        xml = """
        <Trades>
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="CASH" symbol="EUR.USD" description="EUR.USD" conid="12087792" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="1315033695" reportDate="2015-05-29" tradeDate="2015-05-29" tradeTime="105322" settleDateTarget="2015-06-02" transactionType="ExchTrade" exchange="IDEALFX" quantity="57345" tradePrice="1.09755" tradeMoney="62939.00475" proceeds="-62939.00475" taxes="0" ibCommission="-2" ibCommissionCurrency="USD" netCash="0" closePrice="0" openCloseIndicator="" notes="" cost="0" fifoPnlRealized="0" fxPnl="0" mtmPnl="88.88475" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="5379869359" buySell="BUY" ibOrderID="669236976" ibExecID="00011364.55624cab.01.01" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="8071596799694996971" orderTime="2015-05-29;105322" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="LMT" traderID="" isAPIOrder="N" />
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="SSRAP" description="SATURNS SEARS ROEBUCK ACCEPTANCE CO" conid="18313671" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="1389766089" reportDate="2015-09-10" tradeDate="2015-09-10" tradeTime="134859" settleDateTarget="2015-09-15" transactionType="ExchTrade" exchange="MM" quantity="-100" tradePrice="16.2" tradeMoney="-1620" proceeds="1620" taxes="0" ibCommission="-0.28696525" ibCommissionCurrency="USD" netCash="1619.71303475" closePrice="16.2" openCloseIndicator="O" notes="P" cost="-1619.71303475" fifoPnlRealized="0" fxPnl="0" mtmPnl="0" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="5690187435" buySell="SELL" ibOrderID="706190606" ibExecID="00015070.55f176d9.01.01" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="SSRAP-IBKB3402-877507288" orderTime="2015-09-10;133314" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="LMT" traderID="" isAPIOrder="N" />
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="SSRAP" description="SATURNS SEARS ROEBUCK ACCEPTANCE CO" conid="18313671" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="1389985080" reportDate="2015-09-10" tradeDate="2015-09-10" tradeTime="155134" settleDateTarget="2015-09-15" transactionType="ExchTrade" exchange="MM" quantity="-1000" tradePrice="16.2" tradeMoney="-16200" proceeds="16200" taxes="0" ibCommission="-4.1196525" ibCommissionCurrency="USD" netCash="16195.8803475" closePrice="16.2" openCloseIndicator="C" notes="P" cost="-16003.66225" fifoPnlRealized="192.218097" fxPnl="0" mtmPnl="0" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="5690844790" buySell="SELL" ibOrderID="706190606" ibExecID="00015070.55f17811.01.01" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="SSRAP-IBKB3402-877510736" orderTime="2015-09-10;133314" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="LMT" traderID="" isAPIOrder="N" />
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="SSRAP" description="SATURNS SEARS ROEBUCK ACCEPTANCE CO" conid="18313671" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="1389987599" reportDate="2015-09-10" tradeDate="2015-09-10" tradeTime="155220" settleDateTarget="2015-09-15" transactionType="ExchTrade" exchange="MM" quantity="-900" tradePrice="16.2" tradeMoney="-14580" proceeds="14580" taxes="0" ibCommission="-3.70768725" ibCommissionCurrency="USD" netCash="14576.29231275" closePrice="16.2" openCloseIndicator="C" notes="P" cost="-14403.296347" fifoPnlRealized="172.995965" fxPnl="0" mtmPnl="0" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="5690854717" buySell="SELL" ibOrderID="706190606" ibExecID="00015070.55f17816.01.01" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="SSRAP-IBKB3402-877510760" orderTime="2015-09-10;133314" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="LMT" traderID="" isAPIOrder="N" />
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="SSRAP" description="SATURNS SEARS ROEBUCK ACCEPTANCE CO" conid="18313671" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="" reportDate="2015-09-11" tradeDate="2015-09-10" tradeTime="134859" settleDateTarget="" transactionType="TradeCancel" exchange="--" quantity="100" tradePrice="16.2" tradeMoney="1620" proceeds="-1620" taxes="0" ibCommission="0.28696525" ibCommissionCurrency="USD" netCash="-1619.71303475" closePrice="16.1" openCloseIndicator="" notes="Ca" cost="1619.71303475" fifoPnlRealized="0" fxPnl="0" mtmPnl="-10" origTradePrice="16.2" origTradeDate="2015-09-10" origTradeID="1389766089" origOrderID="706190606" clearingFirmID="" transactionID="5694297405" buySell="SELL (Ca.)" ibOrderID="706190606" ibExecID="" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="N/A" orderTime="" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="" traderID="" isAPIOrder="N" />
        </Trades>
        """
        self.parseXML(xml)
        (security1, securityId1) = self.securities[1]  # SSRAP

        # First trade is FX (skipped)
        # Last trade cancels the 2nd trade
        self.reader.doTrades(self.trades)
        trans = self.reader.transactions
        self.assertEqual(len(trans), 2)
        tx0, tx1 = trans

        self.assertIsInstance(tx0, Transaction)
        self.assertEqual(tx0.uniqueid, '5690844790')
        self.assertEqual(tx0.datetime, datetime(2015, 9, 10, 15, 51, 34))
        self.assertEqual(tx0.type, 'trade')
        self.assertEqual(tx0.memo, 'SATURNS SEARS ROEBUCK ACCEPTANCE CO')
        self.assertEqual(tx0.currency, 'USD')
        self.assertEqual(tx0.cash, Decimal('16195.8803475'))
        self.assertEqual(tx0.fiaccount, self.account)
        self.assertEqual(tx0.security, security1)
        self.assertEqual(tx0.units, Decimal('-1000'))
        self.assertEqual(tx0.securityPrice, None)
        self.assertEqual(tx0.fiaccountFrom, None)
        self.assertEqual(tx0.securityFrom, None)
        self.assertEqual(tx0.unitsFrom, None)
        self.assertEqual(tx0.securityFromPrice, None)
        self.assertEqual(tx0.numerator, None)
        self.assertEqual(tx0.denominator, None)
        self.assertEqual(tx0.sort, None)

        self.assertIsInstance(tx1, Transaction)
        self.assertEqual(tx1.uniqueid, '5690854717')
        self.assertEqual(tx1.datetime, datetime(2015, 9, 10, 15, 52, 20))
        self.assertEqual(tx1.type, 'trade')
        self.assertEqual(tx1.memo, 'SATURNS SEARS ROEBUCK ACCEPTANCE CO')
        self.assertEqual(tx1.currency, 'USD')
        self.assertEqual(tx1.cash, Decimal('14576.29231275'))
        self.assertEqual(tx1.fiaccount, self.account)
        self.assertEqual(tx1.security, security1)
        self.assertEqual(tx1.units, Decimal('-900'))
        self.assertEqual(tx1.securityPrice, None)
        self.assertEqual(tx1.fiaccountFrom, None)
        self.assertEqual(tx1.securityFrom, None)
        self.assertEqual(tx1.unitsFrom, None)
        self.assertEqual(tx1.securityFromPrice, None)
        self.assertEqual(tx1.numerator, None)
        self.assertEqual(tx1.denominator, None)
        self.assertEqual(tx1.sort, None)

    def testTradeWithSortField(self):
        xml = """
        <Trades>
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="PPC" description="PILGRIMS PRIDE CORP-NEW" conid="71395583" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="478725910" reportDate="2010-10-29" tradeDate="2010-10-29" tradeTime="143809" settleDateTarget="2010-11-03" transactionType="ExchTrade" exchange="IDEAL" quantity="-200" tradePrice="6.12" tradeMoney="-1224" proceeds="1224" taxes="0" ibCommission="-1" ibCommissionCurrency="USD" netCash="1223" closePrice="6.1" openCloseIndicator="C" notes="ML;P" cost="-1611" fifoPnlRealized="-388" fxPnl="0" mtmPnl="4" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="1920980366" buySell="SELL" ibOrderID="253174109" ibExecID="0000d323.999588c9.02.01" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="379963350S" orderTime="2010-10-29;143809" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="LMT" traderID="" isAPIOrder="N" />
        </Trades>
        """
        self.parseXML(xml)
        (security0, securityId0) = self.securities[0]

        self.reader.doTrades(self.trades)
        trans = self.reader.transactions
        self.assertEqual(len(trans), 1)
        tx0 = trans[0]

        self.assertIsInstance(tx0, Transaction)
        self.assertEqual(tx0.uniqueid, '1920980366')
        self.assertEqual(tx0.datetime, datetime(2010, 10, 29, 14, 38, 9))
        self.assertEqual(tx0.type, 'trade')
        self.assertEqual(tx0.memo, 'PILGRIMS PRIDE CORP-NEW')
        self.assertEqual(tx0.currency, 'USD')
        self.assertEqual(tx0.cash, Decimal('1223'))
        self.assertEqual(tx0.fiaccount, self.account)
        self.assertEqual(tx0.security, security0)
        self.assertEqual(tx0.units, Decimal('-200'))
        self.assertEqual(tx0.securityPrice, None)
        self.assertEqual(tx0.fiaccountFrom, None)
        self.assertEqual(tx0.securityFrom, None)
        self.assertEqual(tx0.unitsFrom, None)
        self.assertEqual(tx0.securityFromPrice, None)
        self.assertEqual(tx0.numerator, None)
        self.assertEqual(tx0.denominator, None)
        self.assertEqual(tx0.sort, 'MINGAIN')


class CashTransactionsTestCase(CashTransactionXmlSnippetTestCase, unittest.TestCase):
    def testFilterCashTransactions(self):
        t0 = parser.CashTransaction(incometype='Dividends',
                                    memo='foo ReTuRn Of CAPitAL bar',
                                    fitid=None, dttrade=None, dtsettle=None,
                                    uniqueidtype=None, uniqueid=None,
                                    currency=None, total=None)
        self.assertEqual(self.reader.filterCashTransactions(t0), True)

        t1 = parser.CashTransaction(incometype='Dividends',
                                    memo='foo InTeRiMlIqUiDaTiOn bar',
                                    fitid=None, dttrade=None, dtsettle=None,
                                    uniqueidtype=None, uniqueid=None,
                                    currency=None, total=None)
        self.assertEqual(self.reader.filterCashTransactions(t1), True)

        t2 = parser.CashTransaction(incometype='Dividends',
                                    memo='foo bar',
                                    fitid=None, dttrade=None, dtsettle=None,
                                    uniqueidtype=None, uniqueid=None,
                                    currency=None, total=None)
        self.assertEqual(self.reader.filterCashTransactions(t2), False)

        t3 = parser.CashTransaction(incometype='Income',
                                    memo='foo interimliquidation bar',
                                    fitid=None, dttrade=None, dtsettle=None,
                                    uniqueidtype=None, uniqueid=None,
                                    currency=None, total=None)
        self.assertEqual(self.reader.filterCashTransactions(t3), False)

    @patch.object(FlexStatementReader, 'stripCashTransactionMemo', wraps=lambda m: m)
    def testGroupCashTransactionsForCancel(self, mock_strip_cash_memo_method,
                                           wraps=lambda memo: memo):
        """
        FlexStatementReader.groupCashTransactionsForCancel() returns
        (tx.dtsettle, (tx.uniqueidtype, tx.uniqueid), tx.memo)
        """
        tx = parser.CashTransaction(memo=sentinel.memo,
                                    fitid=None, dttrade=None,
                                    dtsettle=sentinel.dtsettle,
                                    uniqueidtype=sentinel.uniqueidtype,
                                    uniqueid=sentinel.uniqueid,
                                    currency=None, total=None, incometype=None)
        output = self.reader.groupCashTransactionsForCancel(tx)
        self.assertEqual(output,
                         (sentinel.dtsettle,
                          (sentinel.uniqueidtype, sentinel.uniqueid),
                          sentinel.memo))

    def testStripCashTransactionsMemo(self):
        memo = 'foo - REVERSAL bar'
        self.assertEqual(self.reader.stripCashTransactionMemo(memo), 'foo bar')

        memo = 'foo CANCEL bar'
        self.assertEqual(self.reader.stripCashTransactionMemo(memo), 'foo bar')

    def testFilterCashTransactionCancels(self):
        tx = parser.CashTransaction(memo='foobar',
                                    fitid=None, dttrade=None, dtsettle=None,
                                    uniqueidtype=None, uniqueid=None,
                                    currency=None, total=None, incometype=None)
        output = self.reader.filterCashTransactionCancels(tx)
        self.assertEqual(output, False)

        tx = parser.CashTransaction(memo='fooREVERSALbar',
                                    fitid=None, dttrade=None, dtsettle=None,
                                    uniqueidtype=None, uniqueid=None,
                                    currency=None, total=None, incometype=None)
        output = self.reader.filterCashTransactionCancels(tx)
        self.assertEqual(output, True)

        tx = parser.CashTransaction(memo='fooCANCELbar',
                                    fitid=None, dttrade=None, dtsettle=None,
                                    uniqueidtype=None, uniqueid=None,
                                    currency=None, total=None, incometype=None)
        output = self.reader.filterCashTransactionCancels(tx)
        self.assertEqual(output, True)

    def testSortCanceledCashTransactions(self):
        """
        FlexStatementReader.sortCanceledCashTransactions() returns tx.fitid
        """
        tx = parser.CashTransaction(memo=None, fitid=sentinel.fitid,
                                    dttrade=None, dtsettle=None,
                                    uniqueidtype=None, uniqueid=None,
                                    currency=None, total=None, incometype=None)
        output = self.reader.sortCanceledCashTransactions(tx)
        self.assertEqual(output, tx.fitid)

    def testFixCashTransactions(self):
        div = parser.Dividend(conid=sentinel.conid, exDate=sentinel.exDate,
                              payDate=sentinel.payDate,
                              quantity=None, grossRate=None,
                              taxesAndFees=None, total=None)
        self.reader.dividends[(sentinel.conid, sentinel.payDate)] = div

        tx = parser.CashTransaction(memo=None, fitid=None, dttrade=None,
                                    dtsettle=sentinel.payDate,
                                    uniqueidtype=None, uniqueid=sentinel.conid,
                                    currency=None, total=None, incometype=None)
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

    def testDoCashTransactions(self):
        xml = """
        <CashTransactions>
        <CashTransaction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="RHDGF"
        description="RHDGF(ANN741081064) CASH DIVIDEND 1.00000000 USD PER SHARE (Ordinary Dividend)" conid="24" securityID="ANN741081064" securityIDType="ISIN" cusip="" isin="ANN741081064" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" dateTime="2016-04-12" amount="27800" type="Dividends" tradeID="" code="" transactionID="6349123456" reportDate="2016-04-14" clientReference="" />
        <CashTransaction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="RHDGF" description="RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE (Return of Capital)" conid="24" securityID="ANN741081064" securityIDType="ISIN" cusip="" isin="ANN741081064" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" dateTime="2016-04-13" amount="138215" type="Dividends" tradeID="" code="" transactionID="6352363694" reportDate="2016-04-14" clientReference="" />
        <CashTransaction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="RHDGF" description="RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE - REVERSAL (Return of Capital)" conid="24" securityID="ANN741081064" securityIDType="ISIN" cusip="" isin="ANN741081064" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" dateTime="2016-04-13" amount="-138215" type="Dividends" tradeID="" code="" transactionID="6356130554" reportDate="2016-04-15" clientReference="" />
        <CashTransaction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="RHDGF" description="RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE (Return of Capital)" conid="24" securityID="ANN741081064" securityIDType="ISIN" cusip="" isin="ANN741081064" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" dateTime="2016-04-13" amount="139000" type="Dividends" tradeID="" code="" transactionID="6356130558" reportDate="2016-04-15" clientReference="" />
        </CashTransactions>
        """
        memo = self.parseXML(xml)
        (security0, securityId0) = self.securities[0]
        self.reader.doCashTransactions(self.transactions)
        trans = self.reader.transactions
        # First CashTransaction is not return of capital; filtered out.
        # 3rd CashTransaction cancels 2nd CashTransaction, leaving the last
        # CashTransaction to be persisted.
        self.assertEqual(len(trans), 1)
        net = trans[0]
        self.assertIsInstance(net, Transaction)

        self.assertEqual(net.uniqueid, '6356130558')
        self.assertEqual(net.datetime, datetime(2016, 4, 13))
        self.assertEqual(net.type, 'returnofcapital')
        self.assertEqual(net.memo, 'RHDGF(ANN741081064) CASH DIVIDEND 5.00000000 USD PER SHARE (Return of Capital)')
        self.assertEqual(net.currency, 'USD')
        self.assertEqual(net.cash, Decimal('139000'))
        self.assertEqual(net.fiaccount, self.account)
        self.assertEqual(net.security, security0)
        self.assertEqual(net.units, None)
        self.assertEqual(net.securityPrice, None)
        self.assertEqual(net.fiaccountFrom, None)
        self.assertEqual(net.securityFrom, None)
        self.assertEqual(net.unitsFrom, None)
        self.assertEqual(net.securityFromPrice, None)
        self.assertEqual(net.numerator, None)
        self.assertEqual(net.denominator, None)
        self.assertEqual(net.sort, None)


class TransfersTestCase(DatabaseTest, unittest.TestCase):
    @patch.object(FlexStatementReader, 'merge_account_transfer')
    def testDoTransfers(self, mock_merge_acct_transfer_method):
        tx0 = parser.Transfer(fitid=None, dttrade=None, memo=None,
                              uniqueidtype=None, uniqueid=sentinel.conid0,
                              units=None, tferaction=None, type=None,
                              other_acctid=None)
        tx1 = parser.Transfer(fitid=None, dttrade=None, memo=None,
                              uniqueidtype=None, uniqueid=sentinel.condid1,
                              units=None, tferaction=None, type=None,
                              other_acctid=None)
        tx2 = parser.Transfer(fitid=None, dttrade=None, memo=None,
                              uniqueidtype=None, uniqueid=None, units=None,
                              tferaction=None, type=None, other_acctid=None)
        self.reader.doTransfers([tx0, tx1, tx2])

        self.assertEqual(mock_merge_acct_transfer_method.mock_calls,
                         [call(tx0), call(tx1)])


class CorporateActionsTestCase(CorpActXmlSnippetTestCase, unittest.TestCase):
    def setUp(self):
        super(self.__class__, self).setUp()
        path = os.path.join(os.path.dirname(__file__), 'data',
                            'corpact_descriptions.txt')
        with open(path) as file_:
            self.corpActMemos = file_.readlines()
        self.cmptypes = [
            'SD', 'TO', 'TO', 'TC', 'TC', 'TO', 'TO', 'IC', 'IC', 'TC', 'SO',
            'TC', 'TC', 'TC', 'TC', 'IC', 'IC', 'TC', 'TC', 'IC', 'IC', 'TO',
            'TO', 'TC', 'TO', 'TO', 'TC', 'TO', 'TO', 'TC', 'TC', 'TO', 'TC',
            'TO', 'TC', 'TC', 'TC', 'TC', 'TC', 'TO', 'TO', 'TO', 'TO', 'TC',
            'TC', 'FS', 'FS', 'DW', 'TO', 'TO', 'TC', 'DW', 'SO', 'TC', 'TC',
            'SO', 'TC', 'TC', 'TC', 'TC', 'RI', 'TC', 'TC', 'TC', 'SO', 'DW',
            'TC', 'DW', 'FS', 'FS', 'DW', 'TC', 'TC', 'SO', 'SO', 'TO', 'TO',
            'TC', 'TC', 'SD', 'TO', 'TO', 'TO', 'TO', 'TC', 'SO', 'TO', 'TO',
            'SR', 'RI', 'SR', 'TC', 'TC', 'SO', 'TC', 'TC', 'DW', 'SD', 'SD',
            'TC', 'TC', 'SR', 'OR', 'SR', 'TC', 'TC', 'OR', 'TC', 'TC', 'SD',
            'TO', 'TO', 'TC', 'TC', 'IC', 'IC', 'FS', 'SO', 'SO', 'SO', 'SO',
            'TC', 'TO', 'TO', 'TC', 'TO', 'TC', 'TC', 'TC', 'TC', 'FS', 'FS',
            'SD', 'TC', 'RI', 'SO', 'SO', 'SR', 'OR', 'SR', 'RI', 'RI', 'TO',
            'TO', 'TC', 'TC', 'TC', 'TC', 'DW', 'IC', 'IC', 'TO', 'TO', 'TC',
            'RI', 'RI', 'SR', 'OR', 'SR', 'SR', 'OR', 'SR', 'TC', 'TC', 'TC',
            'OR', 'TC', 'TC', 'TC', 'TC', 'TC', 'SO', 'SO', 'SO', 'TC', 'TC',
            'SO', 'IC', 'IC', 'SO', 'SO', 'TO', 'TO', 'TC', 'TC', 'TC', 'TC',
            'TC', 'TC', 'TC', 'TC', 'TC', 'SO', 'SO', 'TC', 'TC', 'TC', 'TO',
            'IC', 'IC', 'TC', 'TO', 'RI', 'SR', 'OR', 'SR', 'TC', 'SD', 'TC',
            'TC', 'TC', 'TC', 'TO', 'TO', 'TC', 'TC', 'TC', 'TO', 'TO', 'TO',
            'TO', 'TO', 'TO', 'FS', 'FS', 'TO', 'TO', 'TO', 'TO', 'TO', 'TO',
            'FS', 'FS', 'BM', 'TO', 'TO', 'FS', 'FS', 'BM', 'TO', 'TO', 'TC',
            'TO', 'RI', 'TO', 'SR', 'SR', 'TC', 'TC', 'TC']
        self.assertEqual(len(self.corpActMemos), len(self.cmptypes))

    def testDoCorporateActions(self):
        xml = """
        <CorporateActions>
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="ELAN" description="ELAN(US28413U2042) TENDERED TO US28413TEMP2 1 FOR 1 (ELAN, ELANDIA INTERNATIONAL INC, 28413U204)" conid="44939653" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2012-05-16" dateTime="2012-05-14;194500" amount="0" proceeds="0" value="0" quantity="-557915" fifoPnlRealized="0" mtmPnl="0" code="" type="TO" />
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="ELAN.CNT" description="ELAN.TEMP(US28413TEMP2) MERGED(Acquisition)  WITH US284CNT9952 1 FOR 10000 (ELAN.CNT, ELANDIA INTERNATIONAL INC - CONTRA, 284CNT995)" conid="107374662" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2012-05-16" dateTime="2012-05-15;202500" amount="0" proceeds="0" value="21.758685" quantity="55.7915" fifoPnlRealized="0" mtmPnl="0" code="" type="TC" />
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="ELAN.TEMP" description="ELAN(US28413U2042) TENDERED TO US28413TEMP2 1 FOR 1 (ELAN.TEMP, ELANDIA INTERNATIONAL INC - TEMP, 28413TEMP)" conid="107375314" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2012-05-16" dateTime="2012-05-14;194500" amount="0" proceeds="0" value="0" quantity="557915" fifoPnlRealized="0" mtmPnl="0" code="" type="TO" />
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="ELAN.TEMP" description="ELAN.TEMP(US28413TEMP2) MERGED(Acquisition)  WITH US284CNT9952 1 FOR 10000 (ELAN.TEMP, ELANDIA INTERNATIONAL INC - TEMP, 28413TEMP)" conid="107375314" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2012-05-16" dateTime="2012-05-15;202500" amount="0" proceeds="0" value="0" quantity="-557915" fifoPnlRealized="0" mtmPnl="21.7587" code="" type="TC" />
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="ELAN.CNT" description="ELAN.TEMP(US28413TEMP2) MERGED(Acquisition)  WITH US284CNT9952 1 FOR 10000 (ELAN.CNT, ELANDIA INTERNATIONAL INC - CONTRA, 284CNT995)" conid="107374662" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2012-05-24" dateTime="2012-05-15;202500" amount="0" proceeds="0" value="-217586.85" quantity="-55.7915" fifoPnlRealized="0" mtmPnl="0" code="Ca" type="TC" />
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="ELAN.CNT" description="ELAN.TEMP(US28413TEMP2) MERGED(Acquisition)  WITH US284CNT9952 1 FOR 10000 (ELAN.CNT, ELANDIA INTERNATIONAL INC - CONTRA, 284CNT995)" conid="107374662" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2012-05-24" dateTime="2012-05-15;202500" amount="0" proceeds="0" value="218400" quantity="56" fifoPnlRealized="0" mtmPnl="0" code="" type="TC" />
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="ELAN.TEMP" description="ELAN.TEMP(US28413TEMP2) MERGED(Acquisition)  WITH US284CNT9952 1 FOR 10000 (ELAN.TEMP, ELANDIA INTERNATIONAL INC - TEMP, 28413TEMP)" conid="107375314" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2012-05-24" dateTime="2012-05-15;202500" amount="0" proceeds="0" value="0" quantity="557915" fifoPnlRealized="0" mtmPnl="-217586.85" code="Ca" type="TC" />
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="ELAN.TEMP" description="ELAN.TEMP(US28413TEMP2) MERGED(Acquisition)  WITH US284CNT9952 1 FOR 10000 (ELAN.TEMP, ELANDIA INTERNATIONAL INC - TEMP, 28413TEMP)" conid="107375314" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2012-05-24" dateTime="2012-05-15;202500" amount="0" proceeds="0" value="0" quantity="-557915" fifoPnlRealized="0" mtmPnl="218400" code="" type="TC" />
        </CorporateActions>
        """
        self.parseXML(xml)
        (security0, securityId0) = self.securities[0]
        (security1, securityId1) = self.securities[1]
        (security2, securityId2) = self.securities[2]
        self.reader.doCorporateActions(self.transactions)
        trans = self.reader.transactions
        self.assertEqual(len(trans), 2)
        tran0, tran1 = trans
        self.assertIsInstance(tran0, Transaction)
        # self.assertEqual(tran0.id, None)
        # self.assertEqual(tran0.uniqueid, None)
        self.assertEqual(tran0.datetime, datetime(2012, 5, 14, 19, 45))
        self.assertEqual(tran0.type, 'transfer')
        self.assertEqual(tran0.memo, 'ELAN(US28413U2042) TENDERED TO US28413TEMP2 1 FOR 1')
        self.assertEqual(tran0.currency, None)
        self.assertEqual(tran0.cash, None)
        self.assertEqual(tran0.fiaccount, self.account)
        self.assertEqual(tran0.security, security2)
        self.assertEqual(tran0.units, Decimal('557915'))
        self.assertEqual(tran0.securityPrice, None)
        self.assertEqual(tran0.fiaccountFrom, self.account)
        self.assertEqual(tran0.securityFrom, security0)
        self.assertEqual(tran0.unitsFrom, Decimal('-557915'))
        self.assertEqual(tran0.securityFromPrice, None)
        self.assertEqual(tran0.numerator, None)
        self.assertEqual(tran0.denominator, None)
        self.assertEqual(tran0.sort, None)

        self.assertIsInstance(tran1, Transaction)
        self.assertEqual(tran1.fiaccount, self.account)
        # self.assertEqual(tran1.id, None)
        # self.assertEqual(tran1.uniqueid, None)
        self.assertEqual(tran1.datetime, datetime(2012, 5, 15, 20, 25))
        self.assertEqual(tran1.type, 'transfer')
        self.assertEqual(tran1.memo, 'ELAN.TEMP(US28413TEMP2) MERGED(Acquisition)  WITH US284CNT9952 1 FOR 10000')
        self.assertEqual(tran1.currency, None)
        self.assertEqual(tran1.cash, None)
        self.assertEqual(tran1.fiaccount, self.account)
        self.assertEqual(tran1.security, security1)
        self.assertEqual(tran1.units, Decimal('56'))
        self.assertEqual(tran1.securityPrice, None)
        self.assertEqual(tran1.fiaccountFrom, self.account)
        self.assertEqual(tran1.securityFrom, security2)
        self.assertEqual(tran1.unitsFrom, Decimal('-557915'))
        self.assertEqual(tran1.securityFromPrice, None)
        self.assertEqual(tran1.numerator, None)
        self.assertEqual(tran1.denominator, None)
        self.assertEqual(tran1.sort, None)

    def testGroupCorporateAcionsForCancel(self):
        corpAct = parser.CorporateAction(
            fitid=None, dttrade=sentinel.dttrade, memo=sentinel.memo,
            uniqueidtype=sentinel.uniqueidtype, uniqueid=sentinel.uniqueid,
            units=None, currency=None, total=None, type=sentinel.type,
            reportdate=None, code=None)
        result = self.reader.groupCorporateActionsForCancel(corpAct)
        self.assertEqual(result, (
            (corpAct.uniqueidtype, corpAct.uniqueid), corpAct.dttrade,
            corpAct.type, corpAct.memo))

    def testFilterCorporateActionCancels(self):
        corpAct0 = parser.CorporateAction(
            fitid=None, dttrade=None, memo=None, uniqueidtype=None,
            uniqueid=None, units=None, currency=None, total=None,
            type=None, reportdate=None, code=['Ca', 'P'])
        self.assertIs(self.reader.filterCorporateActionCancels(corpAct0),
                      True)

        corpAct0 = parser.CorporateAction(
            fitid=None, dttrade=None, memo=None, uniqueidtype=None,
            uniqueid=None, units=None, currency=None, total=None,
            type=None, reportdate=None, code=['O', 'P'])
        self.assertIs(self.reader.filterCorporateActionCancels(corpAct0),
                      False)

    def testMatchCorporateActionWithCancel(self):
        corpAct0 = parser.CorporateAction(
            fitid=None, dttrade=None, memo=None, uniqueidtype=None,
            uniqueid=None, units=5, currency=None, total=None,
            type=None, reportdate=None, code=None)
        corpAct1 = parser.CorporateAction(
            fitid=None, dttrade=None, memo=None, uniqueidtype=None,
            uniqueid=None, units=5, currency=None, total=None,
            type=None, reportdate=None, code=None)
        corpAct2 = parser.CorporateAction(
            fitid=None, dttrade=None, memo=None, uniqueidtype=None,
            uniqueid=None, units=-5, currency=None, total=None,
            type=None, reportdate=None, code=None)

        self.assertIs(self.reader.matchCorporateActionWithCancel(
            corpAct0, corpAct1), False)
        self.assertIs(self.reader.matchCorporateActionWithCancel(
            corpAct0, corpAct2), True)

    def testSortCanceledCorporateActions(self):
        corpAct = parser.CorporateAction(
            fitid=None, dttrade=None, memo=None, uniqueidtype=None,
            uniqueid=None, units=None, currency=None, total=None,
            type=None, reportdate=sentinel.reportdate, code=None)

        self.assertIs(self.reader.sortCanceledCorporateActions(corpAct),
                      sentinel.reportdate)

    def testNetCorporateActions(self):
        corpAct0 = parser.CorporateAction(
            fitid=sentinel.fitid0, dttrade=sentinel.dttrade0,
            memo=sentinel.memo0, uniqueidtype=sentinel.uniqueidtype0,
            uniqueid=sentinel.uniqueid0, units=2, currency=sentinel.currency0,
            total=20, type=sentinel.type0, reportdate=sentinel.reportdate0,
            code=sentinel.code0)
        corpAct1 = parser.CorporateAction(
            fitid=None, dttrade=None, memo=None, uniqueidtype=None,
            uniqueid=None, units=3, currency=sentinel.currency0, total=30,
            type=None, reportdate=None, code=None)

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
            demarc = memo.rfind('(')
            mem = memo[:demarc].strip()
            payload = memo[demarc:].strip().strip('()').split(', ')
            cusip = payload.pop()
            ticker = payload.pop(0)
            # If ticker has timestamp prepended, strip it
            if ticker.startswith('20'):
                ticker = ticker[14:]
            # Sometimes secname contains commas, so gets split
            # If so, stitch it back together
            secname = ', '.join(payload)

            typ = self.cmptypes[i]
            ca = CorporateAction(None, None, memo, None, None, None, None, None, typ, None, None)
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
            fitid=None, dttrade=sentinel.dttrade, memo=None, uniqueidtype=None,
            uniqueid=None, units=None, currency=None, total=None,
            type=None, reportdate=sentinel.reportdate, code=None)
        pca = reader.ParsedCorpAct(raw=corpAct, type=sentinel.type,
                                   ticker=None, cusip=None, secname=None,
                                   memo=sentinel.memo)
        self.assertEqual(self.reader.groupParsedCorporateActions(pca),
                         (sentinel.dttrade, sentinel.type, sentinel.memo))

    def testSortParsedCorporateActions(self):
        corpAct = parser.CorporateAction(
            fitid=None, dttrade=None, memo=None, uniqueidtype=None,
            uniqueid=None, units=None, currency=None, total=None,
            type=None, reportdate=sentinel.reportdate, code=None)
        pca = reader.ParsedCorpAct(raw=corpAct, type=None, ticker=None,
                                   cusip=None, secname=None, memo=None)
        self.assertEqual(self.reader.sortParsedCorporateActions(pca),
                         sentinel.reportdate)

    def testMergeReorg(self):
        pass

    @patch.object(FlexStatementReader, 'merge_transaction', return_value=sentinel.transaction)
    def testMergeSecurityTransfer(self, mock_merge_transaction_method):
        self.securities[(sentinel.uniqueidtype0, sentinel.uniqueid0)] = sentinel.security0
        self.securities[(sentinel.uniqueidtype1, sentinel.uniqueid1)] = sentinel.security1
        src = parser.CorporateAction(
            fitid=None, dttrade=None, memo=sentinel.memo0,
            uniqueidtype=sentinel.uniqueidtype0, uniqueid=sentinel.uniqueid0,
            units=sentinel.units0, currency=None, total=None, type=None,
            reportdate=None, code=None)
        dest = parser.CorporateAction(
            fitid=sentinel.fitid1, dttrade=sentinel.dttrade1,
            memo=sentinel.memo1, uniqueidtype=sentinel.uniqueidtype1,
            uniqueid=sentinel.uniqueid1, units=sentinel.units1,
            currency=None, total=None, type=None,
            reportdate=None, code=None)
        output = self.reader.merge_security_transfer(
            src=src, dest=dest, memo=sentinel.memo)

        self.assertEqual(
            mock_merge_transaction_method.mock_calls,
            [call(type='transfer', fiaccount=self.reader.account,
                  uniqueid=sentinel.fitid1, datetime=sentinel.dttrade1,
                  memo=sentinel.memo, security=sentinel.security1,
                  units=sentinel.units1, fiaccountFrom=self.reader.account,
                  securityFrom=sentinel.security0, unitsFrom=sentinel.units0)])

        self.assertIsInstance(output, list)
        self.assertEqual(len(output), 1)
        self.assertIs(output[0], sentinel.transaction)

    def testGuessSecurity(self):
        pass

    def testMergeAccountTransfer(self):
        pass


class BondMaturityTestCase(CorpActXmlSnippetTestCase, unittest.TestCase):
    def testBondMaturity(self):
        xml = """
        <CorporateActions>
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="BOND" symbol="WMIH 13 03/19/30 LLB6" description="(US929CALLB67) BOND MATURITY FOR USD 1.00000000 PER BOND (WMIH 13 03/19/30 LLB6, WMIH 13 03/19/30 - PARTIAL CALL RED DATE 7/1, 929CALLB6)" conid="27" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="1" reportDate="2016-07-01" dateTime="2016-06-30;202500" amount="-3" proceeds="3" value="0" quantity="-3" fifoPnlRealized="3" mtmPnl="0.6" code="" type="BM" />
        </CorporateActions>
        """
        memo = self.parseXML(xml)
        (security0, securityId0) = self.securities[0]
        corpAct = ParsedCorpAct(
            self.transactions[0], type='BM',
            ticker=security0.ticker, cusip=securityId0.uniqueid,
            secname=security0.name, memo=memo)
        self.reader.treat_as_trade( [corpAct, ], memo=memo)
        trans = self.reader.transactions
        self.assertEqual(len(trans), 1)
        tran = trans.pop()
        self.assertIsInstance(tran, Transaction)
        # self.assertEqual(tran1.uniqueid, None)
        self.assertEqual(tran.datetime, datetime(2016, 6, 30, 20, 25))
        self.assertEqual(tran.type, 'trade')
        self.assertEqual(tran.memo, '(US929CALLB67) BOND MATURITY FOR USD 1.00000000 PER BOND')
        self.assertEqual(tran.currency, 'USD')
        self.assertEqual(tran.cash, Decimal('3'))
        self.assertEqual(tran.fiaccount, self.account)
        self.assertEqual(tran.security, security0)
        self.assertEqual(tran.units, Decimal('-3'))
        self.assertEqual(tran.securityPrice, None)
        self.assertEqual(tran.fiaccountFrom, None)
        self.assertEqual(tran.securityFrom, None)
        self.assertEqual(tran.unitsFrom, None)
        self.assertEqual(tran.securityFromPrice, None)
        self.assertEqual(tran.numerator, None)
        self.assertEqual(tran.denominator, None)
        self.assertEqual(tran.sort, None)


class DelistTestCase(CorpActXmlSnippetTestCase, unittest.TestCase):
    def testDelist(self):
        xml = """
        <CorporateActions>
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="ELAN.CNT" description="(US284CNT9952) DELISTED (ELAN.CNT, ELANDIA INTERNATIONAL INC - CONTRA, 284CNT995)" conid="266" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2013-10-07" dateTime="2013-10-03;202500" amount="0" proceeds="0" value="0" quantity="-56" fifoPnlRealized="0" mtmPnl="0" code="" type="DW" />
        </CorporateActions>
        """
        memo = self.parseXML(xml)
        (security0, securityId0) = self.securities[0]
        corpAct = ParsedCorpAct(
            self.transactions[0], type='DW', ticker='ELAN.CNT', cusip='US284CNT9952',
            secname='ELANDIA INTERNATIONAL INC - CONTRA',
            memo=memo)
        self.reader.treat_as_trade([corpAct, ], memo=memo)
        trans = self.reader.transactions
        self.assertEqual(len(trans), 1)
        tran = trans.pop()
        self.assertIsInstance(tran, Transaction)
        # self.assertEqual(tran.uniqueid, None)
        self.assertEqual(tran.datetime, datetime(2013, 10, 3, 20, 25))
        self.assertEqual(tran.type, 'trade')
        self.assertEqual(memo, '(US284CNT9952) DELISTED')
        self.assertEqual(tran.currency, 'USD')
        self.assertEqual(tran.cash, Decimal('0'))
        self.assertEqual(tran.fiaccount, self.account)
        self.assertEqual(tran.security, security0)
        self.assertEqual(tran.units, Decimal('-56'))
        self.assertEqual(tran.securityPrice, None)
        self.assertEqual(tran.fiaccountFrom, None)
        self.assertEqual(tran.securityFrom, None)
        self.assertEqual(tran.unitsFrom, None)
        self.assertEqual(tran.securityFromPrice, None)
        self.assertEqual(tran.numerator, None)
        self.assertEqual(tran.denominator, None)
        self.assertEqual(tran.sort, None)


class ChangeSecurityTestCase(CorpActXmlSnippetTestCase, unittest.TestCase):
    def testChangeSecurity(self):
        xml = """
        <CorporateActions>
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="EDCI.OLD" description="EDCI(US2683151086) CUSIP/ISIN CHANGE TO (US2683152076) (EDCI.OLD, EDCI HOLDINGS INC, 268315108)" conid="53562481" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2010-12-01" dateTime="2010-11-30;202500" amount="0" proceeds="0" value="0" quantity="-112833" fifoPnlRealized="0" mtmPnl="0" code="" type="IC" />
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="EDCID" description="EDCI(US2683151086) CUSIP/ISIN CHANGE TO (US2683152076) (EDCID, EDCI HOLDINGS INC, 268315207)" conid="81516263" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2010-12-01" dateTime="2010-11-30;202500" amount="0" proceeds="0" value="0" quantity="112833" fifoPnlRealized="0" mtmPnl="0" code="" type="IC" />
        </CorporateActions>
        """
        memo = self.parseXML(xml)
        (security0, securityId0) = self.securities[0]
        (security1, securityId1) = self.securities[1]
        corpAct0 = ParsedCorpAct(
            self.transactions[0], type='IC',
            ticker=security0.ticker, cusip='268315108',
            # ticker=security0.ticker, cusip=securityId0.uniqueid,
            secname=security0.name, memo=memo)
        corpAct1 = ParsedCorpAct(
            self.transactions[1], type='IC',
            ticker=security1.ticker, cusip='268315207',
            # ticker=security1.ticker, cusip=securityId1.uniqueid,
            secname=security1.name, memo=memo)
        self.reader.change_security([corpAct0, corpAct1], memo=memo)
        trans = self.reader.transactions
        self.assertEqual(len(trans), 1)
        tran = trans.pop()
        self.assertIsInstance(tran, Transaction)
        self.session.add(tran)
        # self.assertEqual(tran1.uniqueid, None)
        self.assertEqual(tran.datetime, datetime(2010, 11, 30, 20, 25))
        self.assertEqual(tran.type, 'transfer')
        self.assertEqual(tran.memo, 'EDCI(US2683151086) CUSIP/ISIN CHANGE TO (US2683152076)')
        self.assertEqual(tran.currency, None)
        self.assertEqual(tran.cash, None)
        self.assertEqual(tran.fiaccount, self.account)
        self.assertEqual(tran.security, security1)
        self.assertEqual(tran.units, Decimal('112833'))
        self.assertEqual(tran.securityPrice, None)
        self.assertEqual(tran.fiaccountFrom, self.account)
        self.assertEqual(tran.securityFrom, security0)
        self.assertEqual(tran.unitsFrom, Decimal('-112833'))
        self.assertEqual(tran.securityFromPrice, None)
        self.assertEqual(tran.numerator, None)
        self.assertEqual(tran.denominator, None)
        self.assertEqual(tran.sort, None)


class OversubscribeTestCase(CorpActXmlSnippetTestCase, unittest.TestCase):
    def testOversubscribe(self):
        xml = """
        <CorporateActions>
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="TPHS.OS" description="OVER SUBSCRIBE TPHS.OS (US89656D10OS) AT 6.00 USD (TPHS.OS, TRINITY PLACE HOLDINGS INC - RIGHTS OVERSUBSCRIPTION, 89656D10O)" conid="214128923" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-11-30" dateTime="2015-11-30;194500" amount="90000" proceeds="-90000" value="0" quantity="15000" fifoPnlRealized="0" mtmPnl="-90000" code="" type="OR" />
        </CorporateActions>
        """
        memo = self.parseXML(xml)
        (security0, securityId0) = self.securities[0]
        corpAct0 = ParsedCorpAct(
            self.transactions[0], type='OR', ticker=security0.ticker,
            cusip='89656D10O', secname=security0.name, memo=memo)
        self.reader.treat_as_trade([corpAct0, ], memo=memo)
        trans = self.reader.transactions
        self.assertEqual(len(trans), 1)
        tran = trans.pop()
        self.assertIsInstance(tran, Transaction)
        # self.assertEqual(tran.uniqueid, None)
        self.assertEqual(tran.datetime, datetime(2015, 11, 30, 19, 45))
        self.assertEqual(tran.type, 'trade')
        self.assertEqual(tran.memo, 'OVER SUBSCRIBE TPHS.OS (US89656D10OS) AT 6.00 USD')
        self.assertEqual(tran.currency, 'USD')
        self.assertEqual(tran.cash, Decimal('-90000'))
        self.assertEqual(tran.fiaccount, self.account)
        self.assertEqual(tran.security, security0)
        self.assertEqual(tran.units, Decimal('15000'))
        self.assertEqual(tran.securityPrice, None)
        self.assertEqual(tran.fiaccountFrom, None)
        self.assertEqual(tran.securityFrom, None)
        self.assertEqual(tran.unitsFrom, None)
        self.assertEqual(tran.securityFromPrice, None)
        self.assertEqual(tran.numerator, None)
        self.assertEqual(tran.denominator, None)
        self.assertEqual(tran.sort, None)


class RightsIssueTestCase(CorpActXmlSnippetTestCase, unittest.TestCase):
    def testIssueRights(self):
        xml = """
        <CorporateActions>
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="EUR" fxRateToBase="1.141" assetCategory="STK" symbol="AMP.D" description="AMP(ES0109260531) SUBSCRIBABLE RIGHTS ISSUE  1 FOR 1 (AMP.D, AMPER SA - BONUS RTS, ES0609260924)" conid="194245312" securityID="ES0609260924" securityIDType="ISIN" cusip="" isin="ES0609260924" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-05-14" dateTime="2015-05-13;202500" amount="0" proceeds="0" value="0" quantity="70576" fifoPnlRealized="0" mtmPnl="0" code="" type="RI" />
        </CorporateActions>
        """
        memo = self.parseXML(xml)
        (security0, securityId0) = self.securities[0]
        security1 = Security.merge(self.session, name='AMPER SA', ticker='AMP',
                                   uniqueidtype='CONID', uniqueid='917393')
        corpAct0 = ParsedCorpAct(
            self.transactions[0], type='RI',
            ticker=security0.ticker, cusip='ES0609260924',
            secname=security0.name, memo=memo)
        self.reader.issue_rights([corpAct0, ], memo=memo)
        trans = self.reader.transactions
        self.assertEqual(len(trans), 1)
        tran = trans.pop()
        self.assertIsInstance(tran, Transaction)
        # self.assertEqual(tran.uniqueid, None)
        self.assertEqual(tran.datetime, datetime(2015, 5, 13, 20, 25))
        self.assertEqual(tran.type, 'spinoff')
        self.assertEqual(tran.memo, 'AMP(ES0109260531) SUBSCRIBABLE RIGHTS ISSUE  1 FOR 1')
        self.assertEqual(tran.currency, None)
        self.assertEqual(tran.cash, None)
        self.assertEqual(tran.fiaccount, self.account)
        self.assertEqual(tran.security, security0)
        self.assertEqual(tran.units, Decimal('70576'))
        self.assertEqual(tran.securityPrice, None)
        self.assertEqual(tran.fiaccountFrom, None)
        self.assertEqual(tran.securityFrom, security1)
        self.assertEqual(tran.unitsFrom, None)
        self.assertEqual(tran.securityFromPrice, None)
        self.assertEqual(tran.numerator, 1)
        self.assertEqual(tran.denominator, 1)
        self.assertEqual(tran.sort, None)


class SplitTestCase(CorpActXmlSnippetTestCase, unittest.TestCase):
    @patch.object(FlexStatementReader, 'merge_transaction', return_value=sentinel.transaction)
    def testMergeSplit(self, mock_merge_transaction_method):
        self.securities[(sentinel.uniqueidtype, sentinel.uniqueid)] = sentinel.security
        corpAct = parser.CorporateAction(
            fitid=sentinel.fitid, dttrade=sentinel.dttrade, memo=sentinel.memo,
            uniqueidtype=sentinel.uniqueidtype, uniqueid=sentinel.uniqueid,
            units=sentinel.units, currency=None, total=None, type=None,
            reportdate=None, code=None)
        output = self.reader.merge_split(
            corpAct, numerator=sentinel.numerator,
            denominator=sentinel.denominator, memo=None)
        self.assertEqual(
            mock_merge_transaction_method.mock_calls,
            [call(type='split', fiaccount=self.reader.account,
                  uniqueid=sentinel.fitid, datetime=sentinel.dttrade,
                  memo=sentinel.memo, security=sentinel.security,
                  numerator=sentinel.numerator,
                  denominator=sentinel.denominator, units=sentinel.units)])
        self.assertIsInstance(output, list)
        self.assertEqual(len(output), 1)
        self.assertIs(output[0], sentinel.transaction)

    @patch.object(FlexStatementReader, 'merge_transaction', return_value=sentinel.transaction)
    def testMergeSplitOverrideMemo(self, mock_merge_transaction_method):
        self.securities[(sentinel.uniqueidtype, sentinel.uniqueid)] = sentinel.security
        corpAct = parser.CorporateAction(
            fitid=sentinel.fitid, dttrade=sentinel.dttrade, memo=sentinel.memo,
            uniqueidtype=sentinel.uniqueidtype, uniqueid=sentinel.uniqueid,
            units=sentinel.units, currency=None, total=None, type=None,
            reportdate=None, code=None)
        output = self.reader.merge_split(
            corpAct, numerator=sentinel.numerator,
            denominator=sentinel.denominator, memo=sentinel.override_memo)
        self.assertEqual(
            mock_merge_transaction_method.mock_calls,
            [call(type='split', fiaccount=self.reader.account,
                  uniqueid=sentinel.fitid, datetime=sentinel.dttrade,
                  memo=sentinel.override_memo, security=sentinel.security,
                  numerator=sentinel.numerator,
                  denominator=sentinel.denominator, units=sentinel.units)])
        self.assertIsInstance(output, list)
        self.assertEqual(len(output), 1)
        self.assertIs(output[0], sentinel.transaction)

    def testSplitWithCusipChange(self):
        xml = """
        <CorporateActions>
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="VXX" description="VXX(US06742E7114) SPLIT 1 FOR 4 (VXX, IPATH S&amp;P 500 VIX S/T FU ETN, 06740Q252)" conid="242500577" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2016-08-09" dateTime="2016-08-08;202500" amount="0" proceeds="0" value="0" quantity="-4250" fifoPnlRealized="0" mtmPnl="0" code="" type="RS" />
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="VXX.OLD" description="VXX(US06742E7114) SPLIT 1 FOR 4 (VXX.OLD, IPATH S&amp;P 500 VIX S/T FU ETN, 06742E711)" conid="137935324" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2016-08-09" dateTime="2016-08-08;202500" amount="0" proceeds="0" value="0" quantity="17000" fifoPnlRealized="0" mtmPnl="0" code="" type="RS" />
        </CorporateActions>
        """
        memo = self.parseXML(xml)
        (security0, securityId0) = self.securities[0]
        (security1, securityId1) = self.securities[1]
        corpAct0 = ParsedCorpAct(
            self.transactions[0], type='RS', ticker=security0.ticker,
            cusip='06740Q252', secname=security0.name, memo=memo)
        corpAct1 = ParsedCorpAct(
            self.transactions[1], type='RS', ticker='VXX.OLD',
            cusip='06742E711', secname=security1.name, memo=memo)
        self.reader.split([corpAct0, corpAct1], memo=memo)
        trans = self.reader.transactions
        self.assertEqual(len(trans), 1)
        tran = trans.pop()
        self.assertIsInstance(tran, Transaction)
        # self.assertEqual(tran.uniqueid, None)
        self.assertEqual(tran.datetime, datetime(2016, 8, 8, 20, 25))
        self.assertEqual(tran.type, 'transfer')
        self.assertEqual(tran.memo, 'VXX(US06742E7114) SPLIT 1 FOR 4')
        self.assertEqual(tran.currency, None)
        self.assertEqual(tran.cash, None)
        self.assertEqual(tran.fiaccount, self.account)
        self.assertEqual(tran.security, security0)
        self.assertEqual(tran.units, Decimal('-4250'))
        self.assertEqual(tran.securityPrice, None)
        self.assertEqual(tran.fiaccountFrom, self.account)
        self.assertEqual(tran.securityFrom, security1)
        self.assertEqual(tran.unitsFrom, Decimal('17000'))
        self.assertEqual(tran.securityFromPrice, None)
        self.assertEqual(tran.numerator, None)
        self.assertEqual(tran.denominator, None)
        self.assertEqual(tran.sort, None)


class StockDividendTestCase(CorpActXmlSnippetTestCase, unittest.TestCase):
    def testStockDividend(self):
        xml = """
        <CorporateActions>
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="MFCAF" description="MFCAF(P64605101) STOCK DIVIDEND 1 FOR 11 (MFCAF, MASS FINANCIAL CORP-CL A, P64605101)" conid="37839182" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2010-01-27" dateTime="2009-12-23;202500" amount="0" proceeds="0" value="10134.54545539" quantity="1090.909091" fifoPnlRealized="0" mtmPnl="10134.5455" code="" type="SD" />
        </CorporateActions>
        """
        memo = self.parseXML(xml)
        (security0, securityId0) = self.securities[0]
        corpAct0 = ParsedCorpAct(
            self.transactions[0],
            type='SD', ticker=security0.ticker, cusip='P64605101',
            secname=security0.name, memo=memo)
        self.reader.stock_dividend([corpAct0, ], memo=memo)
        trans = self.reader.transactions
        self.assertEqual(len(trans), 1)
        tran = trans.pop()
        self.assertIsInstance(tran, Transaction)
        # self.assertEqual(tran.uniqueid, None)
        self.assertEqual(tran.datetime, datetime(2009, 12, 23, 20, 25))
        self.assertEqual(tran.type, 'split')
        self.assertEqual(tran.memo, 'MFCAF(P64605101) STOCK DIVIDEND 1 FOR 11')
        self.assertEqual(tran.currency, None)
        self.assertEqual(tran.cash, None)
        self.assertEqual(tran.fiaccount, self.account)
        self.assertEqual(tran.security, security0)
        self.assertEqual(tran.units, Decimal('1090.909091'))
        self.assertEqual(tran.securityPrice, None)
        self.assertEqual(tran.fiaccountFrom, None)
        self.assertEqual(tran.securityFrom, None)
        self.assertEqual(tran.unitsFrom, None)
        self.assertEqual(tran.securityFromPrice, None)
        # Split numerator = dividend numerator + dividend denominator
        self.assertEqual(tran.numerator, 12)
        self.assertEqual(tran.denominator, 11)
        self.assertEqual(tran.sort, None)


class SpinoffTestCase(CorpActXmlSnippetTestCase, unittest.TestCase):
    @patch.object(FlexStatementReader, 'merge_transaction', return_value=sentinel.transaction)
    def testSpinoff(self, mock_merge_transaction_method):
        self.securities[(sentinel.uniqueidtype, sentinel.uniqueid)] = sentinel.security
        corpAct = parser.CorporateAction(
            fitid=sentinel.fitid, dttrade=sentinel.dttrade, memo=sentinel.memo,
            uniqueidtype=sentinel.uniqueidtype, uniqueid=sentinel.uniqueid,
            units=sentinel.units, currency=None, total=None, type=None,
            reportdate=None, code=None)
        output = self.reader.merge_spinoff(
            corpAct, securityFrom=sentinel.securityFrom,
            numerator=sentinel.numerator, denominator=sentinel.denominator,
            memo=None)
        self.assertEqual(
            mock_merge_transaction_method.mock_calls,
            [call(type='spinoff', fiaccount=self.reader.account,
                  uniqueid=sentinel.fitid, datetime=sentinel.dttrade,
                  memo=sentinel.memo, security=sentinel.security,
                  numerator=sentinel.numerator,
                  denominator=sentinel.denominator, units=sentinel.units,
                  securityFrom=sentinel.securityFrom)])
        self.assertIsInstance(output, list)
        self.assertEqual(len(output), 1)
        self.assertIs(output[0], sentinel.transaction)

    def testMergeSpinoff(self):
        """
        Test merge_spinoff()
        """
        xml = """
        <CorporateActions>
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="GYRO.NTS2" description="GYRO.NOTE(US403NOTE034) SPINOFF  1 FOR 40 (GYRO.NTS2, GYRODYNE CO OF AMERICA INC - GLOBAL DIVIDEND NOTE - PIK, 403PIK103)" conid="160689243" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-08-19" dateTime="2015-06-12;202500" amount="0" proceeds="0" value="0" quantity="1837.125" fifoPnlRealized="0" mtmPnl="0" code="" type="SO" />
        </CorporateActions>
        """
        memo = self.parseXML(xml)
        corpAct = self.transactions[0]
        (security0, securityId0) = self.securities[0]
        security1 = Security.merge(
            self.session,
            name='GYRODYNE CO OF AMERICA INC - GLOBAL DIVIDEND NOTE - PIK',
            ticker='GYRO.NTS2', uniqueidtype='CONID', uniqueid='160689243')
        self.reader.merge_spinoff(corpAct, securityFrom=security1,
                                  numerator=1, denominator=40, memo=memo)
        self.assertEqual(len(self.reader.transactions), 1)
        tran = self.reader.transactions[0]
        self.assertIsInstance(tran, Transaction)
        self.assertEqual(tran.fiaccount, self.account)
        # self.assertEqual(tran.uniqueid, None)
        self.assertEqual(tran.datetime, datetime(2015, 6, 12, 20, 25))
        self.assertEqual(tran.type, 'spinoff')
        self.assertEqual(tran.memo, 'GYRO.NOTE(US403NOTE034) SPINOFF  1 FOR 40')
        self.assertEqual(tran.currency, None)
        self.assertEqual(tran.cash, None)
        self.assertEqual(tran.fiaccount, self.account)
        self.assertEqual(tran.security, security1)
        self.assertEqual(tran.units, Decimal('1837.125'))
        self.assertEqual(tran.securityPrice, None)
        self.assertEqual(tran.fiaccountFrom, None)
        self.assertEqual(tran.securityFrom, security0)
        self.assertEqual(tran.unitsFrom, None)
        self.assertEqual(tran.securityFromPrice, None)
        self.assertEqual(tran.numerator, 1)
        self.assertEqual(tran.denominator, 40)
        self.assertEqual(tran.sort, None)


class SubscribeRightsTestCase(CorpActXmlSnippetTestCase, unittest.TestCase):
    def testSubscribeRightsBadIsinTo(self):
        xml = """
        <CorporateActions>
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="TPHS.EX" description="TPHS.RTS (US8969940274) SUBSCRIBES TO () (TPHS.EX, TRINITY PLACE HOLDINGS INC - RIGHTS SUBSCRIPTION, 89656D10E)" conid="214128916" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-11-30" dateTime="2015-11-30;194500" amount="0" proceeds="0" value="23034" quantity="3839" fifoPnlRealized="0" mtmPnl="0" code="" type="SR" />
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="TPHS.RTS" description="TPHS.RTS (US8969940274) SUBSCRIBES TO () (TPHS.RTS, TRINITY PLACE HOLDINGS INC - RIGHTS, 896994027)" conid="212130559" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-11-30" dateTime="2015-11-30;194500" amount="23034" proceeds="-23034" value="0" quantity="-3839" fifoPnlRealized="0" mtmPnl="0" code="" type="SR" />
        </CorporateActions>
        """
        memo = self.parseXML(xml)
        (security0, securityId0) = self.securities[0]
        (security1, securityId1) = self.securities[1]
        corpAct0 = ParsedCorpAct(
            self.transactions[0],
            type='SR', ticker=security0.ticker, cusip='89656D10E',
            secname=security0.name, memo=memo)
        corpAct1 = ParsedCorpAct(
            self.transactions[1],
            type='SR', ticker=security1.ticker, cusip='896994027',
            secname=security1.name, memo=memo)
        self.reader.subscribe_rights([corpAct0, corpAct1], memo=memo)
        trans = self.reader.transactions
        self.assertEqual(len(trans), 1)
        tran = trans.pop()
        self.assertIsInstance(tran, Transaction)
        self.assertEqual(tran.fiaccount, self.account)
        # self.assertEqual(tran.uniqueid, None)
        self.assertEqual(tran.datetime, datetime(2015, 11, 30, 19, 45))
        self.assertEqual(tran.type, 'exercise')
        self.assertEqual(tran.memo, 'TPHS.RTS (US8969940274) SUBSCRIBES TO ()')
        self.assertEqual(tran.currency, 'USD')
        self.assertEqual(tran.cash, Decimal('-23034'))
        self.assertEqual(tran.fiaccount, self.account)
        self.assertEqual(tran.security, security0)
        self.assertEqual(tran.units, Decimal('3839'))
        self.assertEqual(tran.securityPrice, None)
        self.assertEqual(tran.fiaccountFrom, None)
        self.assertEqual(tran.securityFrom, security1)
        self.assertEqual(tran.unitsFrom, Decimal('-3839'))
        self.assertEqual(tran.securityFromPrice, None)
        self.assertEqual(tran.numerator, None)
        self.assertEqual(tran.denominator, None)
        self.assertEqual(tran.sort, None)


class MergerTestCase(CorpActXmlSnippetTestCase, unittest.TestCase):
    def testMergeReorgBadIsinFrom(self):
        """
        Test merge_reorg() where source ISIN can't be parsed from memo
        """
        xml = """
        <CorporateActions>
        <CorporateAction accountId="5678" acctAlias="Test account" amount="0" assetCategory="STK" code="" conid="135944283" currency="EUR" cusip="" dateTime="2013-10-04;202500" description="AMP.RSTD(135893865) MERGED(Acquisition)  WITH AMP.REST 1 FOR 1 (AMP.REST, AMPER SA - RESTRICTED, ES010RSTD531)" expiry="" fifoPnlRealized="0" fxRateToBase="1.3582" isin="ES010RSTD531" issuer="" model="" mtmPnl="-0.3168" multiplier="1" principalAdjustFactor="" proceeds="0" putCall="" quantity="70576.0000" reportDate="2013-10-07" securityID="ES010RSTD531" securityIDType="ISIN" strike="" symbol="AMP.REST" type="TC" underlyingConid="" underlyingSymbol="" value="90337.2800" />
        <CorporateAction accountId="5678" acctAlias="Test account" amount="0" assetCategory="STK" code="" conid="135893865" currency="USD" cusip="" dateTime="2013-10-04;202500" description="AMP.RSTD(135893865) MERGED(Acquisition)  WITH AMP.REST 1 FOR 1 (AMP.RSTD, AMPER SA - RESTRICTED, ES010RSTD531)" expiry="" fifoPnlRealized="0" fxRateToBase="1" isin="ES010RSTD531" issuer="" model="" mtmPnl="0" multiplier="1" principalAdjustFactor="" proceeds="0" putCall="" quantity="-70576.0000" reportDate="2013-10-07" securityID="ES010RSTD531" securityIDType="ISIN" strike="" symbol="AMP.RSTD" type="TC" underlyingConid="" underlyingSymbol="" value="0" />
        </CorporateActions>
        """
        memo = self.parseXML(xml)
        (security0, securityId0) = self.securities[0]
        (security1, securityId1) = self.securities[1]
        corpAct0 = ParsedCorpAct(
            self.transactions[0], type='TC',
            ticker=security0.ticker, cusip='ES010RSTD531',
            secname=security0.name, memo=memo)
        corpAct1 = ParsedCorpAct(
            self.transactions[1], type='TC',
            ticker=security1.ticker, cusip='ES010RSTD531',
            secname=security1.name, memo=memo)
        match = reader.kindMergerRE.match(memo)
        self.reader.merge_reorg([corpAct0, corpAct1], match, memo=memo)
        trans = self.reader.transactions
        self.assertEqual(len(trans), 1)
        tran = trans.pop()
        self.assertIsInstance(tran, Transaction)
        self.assertEqual(tran.fiaccount, self.account)
        # self.assertEqual(tran.uniqueid, None)
        self.assertEqual(tran.datetime, datetime(2013, 10, 4, 20, 25))
        self.assertEqual(tran.type, 'transfer')
        self.assertEqual(tran.memo, 'AMP.RSTD(135893865) MERGED(Acquisition)  WITH AMP.REST 1 FOR 1')
        self.assertEqual(tran.currency, None)
        self.assertEqual(tran.cash, None)
        self.assertEqual(tran.fiaccount, self.account)
        self.assertEqual(tran.security, security0)
        self.assertEqual(tran.units, Decimal('70576'))
        self.assertEqual(tran.securityPrice, None)
        self.assertEqual(tran.fiaccountFrom, self.account)
        self.assertEqual(tran.securityFrom, security1)
        self.assertEqual(tran.unitsFrom, Decimal('-70576'))
        self.assertEqual(tran.securityFromPrice, None)
        self.assertEqual(tran.numerator, None)
        self.assertEqual(tran.denominator, None)
        self.assertEqual(tran.sort, None)

    def testMergerCash(self):
        """
        Test merger() for all-cash merger
        """
        xml = """
        <CorporateActions>
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="WMI HLDGS CORP 13% SEC LIEN NT 03/16/2030 - PARTIAL CALL" description="92CALLAB6(US92CALLAB67) MERGED(Partial Call)  FOR USD 1.00000000 PER SHARE (WMI HLDGS CORP 13% SEC LIEN NT 03/16/2030 - PARTIAL CALL, WMI HLDGS CORP 13% SEC LIEN NT 03/16/2030 - PARTIAL CALL, 92CALLAB6)" conid="196610660" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2017-03-31" dateTime="2017-03-30;202500" amount="-1" proceeds="1" value="-93" quantity="-1" fifoPnlRealized="1" mtmPnl="-92" code="" type="TC" />
        </CorporateActions>
        """
        memo = self.parseXML(xml)
        (security0, securityId0) = self.securities[0]
        corpAct0 = ParsedCorpAct(self.transactions[0], type='TC', ticker=security0.ticker,
                                 cusip='92CALLAB6', secname=security0.name,
                                 memo=memo)
        self.reader.merger([corpAct0, ], memo=memo)
        trans = self.reader.transactions
        self.assertEqual(len(trans), 1)
        tran = trans.pop()
        self.assertIsInstance(tran, Transaction)
        self.assertEqual(tran.fiaccount, self.account)
        # self.assertEqual(tran.uniqueid, None)
        self.assertEqual(tran.datetime, datetime(2017, 3, 30, 20, 25))
        self.assertEqual(tran.type, 'trade')
        self.assertEqual(tran.memo, '92CALLAB6(US92CALLAB67) MERGED(Partial Call)  FOR USD 1.00000000 PER SHARE')
        self.assertEqual(tran.currency, 'USD')
        self.assertEqual(tran.cash, Decimal('1'))
        self.assertEqual(tran.fiaccount, self.account)
        self.assertEqual(tran.security, security0)
        self.assertEqual(tran.units, Decimal('-1'))
        self.assertEqual(tran.securityPrice, None)
        self.assertEqual(tran.fiaccountFrom, None)
        self.assertEqual(tran.securityFrom, None)
        self.assertEqual(tran.unitsFrom, None)
        self.assertEqual(tran.securityFromPrice, None)
        self.assertEqual(tran.numerator, None)
        self.assertEqual(tran.denominator, None)
        self.assertEqual(tran.sort, None)

    def testMergerKind(self):
        """
        Test merger() for merger in kind
        """
        xml = """
        <CorporateActions>
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="TPHS" description="TPHS.EX(US89656R10EX) MERGED(Voluntary Offer Allocation)  WITH US89656D1019 1 FOR 1 (TPHS, TRINITY PLACE HOLDINGS INC, 89656D101)" conid="113775558" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2017-04-17" dateTime="2017-04-12;202500" amount="0" proceeds="0" value="18256.75" quantity="2575" fifoPnlRealized="0" mtmPnl="0" code="" type="TC" />
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="TPHS.EX" description="TPHS.EX(US89656R10EX) MERGED(Voluntary Offer Allocation)  WITH US89656D1019 1 FOR 1 (TPHS.EX, TRINITY PLACE HOLDINGS INC - RIGHTS SUBSCRIPTION, 89656R10E)" conid="271739961" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2017-04-17" dateTime="2017-04-12;202500" amount="0" proceeds="0" value="-18231" quantity="-2575" fifoPnlRealized="0" mtmPnl="25.75" code="" type="TC" />
        </CorporateActions>
        """
        memo = self.parseXML(xml)
        (security0, securityId0) = self.securities[0]
        (security1, securityId1) = self.securities[1]
        corpAct0 = ParsedCorpAct(
            self.transactions[0], type='TC',
            ticker=security0.ticker, cusip='89656D101',
            secname=security0.name, memo=memo)
        corpAct1 = ParsedCorpAct(
            self.transactions[1],
            type='TC', ticker=security1.ticker, cusip='89656R10E',
            secname=security1.name, memo=memo)
        self.reader.merger([corpAct0, corpAct1], memo=memo)
        trans = self.reader.transactions
        self.assertEqual(len(trans), 1)
        tran = trans.pop()
        self.assertIsInstance(tran, Transaction)
        self.assertEqual(tran.fiaccount, self.account)
        # self.assertEqual(tran.uniqueid, None)
        self.assertEqual(tran.datetime, datetime(2017, 4, 12, 20, 25))
        self.assertEqual(tran.type, 'transfer')
        self.assertEqual(tran.memo, 'TPHS.EX(US89656R10EX) MERGED(Voluntary Offer Allocation)  WITH US89656D1019 1 FOR 1')
        self.assertEqual(tran.currency, None)
        self.assertEqual(tran.cash, None)
        self.assertEqual(tran.fiaccount, self.account)
        self.assertEqual(tran.security, security0)
        self.assertEqual(tran.units, Decimal('2575'))
        self.assertEqual(tran.securityPrice, None)
        self.assertEqual(tran.fiaccountFrom, self.account)
        self.assertEqual(tran.securityFrom, security1)
        self.assertEqual(tran.unitsFrom, Decimal('-2575'))
        self.assertEqual(tran.securityFromPrice, None)
        self.assertEqual(tran.numerator, None)
        self.assertEqual(tran.denominator, None)
        self.assertEqual(tran.sort, None)

    def testMergerCashAndKind(self):
        """
        Test merger() for cash and multiple securities received in kind
        """
        xml = """
        <CorporateActions>
        <CorporateAction accountId="5678" acctAlias="Test account" amount="-16.5" assetCategory="STK" code="" conid="106619225" currency="USD" cusip="" dateTime="2012-04-16;202500" description="DIMEQ.TMP(US254TMP9913) CASH and STOCK MERGER (Voluntary Offer Allocation) WMIH 1146667 FOR 10000000 (DIMEQ.TMP, DIME BANCORP WT - TEMP, 254TMP991)" expiry="" fifoPnlRealized="0" fxRateToBase="1" isin="" issuer="" model="" mtmPnl="10250.653" multiplier="1" principalAdjustFactor="" proceeds="16.5" putCall="" quantity="-150000" reportDate="2012-04-26" securityID="" securityIDType="" strike="" symbol="DIMEQ.TMP" type="TC" underlyingConid="" underlyingSymbol="" value="0" />
        <CorporateAction accountId="5678" acctAlias="Test account" amount="0" assetCategory="STK" code="" conid="105068604" currency="USD" cusip="" dateTime="2012-04-16;202500" description="DIMEQ.TMP(US254TMP9913) CASH and STOCK MERGER (Voluntary Offer Allocation) WMIH 1146667 FOR 10000000 (WMIH, WMI HOLDINGS CORP, 92936P100)" expiry="" fifoPnlRealized="0" fxRateToBase="1" isin="" issuer="" model="" mtmPnl="0" multiplier="1" principalAdjustFactor="" proceeds="0" putCall="" quantity="17200.005" reportDate="2012-04-26" securityID="" securityIDType="" strike="" symbol="WMIH" type="TC" underlyingConid="" underlyingSymbol="" value="10234.002975" />
        <CorporateAction accountId="5678" acctAlias="Test account" amount="0" assetCategory="STK" code="" conid="105951142" currency="USD" cusip="" dateTime="2012-04-16;202500" description="DIMEQ.TMP(US254TMP9913) CASH and STOCK MERGER (Voluntary Offer Allocation) WMIH 1146667 FOR 10000000 (DIME.ESCR, DIME BANCORP WT - ESCROW SHARES, 254ESC890)" expiry="" fifoPnlRealized="0" fxRateToBase="1" isin="" issuer="" model="" mtmPnl="0" multiplier="1" principalAdjustFactor="" proceeds="0" putCall="" quantity="150000" reportDate="2012-04-26" securityID="" securityIDType="" strike="" symbol="DIME.ESCR" type="TC" underlyingConid="" underlyingSymbol="" value="0.15" />
        </CorporateActions>
        """
        memo = self.parseXML(xml)
        (security0, securityId0) = self.securities[0]
        (security1, securityId1) = self.securities[1]
        (security2, securityId2) = self.securities[2]
        corpAct0 = ParsedCorpAct(
            self.transactions[0], type='TC', ticker=security0.ticker, cusip='254TMP991',
            secname=security0.name, memo=memo)
        corpAct1 = ParsedCorpAct(
            self.transactions[1], type='TC', ticker=security1.ticker, cusip='92936P100',
            secname=security1.name, memo=memo)
        corpAct2 = ParsedCorpAct(
            self.transactions[2], type='TC', ticker=security2.ticker, cusip='254ESC890',
            secname=security2.name, memo=memo)
        self.reader.merger([corpAct0, corpAct1, corpAct2], memo=memo)
        trans = self.reader.transactions

        self.assertEqual(len(trans), 3)
        tran0, tran1, tran2 = trans

        self.assertIsInstance(tran0, Transaction)
        # self.assertEqual(tran0.uniqueid, None)
        self.assertEqual(tran0.datetime, datetime(2012, 4, 16, 20, 25))
        self.assertEqual(tran0.type, 'returnofcapital')
        self.assertEqual(tran0.memo, 'DIMEQ.TMP(US254TMP9913) CASH and STOCK MERGER (Voluntary Offer Allocation) WMIH 1146667 FOR 10000000')
        self.assertEqual(tran0.currency, 'USD')
        self.assertEqual(tran0.cash, Decimal('16.5'))
        self.assertEqual(tran0.fiaccount, self.account)
        self.assertEqual(tran0.security, security0)
        self.assertEqual(tran0.units, None)
        self.assertEqual(tran0.securityPrice, None)
        self.assertEqual(tran0.fiaccountFrom, None)
        self.assertEqual(tran0.securityFrom, None)
        self.assertEqual(tran0.unitsFrom, None)
        self.assertEqual(tran0.securityFromPrice, None)
        self.assertEqual(tran0.numerator, None)
        self.assertEqual(tran0.denominator, None)
        self.assertEqual(tran0.sort, None)

        self.assertIsInstance(tran1, Transaction)
        # self.assertEqual(tran2.uniqueid, None)
        self.assertEqual(tran1.datetime, datetime(2012, 4, 16, 20, 25))
        self.assertEqual(tran1.type, 'transfer')
        self.assertEqual(tran1.memo, 'DIMEQ.TMP(US254TMP9913) CASH and STOCK MERGER (Voluntary Offer Allocation) WMIH 1146667 FOR 10000000')
        self.assertEqual(tran1.currency, None)
        self.assertEqual(tran1.cash, None)
        self.assertEqual(tran1.fiaccount, self.account)
        self.assertEqual(tran1.security, security1)
        self.assertEqual(tran1.units, Decimal('17200.005'))
        self.assertEqual(tran1.securityPrice, None)
        self.assertEqual(tran1.fiaccountFrom, self.account)
        self.assertEqual(tran1.securityFrom, security0)
        self.assertEqual(tran1.unitsFrom, Decimal('-150000'))
        self.assertEqual(tran1.securityFromPrice, None)
        self.assertEqual(tran1.numerator, None)
        self.assertEqual(tran1.denominator, None)
        self.assertEqual(tran1.sort, None)

        self.assertIsInstance(tran2, Transaction)
        # self.assertEqual(tran1.uniqueid, None)
        self.assertEqual(tran2.datetime, datetime(2012, 4, 16, 20, 25))
        self.assertEqual(tran2.type, 'spinoff')
        self.assertEqual(tran2.memo, 'DIMEQ.TMP(US254TMP9913) CASH and STOCK MERGER (Voluntary Offer Allocation) WMIH 1146667 FOR 10000000')
        self.assertEqual(tran2.currency, None)
        self.assertEqual(tran2.cash, None)
        self.assertEqual(tran2.fiaccount, self.account)
        self.assertEqual(tran2.security, security2)
        self.assertEqual(tran2.units, Decimal('150000'))
        self.assertEqual(tran2.securityPrice, None)
        self.assertEqual(tran2.fiaccountFrom, None)
        self.assertEqual(tran2.securityFrom, security0)
        self.assertEqual(tran2.unitsFrom, None)
        self.assertEqual(tran2.securityFromPrice, None)
        self.assertEqual(tran2.numerator, Decimal('150000'))
        self.assertEqual(tran2.denominator, Decimal('150000'))
        self.assertEqual(tran2.sort, None)


class TenderTestCase(CorpActXmlSnippetTestCase, unittest.TestCase):
    def testTender(self):
        xml = """
        <CorporateActions>
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="NTP" description="NTP(G63907102) TENDERED TO G63990272 1 FOR 1 (NTP, NAM TAI PROPERTY INC, VGG639071023)" conid="148502652" securityID="VGG639071023" securityIDType="ISIN" cusip="" isin="VGG639071023" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-09-04" dateTime="2015-09-04;194500" amount="0" proceeds="0" value="0" quantity="-60996" fifoPnlRealized="0" mtmPnl="0" code="" type="TO" />
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="NTP.TEN" description="NTP(G63907102) TENDERED TO G63990272 1 FOR 1 (NTP.TEN, NAM TAI PROPERTY INC - TENDER, VGG639902722)" conid="205921721" securityID="VGG639902722" securityIDType="ISIN" cusip="" isin="VGG639902722" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-09-04" dateTime="2015-09-04;194500" amount="0" proceeds="0" value="0" quantity="60996" fifoPnlRealized="0" mtmPnl="0" code="" type="TO" />
        </CorporateActions>
        """
        memo = self.parseXML(xml)
        (security0, securityId0) = self.securities[0]
        (security1, securityId1) = self.securities[1]
        corpAct0 = ParsedCorpAct(
            self.transactions[0], type='TO', ticker=security0.ticker,
            cusip='VGG639071023', secname=security0.name, memo=memo)
        corpAct1 = ParsedCorpAct(
            self.transactions[1], type='TO', ticker=security1.ticker,
            cusip='VGG639902722', secname=security1.name, memo=memo)
        self.reader.tender([corpAct0, corpAct1], memo=memo)
        trans = self.reader.transactions
        self.assertEqual(len(trans), 1)
        tran = trans.pop()
        self.assertIsInstance(tran, Transaction)
        # self.assertEqual(tran1.uniqueid, None)
        self.assertEqual(tran.datetime, datetime(2015, 9, 4, 19, 45))
        self.assertEqual(tran.type, 'transfer')
        self.assertEqual(tran.memo, 'NTP(G63907102) TENDERED TO G63990272 1 FOR 1')
        self.assertEqual(tran.currency, None)
        self.assertEqual(tran.cash, None)
        self.assertEqual(tran.fiaccount, self.account)
        self.assertEqual(tran.security, security1)
        self.assertEqual(tran.units, Decimal('60996'))
        self.assertEqual(tran.securityPrice, None)
        self.assertEqual(tran.fiaccountFrom, self.account)
        self.assertEqual(tran.securityFrom, security0)
        self.assertEqual(tran.unitsFrom, Decimal('-60996'))
        self.assertEqual(tran.securityFromPrice, None)
        self.assertEqual(tran.numerator, None)
        self.assertEqual(tran.denominator, None)
        self.assertEqual(tran.sort, None)


class OptionsExercisTestCase(DatabaseTest, unittest.TestCase):
    def testDoOptionsExercises(self):
        pass

if __name__ == '__main__':
    unittest.main(verbosity=3)
