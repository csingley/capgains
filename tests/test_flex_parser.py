# coding: utf-8
"""Unit tests for capgains.flex.parser.
"""
import unittest
from unittest.mock import patch, sentinel
import datetime
from decimal import Decimal
import xml.etree.ElementTree as ET
import os

import ibflex

from capgains import flex, models
from capgains.flex import parser

from common import setUpModule, tearDownModule, XmlSnippetMixin


class FlexParserTestCase(unittest.TestCase):
    def testParseAccount(self):
        acctinfo = ibflex.Types.AccountInformation(accountId="12345", currency="EUR")
        acctinfo = parser.parse_acctinfo(acctinfo)
        self.assertIsInstance(acctinfo, flex.Types.Account)
        self.assertEqual(acctinfo.acctid, "12345")
        self.assertEqual(acctinfo.brokerid, "4705")
        self.assertEqual(acctinfo.name, None)
        self.assertEqual(acctinfo.currency, "EUR")

    @patch("capgains.flex.parser.parse_security")
    def testParseSecurities(self, mock_parse_security):
        """parse_securities() chains all instances returned by parse_security().
        """
        mock_parse_security.return_value = [sentinel.SECURITY0, sentinel.SECURITY1]
        secinfos = (
            ibflex.Types.SecurityInfo(
                description="Widgets Inc."
            ),
            ibflex.Types.SecurityInfo(
                description="Veeblefetzer Inc."
            ),
        )
        secs = parser.parse_securities(secinfos)
        self.assertEqual(
            secs,
            [
                sentinel.SECURITY0,
                sentinel.SECURITY1,
                sentinel.SECURITY0,
                sentinel.SECURITY1
            ]
        )

    def testParseSecurity(self):
        """parse_security() returns multiple Security instances
        (1 per unique identifier in the input).
        """
        secinfo = ibflex.Types.SecurityInfo(
            conid="54321",
            cusip="1A2B3C",
            symbol="XYZ",
            description="Widgets Inc."
        )

        secs = parser.parse_security(secinfo)
        self.assertIsInstance(secs, list)
        self.assertEqual(len(secs), 2)
        sec0, sec1 = secs[:]

        self.assertIsInstance(sec0, flex.Types.Security)
        self.assertEqual(sec0.uniqueidtype, "CONID")
        self.assertEqual(sec0.uniqueid, "54321")
        self.assertEqual(sec0.ticker, "XYZ")
        self.assertEqual(sec0.secname, "Widgets Inc.")

        self.assertIsInstance(sec1, flex.Types.Security)
        self.assertEqual(sec1.uniqueidtype, "CUSIP")
        self.assertEqual(sec1.uniqueid, "1A2B3C")
        self.assertEqual(sec1.ticker, "XYZ")
        self.assertEqual(sec1.secname, "Widgets Inc.")

    def testParseSecurityTimeStamp(self):
        """parse_security() strips prepended timestamp from ticker.
        """
        secinfo = ibflex.Types.SecurityInfo(
            conid="54321",
            symbol="01234567890123XYZ",
            description="Widgets Inc."
        )

        secs = parser.parse_security(secinfo)
        self.assertEqual(len(secs), 1)
        sec = secs.pop()
        self.assertEqual(sec.ticker, "XYZ")

    def testParseTrade(self):
        xml = """
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="BIL" description="SPDR BBG BARC 1-3 MONTH TBIL" conid="45540682" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="1802867961" reportDate="2017-04-25" tradeDate="2017-04-25" tradeTime="105558" settleDateTarget="2017-04-28" transactionType="ExchTrade" exchange="NITEEXST" quantity="800" tradePrice="45.72" tradeMoney="36576" proceeds="-36576" taxes="0" ibCommission="-2.082058" ibCommissionCurrency="USD" netCash="-36578.082058" closePrice="45.71" openCloseIndicator="C" notes="P" cost="36565.34556" fifoPnlRealized="-12.736498" fxPnl="0" mtmPnl="-8" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="7533686121" buySell="BUY" ibOrderID="892713358" ibExecID="0000de6f.58ff4ddb.01.01" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="25621572" orderTime="2017-04-25;105558" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="LMT" traderID="" isAPIOrder="N" />
        """
        trade = ibflex.parser.parse_element(ET.fromstring(xml))
        tran = parser.parse_trade(trade)
        self.assertIsInstance(tran, flex.Types.Trade)
        self.assertEqual(tran.fitid, "1802867961")
        self.assertEqual(tran.dttrade, datetime.datetime(2017, 4, 25, 10, 55, 58))
        self.assertEqual(tran.memo, "SPDR BBG BARC 1-3 MONTH TBIL")
        self.assertEqual(tran.uniqueidtype, "CONID")
        self.assertEqual(tran.uniqueid, "45540682")
        self.assertEqual(tran.units, Decimal("800"))
        self.assertEqual(tran.total, Decimal("-36578.082058"))

    def testParseCashTransaction(self):
        attrib = {
            "accountId": "5678",
            "acctAlias": "Test account",
            "amount": "593517",
            "assetCategory": "STK",
            "clientReference": "",
            "code": "",
            "conid": "23",
            "currency": "USD",
            "cusip": "",
            "dateTime": "2015-04-23",
            "description": "ORGN(US68619E2081) CASH DIVIDEND 1.50000000 USD PER SHARE (Return of Capital)",
            "expiry": "",
            "fxRateToBase": "1",
            "isin": "",
            "issuer": "",
            "model": "",
            "multiplier": "1",
            "principalAdjustFactor": "",
            "putCall": "",
            "reportDate": "2015-04-23",
            "securityID": "",
            "securityIDType": "",
            "strike": "",
            "symbol": "ORGN",
            "tradeID": "",
            "transactionID": "5279100113",
            "type": "Dividends",
            "underlyingConid": "",
            "underlyingSymbol": "",
        }
        cashtx = ET.Element("CashTransaction", attrib=attrib)
        cashtx = ibflex.parser.parse_element(cashtx)

        tran = parser.parse_cash_transaction(cashtx)
        self.assertIsInstance(tran, flex.Types.CashTransaction)
        self.assertEqual(tran.fitid, "5279100113")
        self.assertEqual(tran.dtsettle, datetime.datetime(2015, 4, 23))
        self.assertEqual(
            tran.memo,
            "ORGN(US68619E2081) CASH DIVIDEND 1.50000000 USD PER SHARE (Return of Capital)",
        )
        self.assertEqual(tran.uniqueidtype, "CONID")
        self.assertEqual(tran.uniqueid, "23")
        self.assertEqual(tran.total, Decimal("593517"))

    def testParseCorporateAction(self):
        xml = """
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="ELAN" description="ELAN(US28413U2042) TENDERED TO US28413TEMP2 1 FOR 1 (ELAN, ELANDIA INTERNATIONAL INC, 28413U204)" conid="44939653" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2012-05-16" dateTime="2012-05-14;194500" amount="0" proceeds="0" value="0" quantity="-557915" fifoPnlRealized="0" mtmPnl="0" code="" type="TO" />
        """
        corpact = ET.fromstring(xml)
        corpact = ibflex.parser.parse_element(corpact)
        tran = parser.parse_corporate_action(corpact)
        self.assertIsInstance(tran, flex.Types.CorporateAction)
        self.assertEqual(tran.fitid, None)
        self.assertEqual(tran.dttrade, datetime.datetime(2012, 5, 14, 19, 45, 0))
        self.assertEqual(
            tran.memo,
            "ELAN(US28413U2042) TENDERED TO US28413TEMP2 1 FOR 1 (ELAN, ELANDIA INTERNATIONAL INC, 28413U204)",
        )
        self.assertEqual(tran.uniqueidtype, "CONID")
        self.assertEqual(tran.uniqueid, "44939653")
        self.assertEqual(tran.units, Decimal("-557915"))
        self.assertEqual(tran.total, Decimal("0"))
        self.assertEqual(tran.type, ibflex.enums.Reorg.TENDER)

    def testParseTransfer(self):
        corpact = ET.Element(
            "Transfer",
            {
                "date": "19970617",
                "description": "Transfer sthing",
                "conid": "27178",
                "quantity": "100",
                "direction": "OUT",
                "type": "ACATS",
                "account": "2112",
                "symbol": "XYZ",
            },
        )
        corpact = ibflex.parser.parse_element(corpact)
        tran = parser.parse_transfer(corpact)
        self.assertIsInstance(tran, flex.Types.Transfer)
        # FIXME - unique ID
        # self.assertEqual(tran.fitid, None)
        self.assertEqual(tran.dttrade, datetime.datetime(1997, 6, 17))
        self.assertEqual(tran.memo, "Transfer sthing")
        self.assertEqual(tran.uniqueidtype, "CONID")
        self.assertEqual(tran.uniqueid, "27178")
        self.assertEqual(tran.units, Decimal("100"))
        self.assertEqual(tran.tferaction, "OUT")
        self.assertEqual(tran.type, ibflex.enums.TransferType.ACATS)
        self.assertEqual(tran.other_acctid, "2112")


class CorporateActionsTypeTestCase(unittest.TestCase):
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

    def testInferCorporateActionType(self):
        for i, memo in enumerate(self.corpActMemos):
            cmp_code = self.cmptypes[i]
            inferredType = parser.inferCorporateActionType(memo)
            self.assertEqual(inferredType, ibflex.enums.Reorg(cmp_code))


class OptionsExerciseTestCase(XmlSnippetMixin, unittest.TestCase):
    stmt_sections = [
        (
            '<Trades>'

            '<Trade accountId="U12345" acctAlias="Test Alias" model="" '
            'currency="USD" fxRateToBase="1" assetCategory="OPT" '
            'symbol="VXX   110805C00020000" description="VXX 05AUG11 20.0 C" '
            'conid="91900358" securityID="" securityIDType="" cusip="" isin="" '
            'underlyingConid="80789235" underlyingSymbol="VXX" issuer="" '
            'multiplier="100" strike="20" expiry="2011-08-05" putCall="C" '
            'principalAdjustFactor="" tradeID="590365479" reportDate="2011-08-08" '
            'tradeDate="2011-08-05" tradeTime="162000" '
            'settleDateTarget="2011-08-08" transactionType="BookTrade" '
            'exchange="--" quantity="20" tradePrice="0" tradeMoney="0" '
            'proceeds="-0" taxes="0" ibCommission="0" ibCommissionCurrency="USD" '
            'netCash="0" closePrice="10.31" openCloseIndicator="C" notes="A" '
            'cost="21792.73144" fifoPnlRealized="0" fxPnl="0" mtmPnl="20620" '
            'origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" '
            'clearingFirmID="" transactionID="2366521558" buySell="BUY" '
            'ibOrderID="2366521558" ibExecID="" brokerageOrderID="" '
            'orderReference="" volatilityOrderLink="" exchOrderId="N/A" '
            'extExecID="N/A" orderTime="" openDateTime="" '
            'holdingPeriodDateTime="" whenRealized="" whenReopened="" '
            'levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" '
            'orderType="" traderID="" isAPIOrder="N" />'

            '<Trade accountId="U12345" acctAlias="Test Alias" model="" '
            'currency="USD" fxRateToBase="1" assetCategory="STK" symbol="VXX" '
            'description="IPATH S&amp;P 500 VIX S/T FU ETN" conid="80789235" '
            'securityID="" securityIDType="" cusip="" isin="" underlyingConid="" '
            'underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" '
            'putCall="" principalAdjustFactor="" tradeID="590365480" '
            'reportDate="2011-08-08" tradeDate="2011-08-05" tradeTime="162000" '
            'settleDateTarget="2011-08-10" transactionType="BookTrade" '
            'exchange="--" quantity="-2000" tradePrice="20" tradeMoney="-40000" '
            'proceeds="40000" taxes="0" ibCommission="-0.768" '
            'ibCommissionCurrency="USD" netCash="39999.232" closePrice="34.78" '
            'openCloseIndicator="O" notes="A" cost="-39999.232" '
            'fifoPnlRealized="0" fxPnl="0" mtmPnl="-29560" origTradePrice="0" '
            'origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" '
            'transactionID="2366521559" buySell="SELL" ibOrderID="2366521559" '
            'ibExecID="" brokerageOrderID="" orderReference="" '
            'volatilityOrderLink="" exchOrderId="N/A" extExecID="N/A" '
            'orderTime="" openDateTime="" holdingPeriodDateTime="" '
            'whenRealized="" whenReopened="" levelOfDetail="EXECUTION" '
            'changeInPrice="0" changeInQuantity="0" orderType="" traderID="" '
            'isAPIOrder="N" />'

            '</Trades>'
        ),
        (
            '<OptionEAE>'

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
    ]
    securities_info = [
        {
            "conid": "91900358",
            "symbol": "VXX   110805C00020000",
        },
        {
            "conid": "80789235",
            "symbol": "VXX",
        },
    ]

    def testOptionEAE(self):
        #  2 Trades and 2 OptionEAEs for 1 assignment
        #  Assign 20 VXX 05AUG11 20.0 C
        transactions = self.statement.transactions
        self.assertIsInstance(transactions, list)
        self.assertEqual(len(transactions), 1)
        tx = transactions[0]
        self.assertEqual(
            tx,
            flex.Types.Exercise(
                fitid="590365479",
                dttrade=datetime.datetime(2011, 8, 5, 16, 20),  # From the option Trade
                memo="Assign 20 VXX 05AUG11 20.0 C",
                uniqueidtype="CONID",  # From the underlying Trade
                uniqueid="80789235",  # From the underlying Trade
                units=Decimal("-2000"),  # From the underlying Trade
                currency="USD",  # From the underlying Trade
                total=Decimal("39999.232"),  # From the underlying Trade, net of comm.
                uniqueidtypeFrom="CONID",  # From the option Trade
                uniqueidFrom="91900358",  # From the option Trade
                unitsfrom=Decimal("20"),  # From the option Trade
                reportdate=datetime.date(2011, 8, 8),  # From the option Trade
                notes=(ibflex.enums.Code.ASSIGNMENT,),  # From the underlying Trade
            )
        )


if __name__ == "__main__":
    unittest.main(verbosity=3)
