# coding: utf-8
"""
"""
# stdlib imports
import unittest
import datetime
from decimal import Decimal
import xml.etree.ElementTree as ET


# 3rd party imports
from ibflex import schemata


# local imports
from capgains.flex import parser


class FlexParserTestCase(unittest.TestCase):
    def testParseAccount(self):
        acctinfo = ET.Element('AccountInformation', {'accountId': '12345'})
        acctinfo = schemata.AccountInformation.convert(acctinfo)
        acctinfo = parser.parse_acctinfo(acctinfo)
        self.assertIsInstance(acctinfo, parser.Account)
        self.assertEqual(acctinfo.acctid, '12345')
        self.assertEqual(acctinfo.brokerid, '4705')

    def testParseSecurities(self):
        pass

    def testParseSecurity(self):
        secinfo = ET.Element('SecurityInfo',
                             {'conid': '54321', 'symbol': 'XYZ',
                              'description': 'Widgets Inc.'})
        secinfo = schemata.SecurityInfo.convert(secinfo)
        secs = parser.parse_security(secinfo)
        self.assertEqual(len(secs), 1)
        sec = secs.pop()
        self.assertIsInstance(sec, parser.Security)
        self.assertEqual(sec.uniqueidtype, 'CONID')
        self.assertEqual(sec.uniqueid, '54321')
        self.assertEqual(sec.ticker, 'XYZ')
        self.assertEqual(sec.secname, 'Widgets Inc.')

    def testParseSecurityTimeStamp(self):
        secinfo = ET.Element('SecurityInfo',
                             {'conid': '54321', 'symbol': '01234567890123XYZ',
                              'description': 'Widgets Inc.'})
        secinfo = schemata.SecurityInfo.convert(secinfo)
        secs = parser.parse_security(secinfo)
        self.assertEqual(len(secs), 1)
        sec = secs.pop()
        self.assertEqual(sec.ticker, 'XYZ')

    def testParseTrade(self):
        xml = """
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="BIL" description="SPDR BBG BARC 1-3 MONTH TBIL" conid="45540682" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="1802867961" reportDate="2017-04-25" tradeDate="2017-04-25" tradeTime="105558" settleDateTarget="2017-04-28" transactionType="ExchTrade" exchange="NITEEXST" quantity="800" tradePrice="45.72" tradeMoney="36576" proceeds="-36576" taxes="0" ibCommission="-2.082058" ibCommissionCurrency="USD" netCash="-36578.082058" closePrice="45.71" openCloseIndicator="C" notes="P" cost="36565.34556" fifoPnlRealized="-12.736498" fxPnl="0" mtmPnl="-8" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="7533686121" buySell="BUY" ibOrderID="892713358" ibExecID="0000de6f.58ff4ddb.01.01" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="25621572" orderTime="2017-04-25;105558" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="LMT" traderID="" isAPIOrder="N" />
        """
        trade = schemata.Trade.convert(ET.fromstring(xml))
        tran = parser.parse_trade(trade)
        self.assertIsInstance(tran, parser.Trade)
        self.assertEqual(tran.fitid, '1802867961')
        self.assertEqual(tran.dttrade, datetime.datetime(2017, 4, 25, 10, 55, 58))
        self.assertEqual(tran.memo, 'SPDR BBG BARC 1-3 MONTH TBIL')
        self.assertEqual(tran.uniqueidtype, 'CONID')
        self.assertEqual(tran.uniqueid, '45540682')
        self.assertEqual(tran.units, Decimal('800'))
        self.assertEqual(tran.total, Decimal('-36578.082058'))

    def testParseCashTransaction(self):
        attrib = {'accountId': '5678', 'acctAlias': 'Test account',
                  'amount': '593517', 'assetCategory': 'STK',
                  'clientReference': '', 'code': '', 'conid': '23',
                  'currency': 'USD', 'cusip': '', 'dateTime': '2015-04-23',
                  'description': 'ORGN(US68619E2081) CASH DIVIDEND 1.50000000 USD PER SHARE (Return of Capital)',
                  'expiry': '', 'fxRateToBase': '1', 'isin': '', 'issuer': '',
                  'model': '', 'multiplier': '1', 'principalAdjustFactor': '',
                  'putCall': '', 'reportDate': '2015-04-23', 'securityID': '',
                  'securityIDType': '', 'strike': '', 'symbol': 'ORGN',
                  'tradeID': '', 'transactionID': '5279100113',
                  'type': 'Dividends', 'underlyingConid': '',
                  'underlyingSymbol': '', }
        cashtx = ET.Element('CashTransaction', attrib=attrib)
        cashtx = schemata.CashTransaction.convert(cashtx)

        tran = parser.parse_cash_transaction(cashtx)
        self.assertIsInstance(tran, parser.CashTransaction)
        self.assertEqual(tran.fitid, '5279100113')
        self.assertEqual(tran.dtsettle, datetime.datetime(2015, 4, 23))
        self.assertEqual(tran.memo, 'ORGN(US68619E2081) CASH DIVIDEND 1.50000000 USD PER SHARE (Return of Capital)')
        self.assertEqual(tran.uniqueidtype, 'CONID')
        self.assertEqual(tran.uniqueid, '23')
        self.assertEqual(tran.total, Decimal('593517'))

    def testParseCorporateAction(self):
        xml = """
        <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="ELAN" description="ELAN(US28413U2042) TENDERED TO US28413TEMP2 1 FOR 1 (ELAN, ELANDIA INTERNATIONAL INC, 28413U204)" conid="44939653" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2012-05-16" dateTime="2012-05-14;194500" amount="0" proceeds="0" value="0" quantity="-557915" fifoPnlRealized="0" mtmPnl="0" code="" type="TO" />
        """
        corpact = ET.fromstring(xml)
        corpact = schemata.CorporateAction.convert(corpact)
        tran = parser.parse_corporate_action(corpact)
        self.assertIsInstance(tran, parser.CorporateAction)
        self.assertEqual(tran.fitid, None)
        self.assertEqual(tran.dttrade, datetime.datetime(2012, 5, 14, 19, 45, 0))
        self.assertEqual(tran.memo, 'ELAN(US28413U2042) TENDERED TO US28413TEMP2 1 FOR 1 (ELAN, ELANDIA INTERNATIONAL INC, 28413U204)')
        self.assertEqual(tran.uniqueidtype, 'CONID')
        self.assertEqual(tran.uniqueid, '44939653')
        self.assertEqual(tran.units, Decimal('-557915'))
        self.assertEqual(tran.total, Decimal('0'))
        self.assertEqual(tran.type, 'TO')

    def testParseTransfer(self):
        corpact = ET.Element('Transfer',
                             {'date': '19970617',
                              'description': 'Transfer sthing',
                              'conid': '27178', 'quantity': '100',
                              'direction': 'OUT', 'type': 'ACATS',
                              'account': '2112', 'symbol': 'XYZ'})
        corpact = schemata.Transfer.convert(corpact)
        tran = parser.parse_transfer(corpact)
        self.assertIsInstance(tran, parser.Transfer)
        # FIXME - unique ID
        # self.assertEqual(tran.fitid, None)
        self.assertEqual(tran.dttrade, datetime.date(1997, 6, 17))
        self.assertEqual(tran.memo, 'Transfer sthing')
        self.assertEqual(tran.uniqueidtype, 'CONID')
        self.assertEqual(tran.uniqueid, '27178')
        self.assertEqual(tran.units, Decimal('100'))
        self.assertEqual(tran.tferaction, 'OUT')
        self.assertEqual(tran.type, 'ACATS')
        self.assertEqual(tran.other_acctid, '2112')


if __name__ == '__main__':
    unittest.main(verbosity=3)
