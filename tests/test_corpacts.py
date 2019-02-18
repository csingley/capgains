# coding: utf-8
""" End-to-end (ish) tests of corporate action transaction import """
# stdlib imports
import unittest
from datetime import (datetime, date)
from decimal import Decimal
from collections import namedtuple
import re


# local imports
from capgains import (flex, containers, )
from capgains.inventory import Transaction
from common import (
    setUpModule,
    tearDownModule,
    XmlSnippetMixin,
)


# Data containers used to construct tests
CorpActKey = namedtuple('CorpActKey', ['dttrade', 'type', 'memo'])

CorpActFields = namedtuple('CorpActFields',
                           ['uniqueid', 'units', 'total', 'reportdate',
                            'ticker', 'cusip', 'secname'])


# Test case mixins
class CorpActXmlSnippetMixin(XmlSnippetMixin):
    txs_entry_point = 'doCorporateActions'


class CorpActPreprocessingMixin(object):
    ticker_regex = re.compile(r"(\d{14,14})?(?P<ticker>.+)")
    # preprocessed_txs is a sequence of duples of
    # (CorpAct Key, sequence of CorpActFields)
    # that's fed to _makeParsedCorpAct() to make
    # flex.readerParsedCorpAct instances
    preprocessed_txs = NotImplemented  # Implement in subclass

    def _makeParsedCorpAct(self, key, fields):
        # For ParsedCorpAct.ticker, strip prepended date
        match = self.ticker_regex.match(fields.ticker)
        stripped_ticker = match.group('ticker')

        raw_memo = "{} ({}, {}, {})".format(
            key.memo, fields.ticker, fields.secname, fields.cusip)
        raw = flex.parser.CorporateAction(
            fitid=None, dttrade=key.dttrade, memo=raw_memo,
            uniqueidtype='CONID', uniqueid=fields.uniqueid, units=fields.units,
            currency='USD', total=fields.total, type=key.type,
            reportdate=fields.reportdate, code=[])
        return flex.reader.ParsedCorpAct(
            raw=raw, type=key.type, ticker=stripped_ticker, cusip=fields.cusip,
            secname=fields.secname, memo=key.memo)

    def testPreprocessCorpActs(self):
        """ Test output of FlexStatementReader.preprocessCorporateActions() """
        group = self.reader.preprocessCorporateActions(self.parsed_txs)

        group_predict = containers.GroupedList(
            [containers.GroupedList(
                [self._makeParsedCorpAct(key, field) for field in fields],
                grouped=False, key=tuple(key))
                for key, fields in self.preprocessed_txs],
            grouped=True, key=None)

        self.assertEqual(group, group_predict)


# Test case classes
class CorpActCancelTestCase(CorpActXmlSnippetMixin, CorpActPreprocessingMixin,
                            unittest.TestCase):
    """
    Tests flex.reader.doCorporateActions() logic where the data set includes
    cancelling transactions.
    """
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

    preprocessed_txs = [
        (CorpActKey(dttrade=datetime(2012, 5, 14, 19, 45), type='TO',
                    memo='ELAN(US28413U2042) TENDERED TO US28413TEMP2 1 FOR 1'),
         # Same as XML - 3rd & 1st transactions - sorted backwards b/c
         # flex.reader.FlexStatementReader.groupCorporateActionsForCancel()
         # sorts by CorporateAction.uniqueid (str not int,
         # so '107375314' sorts before '44939653').
         [CorpActFields(uniqueid='107375314', units=Decimal('557915'),
                        total=Decimal('0'), reportdate=date(2012, 5, 16),
                        ticker='ELAN.TEMP', cusip='28413TEMP',
                        secname='ELANDIA INTERNATIONAL INC - TEMP'),
          CorpActFields(uniqueid='44939653', units=Decimal('-557915'),
                        total=Decimal('0'),
                        reportdate=date(2012, 5, 16), ticker='ELAN',
                        cusip='28413U204',
                        secname='ELANDIA INTERNATIONAL INC'),
        ]),
        (CorpActKey(dttrade=datetime(2012, 5, 15, 20, 25), type='TC',
                    memo='ELAN.TEMP(US28413TEMP2) MERGED(Acquisition)  WITH US284CNT9952 1 FOR 10000'),
         # In the XML list above, the 5th transaction cancels the 2nd, and
         # the 7th transaction cancels the 4th.  That leaves the 6th and
         # 8th transactions as the surviving members of this group (since
         # the 1st and 3rd belong to the 2012-05-14 type='TO' group above.
         [CorpActFields(uniqueid='107374662', units=Decimal('56'),
                        total=Decimal('0'), reportdate=date(2012, 5, 24),
                        ticker='ELAN.CNT', cusip='284CNT995',
                        secname='ELANDIA INTERNATIONAL INC - CONTRA'),
          CorpActFields(uniqueid='107375314', units=Decimal('-557915'),
                        total=Decimal('0'), reportdate=date(2012, 5, 24),
                        ticker='ELAN.TEMP', cusip='28413TEMP',
                        secname='ELANDIA INTERNATIONAL INC - TEMP'),
        ]),
    ]

    @property
    def persisted_txs(self):
        # Each of the groups in preprocessed_txs() gets processed as transfer
        return [
            # Synthetic trade created by voluntary subscription (backdated)
            Transaction(datetime=datetime(2012, 5, 14, 19, 45),
                        type='transfer',
                        memo='ELAN(US28413U2042) TENDERED TO US28413TEMP2 1 FOR 1',
                        fiaccount=self.account, security=self.securities[2],
                        units=Decimal('557915'), fiaccountFrom=self.account,
                        securityFrom=self.securities[0],
                        unitsFrom=Decimal('-557915')),
            Transaction(datetime=datetime(2012, 5, 15, 20, 25),
                        type='transfer',
                        memo='ELAN.TEMP(US28413TEMP2) MERGED(Acquisition)  WITH US284CNT9952 1 FOR 10000',
                        fiaccount=self.account, security=self.securities[1],
                        units=Decimal('56'), fiaccountFrom=self.account,
                        securityFrom=self.securities[2],
                        unitsFrom=Decimal('-557915')),
    ]


class SubscriptionSpanningMultipleDatesTestCase(CorpActXmlSnippetMixin,
                                                CorpActPreprocessingMixin,
                                                unittest.TestCase):
    """
    Tests the hack in flex.reader.FlexStatementReader.tender() and
    flex.reader.FlexStatementReader.merge_reorg() to correct cost basis for
    cash subscriptions where the two legs occur on different dates, so the
    necessary data isn't contained within each transaction group.
    """
    xml = """
    <CorporateActions>
    <CorporateAction accountId="U999999" acctAlias="Test Account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="SGGHU" description="SGGHU(US82670K1280) TENDERED TO US8269922402 1 FOR 1 (SGGHU, SIGNATURE GROUP HOLDINGS INC, 82670K128)" conid="182241365" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-02-19" dateTime="2015-02-19;194500" amount="0" proceeds="0" value="0" quantity="-34000" fifoPnlRealized="0" mtmPnl="0" code="" type="TO" />
    <CorporateAction accountId="U999999" acctAlias="Test Account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="SGGHU.EX" description="SGGHU(US82670K1280) TENDERED TO US8269922402 1 FOR 1 (SGGHU.EX, SIGNATURE GROUP HOLDINGS INC - BASIC SUBSCRIPTION, 826992240)" conid="184282118" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-02-19" dateTime="2015-02-19;194500" amount="0" proceeds="0" value="0" quantity="34000" fifoPnlRealized="0" mtmPnl="0" code="" type="TO" />
    <CorporateAction accountId="U999999" acctAlias="Test Account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="20150224001800SGGHU" description="SGGHU.EX(US8269922402) MERGED(Voluntary Offer Allocation)  WITH SGRH 1 FOR 1,US8269922576 562 FOR 10 (20150224001800SGGHU, SIGNATURE GROUP HOLDINGS COMMON STOCK, 826992257)" conid="185081596" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-02-26" dateTime="2015-02-25;202500" amount="0" proceeds="0" value="101272.4" quantity="19108" fifoPnlRealized="0" mtmPnl="0" code="" type="TC" />
    <CorporateAction accountId="U999999" acctAlias="Test Account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="SGGHU" description="SGGHU(US82670K1280) TENDERED TO US8269922402 1 FOR 1 (SGGHU, SIGNATURE GROUP HOLDINGS INC, 82670K128)" conid="182241365" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-02-26" dateTime="2015-02-19;194500" amount="107769.12" proceeds="-107769.12" value="0" quantity="0" fifoPnlRealized="0" mtmPnl="-107769.12" code="" type="TO" />
    <CorporateAction accountId="U999999" acctAlias="Test Account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="SGGHU.EX" description="SGGHU.EX(US8269922402) MERGED(Voluntary Offer Allocation)  WITH SGRH 1 FOR 1,US8269922576 562 FOR 10 (SGGHU.EX, SIGNATURE GROUP HOLDINGS INC - BASIC SUBSCRIPTION, 826992240)" conid="184282118" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-02-26" dateTime="2015-02-25;202500" amount="0" proceeds="0" value="-243440" quantity="-34000" fifoPnlRealized="0" mtmPnl="85292.4" code="" type="TC" />
    <CorporateAction accountId="U999999" acctAlias="Test Account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="SGRH" description="SGGHU.EX(US8269922402) MERGED(Voluntary Offer Allocation)  WITH SGRH 1 FOR 1,US8269922576 562 FOR 10 (SGRH, SIGNATURE GROUP HOLDINGS INC, 82670K201)" conid="185081553" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-02-26" dateTime="2015-02-25;202500" amount="0" proceeds="0" value="227460" quantity="34000" fifoPnlRealized="0" mtmPnl="0" code="" type="TC" />
    </CorporateActions>
    """

    preprocessed_txs = [
        (CorpActKey(dttrade=datetime(2015, 2, 19, 19, 45), type='TO',
                    memo='SGGHU(US82670K1280) TENDERED TO US8269922402 1 FOR 1'),
         # uniqueid='182241365' netted units/total into single transaction,
         # so we have 2 of them instead of 3.
         [CorpActFields(uniqueid='182241365', units=Decimal('-34000'),
                        total=Decimal('-107769.12'),
                        reportdate=date(2015, 2, 19), ticker='SGGHU',
                        cusip='82670K128',
                        secname='SIGNATURE GROUP HOLDINGS INC'),
          CorpActFields(uniqueid='184282118', units=Decimal('34000'),
                        total=Decimal('0'), reportdate=date(2015, 2, 19),
                        ticker='SGGHU.EX', cusip='826992240',
                        secname='SIGNATURE GROUP HOLDINGS INC - BASIC SUBSCRIPTION'),
        ]),
        (CorpActKey(dttrade=datetime(2015, 2, 25, 20, 25), type='TC',
                    memo='SGGHU.EX(US8269922402) MERGED(Voluntary Offer Allocation)  WITH SGRH 1 FOR 1,US8269922576 562 FOR 10'),
         # Same as XML
         [CorpActFields(uniqueid='184282118', units=Decimal('-34000'),
                        total=Decimal('0'), reportdate=date(2015, 2, 26),
                        ticker='SGGHU.EX', cusip='826992240',
                        secname='SIGNATURE GROUP HOLDINGS INC - BASIC SUBSCRIPTION'),
          CorpActFields(uniqueid='185081553', units=Decimal('34000'),
                        total=Decimal('0'), reportdate=date(2015, 2, 26),
                        ticker='SGRH', cusip='82670K201',
                        secname='SIGNATURE GROUP HOLDINGS INC'),
          CorpActFields(uniqueid='185081596', units=Decimal('19108'),
                        total=Decimal('0'), reportdate=date(2015, 2, 26),
                        ticker='20150224001800SGGHU', cusip='826992257',
                        secname='SIGNATURE GROUP HOLDINGS COMMON STOCK'),
        ]),
    ]

    @property
    def persisted_txs(self):
        # On a given datetime, CorpActXmlSnippetMixin.testDoCorporateActions()
        # sorts type='trade' before type='transfer'
        return [
            # Synthetic trade created by voluntary subscription (backdated)
            Transaction(datetime=datetime(2015, 2, 19, 19, 45),
                        type='trade',
                        memo='SGGHU.EX(US8269922402) MERGED(Voluntary Offer Allocation)  WITH SGRH 1 FOR 1,US8269922576 562 FOR 10',
                        currency='USD', cash=Decimal('-107769.12'),
                        fiaccount=self.account, security=self.securities[2],
                        units=Decimal('19108')),
            Transaction(datetime=datetime(2015, 2, 19, 19, 45),
                        type='transfer',
                        memo='SGGHU(US82670K1280) TENDERED TO US8269922402 1 FOR 1',
                        fiaccount=self.account, security=self.securities[1],
                        units=Decimal('34000'), fiaccountFrom=self.account,
                        securityFrom=self.securities[0],
                        unitsFrom=Decimal('-34000')),
            Transaction(datetime=datetime(2015, 2, 25, 20, 25),
                        type='transfer',
                        memo='SGGHU.EX(US8269922402) MERGED(Voluntary Offer Allocation)  WITH SGRH 1 FOR 1,US8269922576 562 FOR 10',
                        fiaccount=self.account, security=self.securities[3],
                        units=Decimal('34000'), fiaccountFrom=self.account,
                        securityFrom=self.securities[1],
                        unitsFrom=Decimal('-34000')),
    ]


class BondMaturityTestCase(CorpActXmlSnippetMixin, unittest.TestCase):
    xml = """
    <CorporateActions>
    <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="BOND" symbol="WMIH 13 03/19/30 LLB6" description="(US929CALLB67) BOND MATURITY FOR USD 1.00000000 PER BOND (WMIH 13 03/19/30 LLB6, WMIH 13 03/19/30 - PARTIAL CALL RED DATE 7/1, 929CALLB6)" conid="27" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="1" reportDate="2016-07-01" dateTime="2016-06-30;202500" amount="-3" proceeds="3" value="0" quantity="-3" fifoPnlRealized="3" mtmPnl="0.6" code="" type="BM" />
    </CorporateActions>
    """
    @property
    def persisted_txs(self):
        # Bond maturity processed as a trade
        return [
            Transaction(datetime=datetime(2016, 6, 30, 20, 25),
                        type='trade',
                        memo='(US929CALLB67) BOND MATURITY FOR USD 1.00000000 PER BOND',
                        currency='USD', cash=Decimal('3'),
                        fiaccount=self.account, security=self.securities[0],
                        units=Decimal('-3')),
    ]


class DelistTestCase(CorpActXmlSnippetMixin, unittest.TestCase):
    xml = """
    <CorporateActions>
    <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="ELAN.CNT" description="(US284CNT9952) DELISTED (ELAN.CNT, ELANDIA INTERNATIONAL INC - CONTRA, 284CNT995)" conid="266" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2013-10-07" dateTime="2013-10-03;202500" amount="0" proceeds="0" value="0" quantity="-56" fifoPnlRealized="0" mtmPnl="0" code="" type="DW" />
    </CorporateActions>
    """
    @property
    def persisted_txs(self):
        # Delisting processed as a trade with zero proceeds
        return [
            Transaction(datetime=datetime(2013, 10, 3, 20, 25),
                        type='trade',
                        memo='(US284CNT9952) DELISTED',
                        currency='USD', cash=Decimal('0'),
                        fiaccount=self.account, security=self.securities[0],
                        units=Decimal('-56')),
    ]


class ChangeSecurityTestCase(CorpActXmlSnippetMixin, unittest.TestCase):
    xml = """
    <CorporateActions>
    <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="EDCI.OLD" description="EDCI(US2683151086) CUSIP/ISIN CHANGE TO (US2683152076) (EDCI.OLD, EDCI HOLDINGS INC, 268315108)" conid="53562481" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2010-12-01" dateTime="2010-11-30;202500" amount="0" proceeds="0" value="0" quantity="-112833" fifoPnlRealized="0" mtmPnl="0" code="" type="IC" />
    <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="EDCID" description="EDCI(US2683151086) CUSIP/ISIN CHANGE TO (US2683152076) (EDCID, EDCI HOLDINGS INC, 268315207)" conid="81516263" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2010-12-01" dateTime="2010-11-30;202500" amount="0" proceeds="0" value="0" quantity="112833" fifoPnlRealized="0" mtmPnl="0" code="" type="IC" />
    </CorporateActions>
    """
    @property
    def persisted_txs(self):
        # CUSIP change processed as transfer
        return [
            Transaction(datetime=datetime(2010, 11, 30, 20, 25),
                        type='transfer',
                        memo="EDCI(US2683151086) CUSIP/ISIN CHANGE TO (US2683152076)",
                        fiaccount=self.account, security=self.securities[1], units=Decimal('112833'),
                        fiaccountFrom=self.account, securityFrom=self.securities[0], unitsFrom=Decimal('-112833'),),
    ]


class OversubscribeTestCase(CorpActXmlSnippetMixin, unittest.TestCase):
    xml = """
    <CorporateActions>
    <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="TPHS.OS" description="OVER SUBSCRIBE TPHS.OS (US89656D10OS) AT 6.00 USD (TPHS.OS, TRINITY PLACE HOLDINGS INC - RIGHTS OVERSUBSCRIPTION, 89656D10O)" conid="214128923" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-11-30" dateTime="2015-11-30;194500" amount="90000" proceeds="-90000" value="0" quantity="15000" fifoPnlRealized="0" mtmPnl="-90000" code="" type="OR" />
    </CorporateActions>
    """
    @property
    def persisted_txs(self):
        # Oversubscription processed as trade
        return [
            Transaction(datetime=datetime(2015, 11, 30, 19, 45),
                        type='trade',
                        memo='OVER SUBSCRIBE TPHS.OS (US89656D10OS) AT 6.00 USD',
                        currency='USD', cash=Decimal('-90000'),
                        fiaccount=self.account, security=self.securities[0], units=Decimal('15000')),
    ]


class RightsIssueTestCase(CorpActXmlSnippetMixin, unittest.TestCase):
    xml = """
    <CorporateActions>
    <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="EUR" fxRateToBase="1.141" assetCategory="STK" symbol="AMP.D" description="AMP(ES0109260531) SUBSCRIBABLE RIGHTS ISSUE  1 FOR 1 (AMP.D, AMPER SA - BONUS RTS, ES0609260924)" conid="194245312" securityID="ES0609260924" securityIDType="ISIN" cusip="" isin="ES0609260924" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-05-14" dateTime="2015-05-13;202500" amount="0" proceeds="0" value="0" quantity="70576" fifoPnlRealized="0" mtmPnl="0" code="" type="RI" />
    </CorporateActions>
    """

    extra_securities = [
        {'name': 'AMPER SA', 'ticker': 'AMP', 'uniqueidtype': 'CONID', 'uniqueid': '917393'},
    ]  # loaded by setUpClass()

    @property
    def persisted_txs(self):
        # Rights issue processed as spinoff
        return [
            Transaction(datetime=datetime(2015, 5, 13, 20, 25),
                        type='spinoff',
                        memo='AMP(ES0109260531) SUBSCRIBABLE RIGHTS ISSUE  1 FOR 1',
                        fiaccount=self.account, security=self.securities[0], units=Decimal('70576'),
                        securityFrom=self.securities[1],
                        numerator=Decimal('1'), denominator=Decimal('1')),
    ]


class SplitWithCusipChangeTestCase(CorpActXmlSnippetMixin, unittest.TestCase):
    xml = """
    <CorporateActions>
    <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="VXX" description="VXX(US06742E7114) SPLIT 1 FOR 4 (VXX, IPATH S&amp;P 500 VIX S/T FU ETN, 06740Q252)" conid="242500577" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2016-08-09" dateTime="2016-08-08;202500" amount="0" proceeds="0" value="0" quantity="-4250" fifoPnlRealized="0" mtmPnl="0" code="" type="RS" />
    <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="VXX.OLD" description="VXX(US06742E7114) SPLIT 1 FOR 4 (VXX.OLD, IPATH S&amp;P 500 VIX S/T FU ETN, 06742E711)" conid="137935324" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2016-08-09" dateTime="2016-08-08;202500" amount="0" proceeds="0" value="0" quantity="17000" fifoPnlRealized="0" mtmPnl="0" code="" type="RS" />
    </CorporateActions>
    """
    @property
    def persisted_txs(self):
        # Stock split with CUSIP change processed as transfer
        return [
            Transaction(datetime=datetime(2016, 8, 8, 20, 25),
                        type='transfer',
                        memo='VXX(US06742E7114) SPLIT 1 FOR 4',
                        fiaccount=self.account, security=self.securities[0], units=Decimal('-4250'),
                        fiaccountFrom=self.account, securityFrom=self.securities[1], unitsFrom=Decimal('17000'),),
    ]


class StockDividendTestCase(CorpActXmlSnippetMixin, unittest.TestCase):
    xml = """
    <CorporateActions>
    <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="MFCAF" description="MFCAF(P64605101) STOCK DIVIDEND 1 FOR 11 (MFCAF, MASS FINANCIAL CORP-CL A, P64605101)" conid="37839182" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2010-01-27" dateTime="2009-12-23;202500" amount="0" proceeds="0" value="10134.54545539" quantity="1090.909091" fifoPnlRealized="0" mtmPnl="10134.5455" code="" type="SD" />
    </CorporateActions>
    """
    @property
    def persisted_txs(self):
        # Stock dividend processed as split (with numerator adjusted)
        return [
            Transaction(datetime=datetime(2009, 12, 23, 20, 25),
                        type='split',
                        memo='MFCAF(P64605101) STOCK DIVIDEND 1 FOR 11',
                        fiaccount=self.account, security=self.securities[0], units=Decimal('1090.909091'),
                        numerator=Decimal('12'), denominator=Decimal('11')),
    ]


class SpinoffTestCase(CorpActXmlSnippetMixin, unittest.TestCase):
    xml = """
    <CorporateActions>
    <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="GYRO.NTS2" description="GYRO.NOTE(US403NOTE034) SPINOFF  1 FOR 40 (GYRO.NTS2, GYRODYNE CO OF AMERICA INC - GLOBAL DIVIDEND NOTE - PIK, 403PIK103)" conid="160689243" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-08-19" dateTime="2015-06-12;202500" amount="0" proceeds="0" value="0" quantity="1837.125" fifoPnlRealized="0" mtmPnl="0" code="" type="SO" />
    </CorporateActions>
    """

    extra_securities = [
        {'name': 'GYRODYNE CO OF AMERICA INC - 10.89 NON-TRANSFERABLE NOTES', 'ticker': 'GYRO.NOTE', 'uniqueidtype': 'CONID', 'uniqueid': '144464070'},
    ]  # loaded by setUpClass()

    @property
    def persisted_txs(self):
        return [
            Transaction(datetime=datetime(2015, 6, 12, 20, 25),
                        type='spinoff',
                        memo='GYRO.NOTE(US403NOTE034) SPINOFF  1 FOR 40',
                        fiaccount=self.account, security=self.securities[0], units=Decimal('1837.125'),
                        securityFrom=self.securities[1],
                        numerator=Decimal('1'), denominator=Decimal('40')),
    ]


class SubscribeRightsBadIsinToTestCase(CorpActXmlSnippetMixin, unittest.TestCase):
    xml = """
    <CorporateActions>
    <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="TPHS.EX" description="TPHS.RTS (US8969940274) SUBSCRIBES TO () (TPHS.EX, TRINITY PLACE HOLDINGS INC - RIGHTS SUBSCRIPTION, 89656D10E)" conid="214128916" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-11-30" dateTime="2015-11-30;194500" amount="0" proceeds="0" value="23034" quantity="3839" fifoPnlRealized="0" mtmPnl="0" code="" type="SR" />
    <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="TPHS.RTS" description="TPHS.RTS (US8969940274) SUBSCRIBES TO () (TPHS.RTS, TRINITY PLACE HOLDINGS INC - RIGHTS, 896994027)" conid="212130559" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-11-30" dateTime="2015-11-30;194500" amount="23034" proceeds="-23034" value="0" quantity="-3839" fifoPnlRealized="0" mtmPnl="0" code="" type="SR" />
    </CorporateActions>
    """
    @property
    def persisted_txs(self):
        # Subscribe to rights treated as option exercise
        return [
            Transaction(datetime=datetime(2015, 11, 30, 19, 45),
                        type='exercise',
                        memo='TPHS.RTS (US8969940274) SUBSCRIBES TO ()',
                        currency='USD', cash=Decimal('-23034'),
                        fiaccount=self.account, security=self.securities[0], units=Decimal('3839'),
                        securityFrom=self.securities[1], unitsFrom=Decimal('-3839')),
    ]


class MergerBadIsinFromTestCase(CorpActXmlSnippetMixin, unittest.TestCase):
    """
    Tests merge_reorg() where source ISIN can't be parsed from memo
    """
    xml = """
    <CorporateActions>
    <CorporateAction accountId="5678" acctAlias="Test account" amount="0" assetCategory="STK" code="" conid="135944283" currency="EUR" cusip="" dateTime="2013-10-04;202500" description="AMP.RSTD(135893865) MERGED(Acquisition)  WITH AMP.REST 1 FOR 1 (AMP.REST, AMPER SA - RESTRICTED, ES010RSTD531)" expiry="" fifoPnlRealized="0" fxRateToBase="1.3582" isin="ES010RSTD531" issuer="" model="" mtmPnl="-0.3168" multiplier="1" principalAdjustFactor="" proceeds="0" putCall="" quantity="70576.0000" reportDate="2013-10-07" securityID="ES010RSTD531" securityIDType="ISIN" strike="" symbol="AMP.REST" type="TC" underlyingConid="" underlyingSymbol="" value="90337.2800" />
    <CorporateAction accountId="5678" acctAlias="Test account" amount="0" assetCategory="STK" code="" conid="135893865" currency="USD" cusip="" dateTime="2013-10-04;202500" description="AMP.RSTD(135893865) MERGED(Acquisition)  WITH AMP.REST 1 FOR 1 (AMP.RSTD, AMPER SA - RESTRICTED, ES010RSTD531)" expiry="" fifoPnlRealized="0" fxRateToBase="1" isin="ES010RSTD531" issuer="" model="" mtmPnl="0" multiplier="1" principalAdjustFactor="" proceeds="0" putCall="" quantity="-70576.0000" reportDate="2013-10-07" securityID="ES010RSTD531" securityIDType="ISIN" strike="" symbol="AMP.RSTD" type="TC" underlyingConid="" underlyingSymbol="" value="0" />
    </CorporateActions>
    """
    @property
    def persisted_txs(self):
        return [
            Transaction(datetime=datetime(2013, 10, 4, 20, 25),
                        type='transfer',
                        memo='AMP.RSTD(135893865) MERGED(Acquisition)  WITH AMP.REST 1 FOR 1',
                        fiaccount=self.account, security=self.securities[0], units=Decimal('70576'),
                        fiaccountFrom=self.account, securityFrom=self.securities[1], unitsFrom=Decimal('-70576'),
                       ),
    ]


class CashMergerTestCase(CorpActXmlSnippetMixin, unittest.TestCase):
    xml = """
    <CorporateActions>
    <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="WMI HLDGS CORP 13% SEC LIEN NT 03/16/2030 - PARTIAL CALL" description="92CALLAB6(US92CALLAB67) MERGED(Partial Call)  FOR USD 1.00000000 PER SHARE (WMI HLDGS CORP 13% SEC LIEN NT 03/16/2030 - PARTIAL CALL, WMI HLDGS CORP 13% SEC LIEN NT 03/16/2030 - PARTIAL CALL, 92CALLAB6)" conid="196610660" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2017-03-31" dateTime="2017-03-30;202500" amount="-1" proceeds="1" value="-93" quantity="-1" fifoPnlRealized="1" mtmPnl="-92" code="" type="TC" />
    </CorporateActions>
    """
    @property
    def persisted_txs(self):
        # All-cash merger processed as trade
        return [
            Transaction(datetime=datetime(2017, 3, 30, 20, 25),
                        type='trade',
                        memo='92CALLAB6(US92CALLAB67) MERGED(Partial Call)  FOR USD 1.00000000 PER SHARE',
                        currency='USD', cash=Decimal('1'),
                        fiaccount=self.account, security=self.securities[0], units=Decimal('-1')),
    ]


class KindMergerTestCase(CorpActXmlSnippetMixin, unittest.TestCase):
    xml = """
    <CorporateActions>
    <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="TPHS" description="TPHS.EX(US89656R10EX) MERGED(Voluntary Offer Allocation)  WITH US89656D1019 1 FOR 1 (TPHS, TRINITY PLACE HOLDINGS INC, 89656D101)" conid="113775558" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2017-04-17" dateTime="2017-04-12;202500" amount="0" proceeds="0" value="18256.75" quantity="2575" fifoPnlRealized="0" mtmPnl="0" code="" type="TC" />
    <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="TPHS.EX" description="TPHS.EX(US89656R10EX) MERGED(Voluntary Offer Allocation)  WITH US89656D1019 1 FOR 1 (TPHS.EX, TRINITY PLACE HOLDINGS INC - RIGHTS SUBSCRIPTION, 89656R10E)" conid="271739961" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2017-04-17" dateTime="2017-04-12;202500" amount="0" proceeds="0" value="-18231" quantity="-2575" fifoPnlRealized="0" mtmPnl="25.75" code="" type="TC" />
    </CorporateActions>
    """
    @property
    def persisted_txs(self):
        # Merger in kind processed as transfer
        return [
            Transaction(datetime=datetime(2017, 4, 12, 20, 25),
                        type='transfer',
                        memo='TPHS.EX(US89656R10EX) MERGED(Voluntary Offer Allocation)  WITH US89656D1019 1 FOR 1',
                        fiaccount=self.account, security=self.securities[0], units=Decimal('2575'),
                        fiaccountFrom=self.account, securityFrom=self.securities[1], unitsFrom=Decimal('-2575'),
                       ),
    ]


class CashKindMergerTestCase(CorpActXmlSnippetMixin, unittest.TestCase):
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
    @property
    def persisted_txs(self):
        # Merger for cash & kind processes cash portion as return of capital,
        # then processes receipt of in-kind consideration as transfer (further
        # processing spinoffs for any additional in-kind consideration)
        #
        # On a given datetime, CorpActXmlSnippetMixin.testDoCorporateActions()
        # sorts type='returnofcapital' before type='spinoff' before type='transfer'
        return [
            #
            # Synthetic return of capital
            Transaction(datetime=datetime(2012, 4, 16, 20, 25),
                        dtsettle=datetime(2012, 4, 16, 20, 25),
                        type='returnofcapital',
                        memo='DIMEQ.TMP(US254TMP9913) CASH and STOCK MERGER (Voluntary Offer Allocation) WMIH 1146667 FOR 10000000',
                        currency='USD', cash=Decimal('16.5'),
                        fiaccount=self.account, security=self.securities[0]),
            Transaction(datetime=datetime(2012, 4, 16, 20, 25),
                        type='transfer',
                        memo='DIMEQ.TMP(US254TMP9913) CASH and STOCK MERGER (Voluntary Offer Allocation) WMIH 1146667 FOR 10000000',
                        fiaccount=self.account, security=self.securities[1], units=Decimal('17200.005'),
                        fiaccountFrom=self.account, securityFrom=self.securities[0], unitsFrom=Decimal('-150000')),
            Transaction(datetime=datetime(2012, 4, 16, 20, 25),
                        type='spinoff',
                        memo='DIMEQ.TMP(US254TMP9913) CASH and STOCK MERGER (Voluntary Offer Allocation) WMIH 1146667 FOR 10000000',
                        fiaccount=self.account, security=self.securities[2], units=Decimal('150000'),
                        securityFrom=self.securities[0],
                        numerator=Decimal('150000'), denominator=Decimal('150000')),
    ]


class TenderTestCase(CorpActXmlSnippetMixin, unittest.TestCase):
    xml = """
    <CorporateActions>
    <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="NTP" description="NTP(G63907102) TENDERED TO G63990272 1 FOR 1 (NTP, NAM TAI PROPERTY INC, VGG639071023)" conid="148502652" securityID="VGG639071023" securityIDType="ISIN" cusip="" isin="VGG639071023" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-09-04" dateTime="2015-09-04;194500" amount="0" proceeds="0" value="0" quantity="-60996" fifoPnlRealized="0" mtmPnl="0" code="" type="TO" />
    <CorporateAction accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="NTP.TEN" description="NTP(G63907102) TENDERED TO G63990272 1 FOR 1 (NTP.TEN, NAM TAI PROPERTY INC - TENDER, VGG639902722)" conid="205921721" securityID="VGG639902722" securityIDType="ISIN" cusip="" isin="VGG639902722" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" reportDate="2015-09-04" dateTime="2015-09-04;194500" amount="0" proceeds="0" value="0" quantity="60996" fifoPnlRealized="0" mtmPnl="0" code="" type="TO" />
    </CorporateActions>
    """
    @property
    def persisted_txs(self):
        # Tender processed as transfer
        return [
            Transaction(datetime=datetime(2015, 9, 4, 19, 45),
                        type='transfer',
                        memo='NTP(G63907102) TENDERED TO G63990272 1 FOR 1',
                        fiaccount=self.account, security=self.securities[1], units=Decimal('60996'),
                        fiaccountFrom=self.account, securityFrom=self.securities[0], unitsFrom=Decimal('-60996'),),
    ]


if __name__ == '__main__':
    unittest.main()
