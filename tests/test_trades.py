# coding: utf-8
""" End-to-end (ish) tests of trade transaction import """
# stdlib imports
import unittest
from datetime import datetime
from decimal import Decimal


# local imports
from capgains.models.transactions import TransactionType
from capgains.inventory import Trade
from common import setUpModule, tearDownModule, XmlSnippetMixin


# Test case mixins
class TradeXmlSnippetMixin(XmlSnippetMixin):
    txs_entry_point = "doTrades"


# Test case classes
class TradesWithCancelTestCase(TradeXmlSnippetMixin, unittest.TestCase):
    xml = """
    <Trades>
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="CNVR.SPO" description="CONVERA CORPORATION - SPINOFF" conid="132118505" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="" reportDate="2013-08-01" tradeDate="2011-05-09" tradeTime="000000" settleDateTarget="" transactionType="FracShare" exchange="--" quantity="-0.276942" tradePrice="0.00001" tradeMoney="-0.000002769" proceeds="0.000002769" taxes="0" ibCommission="0" ibCommissionCurrency="USD" netCash="0.000002769" closePrice="0.00001" openCloseIndicator="C" notes="" cost="-449.712074" fifoPnlRealized="-449.712071" fxPnl="0" mtmPnl="-0" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="3723709320" buySell="SELL" ibOrderID="3723709320" ibExecID="" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="N/A" orderTime="" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="" traderID="" isAPIOrder="N" />
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="CNVR.SPO" description="CONVERA CORPORATION - SPINOFF" conid="132118505" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="" reportDate="2013-09-20" tradeDate="2011-05-09" tradeTime="000000" settleDateTarget="" transactionType="FracShareCancel" exchange="--" quantity="0.276942" tradePrice="0.00001" tradeMoney="0.000002769" proceeds="-0.000002769" taxes="0" ibCommission="0" ibCommissionCurrency="USD" netCash="-0.000002769" closePrice="0.00001" openCloseIndicator="" notes="Ca" cost="0.000002769" fifoPnlRealized="0" fxPnl="0" mtmPnl="0" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="3831648707" buySell="SELL (Ca.)" ibOrderID="3831648707" ibExecID="" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="N/A" orderTime="" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="" traderID="" isAPIOrder="N" />
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="CNVR.SPO" description="CONVERA CORPORATION - SPINOFF" conid="132118505" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="" reportDate="2013-09-20" tradeDate="2011-05-09" tradeTime="000000" settleDateTarget="" transactionType="FracShare" exchange="--" quantity="-0.276942" tradePrice="203.698646" tradeMoney="-56.412710421" proceeds="56.412710421" taxes="0" ibCommission="0" ibCommissionCurrency="USD" netCash="56.412710421" closePrice="0.00001" openCloseIndicator="C" notes="" cost="-0.000003" fifoPnlRealized="56.412708" fxPnl="0" mtmPnl="56.4127" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="3831652905" buySell="SELL" ibOrderID="3831652905" ibExecID="" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="N/A" orderTime="" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="" traderID="" isAPIOrder="N" />
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="CNVR.SPO" description="CONVERA CORPORATION - SPINOFF" conid="132118505" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="" reportDate="2013-11-18" tradeDate="2011-05-09" tradeTime="000000" settleDateTarget="" transactionType="FracShareCancel" exchange="--" quantity="0.276942" tradePrice="203.698646" tradeMoney="56.412710421" proceeds="-56.412710421" taxes="0" ibCommission="0" ibCommissionCurrency="USD" netCash="-56.412710421" closePrice="0.00001" openCloseIndicator="" notes="Ca" cost="56.412710421" fifoPnlRealized="0" fxPnl="0" mtmPnl="-56.4127" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="3964505548" buySell="SELL (Ca.)" ibOrderID="3964505548" ibExecID="" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="N/A" orderTime="" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="" traderID="" isAPIOrder="N" />
        <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="CNVR.SPO" description="CONVERA CORPORATION - SPINOFF" conid="132118505" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="" reportDate="2013-11-18" tradeDate="2011-05-09" tradeTime="000000" settleDateTarget="" transactionType="FracShare" exchange="--" quantity="-0.276942" tradePrice="5334.4" tradeMoney="-1477.3194048" proceeds="1477.3194048" taxes="0" ibCommission="0" ibCommissionCurrency="USD" netCash="1477.3194048" closePrice="0.00001" openCloseIndicator="C" notes="" cost="-56.41271" fifoPnlRealized="1420.906694" fxPnl="0" mtmPnl="1477.3194" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="3964508206" buySell="SELL" ibOrderID="3964508206" ibExecID="" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="N/A" orderTime="" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="" traderID="" isAPIOrder="N" />
        </Trades>
        """

    @property
    def persisted_txs(self):
        # 2nd trade cancels the 1st; 4th trade cancels the 3rd
        # Leaving the last trade as the only one
        return [
            Trade(
                id=1,
                uniqueid="DEADBEEF",
                datetime=datetime(2011, 5, 9),
                memo="CONVERA CORPORATION - SPINOFF",
                currency="USD",
                cash=Decimal("1477.3194048"),
                fiaccount=self.account,
                security=self.securities[0],
                units=Decimal("-0.276942"),
            )
        ]


class TradesIgnoreTestCase(TradeXmlSnippetMixin, unittest.TestCase):
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

    @property
    def persisted_txs(self):
        # First trade is FX (skipped)
        # Last trade cancels the 2nd trade
        # Leaving 3rd & 4th XML transactions parsed as trades
        return [
            Trade(
                id=1,
                uniqueid="DEADBEEF",
                datetime=datetime(2015, 9, 10, 15, 51, 34),
                memo="SATURNS SEARS ROEBUCK ACCEPTANCE CO",
                currency="USD",
                cash=Decimal("16195.8803475"),
                fiaccount=self.account,
                security=self.securities[1],
                units=Decimal("-1000"),
            ),
            Trade(
                id=1,
                uniqueid="DEADBEEF",
                datetime=datetime(2015, 9, 10, 15, 52, 20),
                memo="SATURNS SEARS ROEBUCK ACCEPTANCE CO",
                currency="USD",
                cash=Decimal("14576.29231275"),
                fiaccount=self.account,
                security=self.securities[1],
                units=Decimal("-900"),
            ),
        ]


class TradesWithSortFieldTestCase(TradeXmlSnippetMixin, unittest.TestCase):
    xml = """
    <Trades>
    <Trade accountId="5678" acctAlias="Test account" model="" currency="USD" fxRateToBase="1" assetCategory="STK" symbol="PPC" description="PILGRIMS PRIDE CORP-NEW" conid="71395583" securityID="" securityIDType="" cusip="" isin="" underlyingConid="" underlyingSymbol="" issuer="" multiplier="1" strike="" expiry="" putCall="" principalAdjustFactor="" tradeID="478725910" reportDate="2010-10-29" tradeDate="2010-10-29" tradeTime="143809" settleDateTarget="2010-11-03" transactionType="ExchTrade" exchange="IDEAL" quantity="-200" tradePrice="6.12" tradeMoney="-1224" proceeds="1224" taxes="0" ibCommission="-1" ibCommissionCurrency="USD" netCash="1223" closePrice="6.1" openCloseIndicator="C" notes="ML;P" cost="-1611" fifoPnlRealized="-388" fxPnl="0" mtmPnl="4" origTradePrice="0" origTradeDate="" origTradeID="" origOrderID="0" clearingFirmID="" transactionID="1920980366" buySell="SELL" ibOrderID="253174109" ibExecID="0000d323.999588c9.02.01" brokerageOrderID="" orderReference="" volatilityOrderLink="" exchOrderId="N/A" extExecID="379963350S" orderTime="2010-10-29;143809" openDateTime="" holdingPeriodDateTime="" whenRealized="" whenReopened="" levelOfDetail="EXECUTION" changeInPrice="0" changeInQuantity="0" orderType="LMT" traderID="" isAPIOrder="N" />
    </Trades>
    """

    @property
    def persisted_txs(self):
        return [
            Trade(
                id=1,
                uniqueid="DEADBEEF",
                datetime=datetime(2010, 10, 29, 14, 38, 9),
                memo="PILGRIMS PRIDE CORP-NEW",
                currency="USD",
                cash=Decimal("1223"),
                fiaccount=self.account,
                security=self.securities[0],
                units=Decimal("-200"),
                sort="MINGAIN",
            )
        ]


if __name__ == "__main__":
    unittest.main()
