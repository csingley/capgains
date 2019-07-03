# coding: utf-8
"""
"""
from datetime import datetime as datetime_, date as date_, time as time_
from decimal import Decimal
from typing import NamedTuple, Tuple, List, Dict, Union, Optional


########################################################################################
# DATA CONTAINERS - implement ofxtools.models data structures (plus some extra fields)
########################################################################################
class Account(NamedTuple):
    acctid: str
    brokerid: str
    name: str
    currency: str


class Security(NamedTuple):
    uniqueidtype: str
    uniqueid: str
    ticker: str
    secname: str


class Dividend(NamedTuple):
    conid: str
    exDate: datetime_
    payDate: datetime_
    quantity: Decimal
    grossRate: Decimal
    taxesAndFees: Decimal
    total: Decimal


class Trade(NamedTuple):
    fitid: str
    dttrade: datetime_
    memo: str
    uniqueidtype: str
    uniqueid: str
    units: Decimal
    currency: str
    total: Decimal
    reportdate: datetime_
    orig_tradeid: str
    notes: str


class CashTransaction(NamedTuple):
    fitid: str
    dttrade: Optional[datetime_]
    dtsettle: Optional[datetime_]
    memo: str
    uniqueidtype: str
    uniqueid: str
    incometype: str
    currency: str
    total: Decimal


class Transfer(NamedTuple):
    fitid: Optional[str]
    dttrade: datetime_
    memo: str
    uniqueidtype: str
    uniqueid: str
    units: Decimal
    tferaction: str
    type: str
    other_acctid: str


class CorporateAction(NamedTuple):
    fitid: Optional[str]
    dttrade: datetime_
    memo: str
    uniqueidtype: str
    uniqueid: str
    units: Decimal
    currency: str
    total: Decimal
    type: str
    reportdate: datetime_
    code: str


class Exercise(NamedTuple):
    fitid: str
    dttrade: datetime_
    memo: str
    uniqueidtype: str
    uniqueid: str
    units: Decimal
    uniqueidtypeFrom: str
    uniqueidFrom: str
    unitsfrom: Decimal
    currency: str
    total: Decimal
    reportdate: datetime_
    notes: str


Transaction = Union[
    Dividend, Trade, CashTransaction, Transfer, CorporateAction, Exercise
]


###############################################################################
# DATA CONTAINERS - Flex specific
###############################################################################
class ConversionRate(NamedTuple):
    date: date_
    fromcurrency: str
    tocurrency: str
    rate: Decimal


class FlexStatement(NamedTuple):
    account: Account
    securities: List[Security]
    dividends: List[Dividend]
    transactions: List[Transaction]
    conversionrates: List[ConversionRate]


###############################################################################
# ibflex types
###############################################################################
from mypy_extensions import TypedDict


AccountInformationDict = TypedDict(
    "AccountInformationDict",
    {
        "accountId": str,
        "acctAlias": str,
        "currency": str,
        "name": str,
        "accountType": str,
        "customerType": str,
        "accountCapabilities": str,
        "tradingPermissions": str,
        "dateOpened": date_,
        "dateFunded": date_,
        "dateClosed": date_,
        "masterName": str,
        "ibEntity": str,
    }
)


FxLot = TypedDict(
    "FxLot",
    {
        "assetCategory": str,
        "reportDate": date_,
        "functionalCurrency": str,
        "fxCurrency": str,
        "quantity": Decimal,
        "costPrice": Decimal,
        "costBasis": Decimal,
        "closePrice": Decimal,
        "value": Decimal,
        "unrealizedPL": Decimal,
        "code": List[str],
        "lotDescription": str,
        "lotOpenDateTime": datetime_,
        "levelOfDetail": str,
    }
)


ConversionRatesDict = Dict[Tuple[str, str, date_], Decimal]

# FIXME
ChangeInNavDict = dict


FlexStatementDict = TypedDict(
    "FlexStatementDict",
    {
        "accountId": str,
        "fromDate": date_,
        "toDate": date_,
        "period": str,
        "whenGenerated": datetime_,
        "AccountInformation": AccountInformationDict,
        "ConversionRates": ConversionRatesDict,
        "FxPositions": List[FxLot],
        "ChangeInNAV": ChangeInNavDict,
    }
)


FlexQueryResponseDict = TypedDict(
    "FlexQueryResponseDict",
    {
        "queryName": str,
        "type": str,
        "FlexStatements": List[FlexStatementDict],
    }
)

#  TradeDict = dict
TradeDict = TypedDict(
    "TradeDict",
    {
        # AccountMixin
        "accountId": str,
        "acctAlias": str,
        "model": str,
        # CurrencyMixin
        "currency": str,
        "fxRateToBase": Decimal,
        # SecurityMixin
        "assetCategory": str,
        "symbol": str,
        "description": str,
        "conid": str,
        "securityID": str,
        "securityIDType": str,
        "cusip": str,
        "isin": str,
        "underlyingConid": str,
        "underlyingSymbol": str,
        "issuer": str,
        "multiplier": Decimal,
        "strike": Decimal,
        "expiry": date_,
        "putCall": str,
        "principalAdjustFactor": Decimal,
        # TradeMixin
        "tradeID": str,
        "reportDate": date_,
        "tradeDate": date_,
        "tradeTime": time_,
        "settleDateTarget": date_,
        "transactionType": str,
        "exchange": str,
        "quantity": Decimal,
        "tradePrice": Decimal,
        "tradeMoney": Decimal,
        "proceeds": Decimal,
        "taxes": Decimal,
        "ibCommission": Decimal,
        "ibCommissionCurrency": str,
        "netCash": Decimal,
        "closePrice": Decimal,
        "openCloseIndicator": str,
        "notes": List,
        "cost": Decimal,
        "fifoPnlRealized": Decimal,
        "fxPnl": Decimal,
        "mtmPnl": Decimal,
        "origTradePrice": Decimal,
        "origTradeDate": date_,
        "origTradeID": str,
        "origOrderID": str,
        "clearingFirmID": str,
        "transactionID": str,
        "openDateTime": datetime_,
        "holdingPeriodDateTime": datetime_,
        "whenRealized": datetime_,
        "whenReopened": datetime_,
        "levelOfDetail": str,
        # Trade
        "buySell": str,
        "ibOrderID": str,
        "ibExecID": str,
        "brokerageOrderID": str,
        "orderReference": str,
        "volatilityOrderLink": str,
        "exchOrderId": str,
        "extExecID": str,
        # Despite the name, orderTime actually contains both date & time data.
        "orderTime": datetime_,
        "changeInPrice": Decimal,
        "changeInQuantity": Decimal,
        "orderType": str,
        "traderID": str,
        "isAPIOrder": bool,
    }
)


# FIXME
TradeTransferDict = dict
CashTransactionDict = dict
CorporateActionDict = dict
TransferDict = dict
OptionEaeDict = dict
