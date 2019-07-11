# coding: utf-8
"""Data containers used to map IB Flex data onto the ofxtools.models interface
so we can reuse the code in capgains.ofx.reader to process Flex data.
"""
import datetime
import decimal
from typing import NamedTuple, Tuple, List, Union, Optional

import ibflex


########################################################################################
# DATA CONTAINERS - implement ofxtools.models data structures (plus some extra fields)
########################################################################################
class Account(NamedTuple):
    """Synthetic data type implementing OFX INVACCTFROM interface.
    """
    brokerid: str
    acctid: str
    # Extra fields not in OFX
    name: Optional[str]
    currency: str


class Security(NamedTuple):
    """Synthetic data type implementing OFX SECINFO interface.
    """
    uniqueidtype: str
    uniqueid: str
    secname: Optional[str]
    ticker: str


class Trade(NamedTuple):
    """Synthetic data type implementing OFX INVBUY/INVSELL interface.
    """
    fitid: Optional[str]
    dttrade: datetime.datetime
    memo: str
    uniqueidtype: str
    uniqueid: str
    units: decimal.Decimal
    currency: str
    total: decimal.Decimal
    # Extra fields not in OFX
    reportdate: datetime.date
    orig_tradeid: Optional[str]
    notes: Tuple[ibflex.enums.Code, ...]


class CashTransaction(NamedTuple):
    """Synthetic data type implementing OFX INCOME interface.

    We're really only looking for return of capital, but the base OFXReader
    class doesn't actually handle RETOFCAP since the brokers don't send it.
    Therefore we map Flex dividends to INCOME, with OFXReader does handle.
    """
    fitid: Optional[str]
    dttrade: Optional[datetime.datetime]
    dtsettle: Optional[datetime.datetime]
    memo: str
    uniqueidtype: Optional[str]
    uniqueid: Optional[str]
    incometype: str
    currency: str
    total: decimal.Decimal


class Transfer(NamedTuple):
    """Synthetic data type implementing OFX TRANSFER interface.
    """
    fitid: Optional[str]
    dttrade: datetime.datetime
    memo: Optional[str]
    uniqueidtype: Optional[str]
    uniqueid: Optional[str]
    units: decimal.Decimal
    tferaction: str
    # Extra fields not in OFX
    type: Optional[ibflex.enums.TransferType]
    other_acctid: Optional[str]


class CorporateAction(NamedTuple):
    """Synthetic data type to map Flex CorporateAction for OfxStatementReader.

    The main utility of

    Type definition analogous to OFX INVBUY/INVSELL, to more conveniently
    handle reorgs that can be treated as trades.
    """
    fitid: Optional[str]
    dttrade: datetime.datetime
    memo: str
    uniqueidtype: str
    uniqueid: str
    units: decimal.Decimal
    currency: str
    total: decimal.Decimal
    # Extra fields not in OFX
    type: ibflex.enums.Reorg
    reportdate: datetime.date
    code: Tuple[ibflex.enums.Code, ...]


class Exercise(NamedTuple):
    """Synthetic data type implementing OFX CLOSUREOPT interface.
    """
    fitid: str
    dttrade: datetime.datetime
    memo: str
    uniqueidtype: str
    uniqueid: str
    units: decimal.Decimal
    currency: str
    total: decimal.Decimal
    # Extra fields not in OFX
    uniqueidtypeFrom: str
    uniqueidFrom: str
    unitsfrom: decimal.Decimal
    reportdate: datetime.datetime
    notes: Tuple[ibflex.enums.Code, ...]


Transaction = Union[
    Trade, CashTransaction, Transfer, CorporateAction, Exercise
]


###############################################################################
# DATA CONTAINERS - Flex specific
###############################################################################
class FlexStatement(NamedTuple):
    account: Account
    securities: List[Security]
    transactions: List[Transaction]
    changeInDividendAccruals: Tuple[ibflex.Types.ChangeInDividendAccrual, ...]
    conversionRates: Tuple[ibflex.Types.ConversionRate, ...]

    def __repr__(self):
        return (
            f"{type(self).__name__}(account={self.account})"
        )
