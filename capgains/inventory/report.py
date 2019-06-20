# coding: utf-8
"""Data structures and functions to (de)serialize inventory Lots and Gains.
"""
__all__ = [
    "FlatLot",
    "FlatGain",
    "flatten_portfolio",
    "flatten_position",
    "flatten_gains",
    "flatten_gain",
    "translate_gain",
    "translate_transaction",
]

# stdlib imports
from decimal import Decimal
import datetime as _datetime
from datetime import date, timedelta
import itertools
import functools
from typing import NamedTuple, Sequence, Union, Optional

# 3rd part imports
import tablib

# local imports
from capgains import models, inventory, utils, CONFIG


class FlatLot(NamedTuple):
    """Un-nested container for Lot data, suitable for serialization.

    Attributes:
        brokerid: OFX <FI><BROKERID>.
        acctid: brokerage account #.
        ticker: security symbol.
        secname: security description.
        opendt: date/time of Lot's opening transaction.
        opentxid: uniqueid of Lot's opening transaction.
        units: amount of security comprising the Lot.
        cost: cost basis of Lot.
        currency: denomination of cost basis.
        CUSIP: Committee on Uniform Securities Identification Procedures identifier.
        ISIN: International Securities Identification Number.
        CONID: Interactive Brokers contract identifier.
        TICKER: security symbol, as sourced from CSV file.
    """

    brokerid: str
    acctid: str
    ticker: str
    secname: str
    opendt: Optional[_datetime.datetime]
    opentxid: Optional[str]
    units: Decimal
    cost: Decimal
    currency: models.Currency
    CUSIP: Optional[str] = None
    ISIN: Optional[str] = None
    CONID: Optional[str] = None
    TICKER: Optional[str] = None


class FlatGain(NamedTuple):
    """Un-nested container for Gain data, suitable for serialization.

    Order of attributes defines column order of serialized data.

    Attributes:
        brokerid: OFX <FI><BROKERID>.
        acctid: brokerage account #.
        ticker: security symbol.
        secname: security description.
        opendt: date/time of opening transaction.
        opentxid: uniqueid of opening transaction.
        gaindt: date/time of realizing transaction.
        gaintxid: uniqueid of realizing transaction.
        units: amount of security being realized.
        proceeds: money amount of realization (technically cost not proceeds for short).
        cost: basis of Lot (technically proceeds not cost for short).
        currency: denomination of cost basis.
        longterm: if True, signals long-term treatment for capital gain/loss.
        disallowed: if True, signals a wash sale (not yet implemented).
    """

    brokerid: str
    acctid: str
    ticker: str
    secname: str
    opendt: Optional[_datetime.datetime]
    opentxid: Optional[str]
    gaindt: Optional[_datetime.datetime]
    gaintxid: Optional[str]
    units: Decimal
    proceeds: Decimal
    cost: Decimal
    currency: models.Currency
    longterm: bool
    disallowed: Optional[bool] = None


def flatten_portfolio(
    portfolio: inventory.api.PortfolioType, *, consolidate: bool = False
) -> Sequence[FlatLot]:
    """Convert a Portfolio into tablib.Dataset prepared for serialization.

    Columns are the fields of FlatLot; rows represent Lot instances.

    Args:
        portfolio: a mapping of (FiAccount, Security) to a sequence of Lot instances.
        consolidate: if True, sum all Lots for each (account, security) position.
    """
    data = tablib.Dataset(headers=FlatLot._fields)
    for (acc, sec), position in portfolio.items():
        report = flatten_position(acc, sec, position, consolidate=consolidate)
        for item in report:
            data.append(item)
    return data


def flatten_position(
    account: models.FiAccount,
    security: models.Security,
    position: Sequence[inventory.types.Lot],
    *,
    consolidate: bool = False,
) -> Sequence[FlatLot]:
    """Construct a sequence of LotReports from a portfolio position.

    Args:
        account: FiAccount of position "pocket" (portfolio key).
        security: Security of position "pocket" (portfolio key).
        position: sequence of Lot instances to report.
        consolidate: if True, sum all Lots for the position.
    """
    common_attrs = {secid.uniqueidtype: secid.uniqueid for secid in security.ids}
    common_attrs.update(
        {
            "brokerid": account.fi.brokerid,
            "acctid": account.number,
            "ticker": security.ticker,
            "secname": security.name,
        }
    )
    if consolidate:
        extract = [(lot.units, lot.units * lot.price, lot.currency) for lot in position]
        if not extract:
            return []
        units, cost, currency = zip(*extract)
        currency = tuple(currency)
        assert utils.all_equal(currency)  # FIXME

        flatlot = FlatLot(
            opendt=None,
            opentxid=None,
            units=utils.round_decimal(sum(units)),
            cost=utils.round_decimal(sum(cost)),
            currency=currency[0],
            **common_attrs,
        )
        return [flatlot]

    return [
        FlatLot(
            opendt=lot.opentransaction.datetime,
            opentxid=lot.opentransaction.uniqueid,
            units=utils.round_decimal(lot.units),
            cost=utils.round_decimal(lot.units * lot.price),
            currency=lot.currency,
            **common_attrs,
        )
        for lot in position
    ]


def flatten_gains(
    session, gains: Sequence[inventory.api.Gain], *, consolidate: bool = False
) -> Sequence[FlatLot]:
    """Convert a sequence of Gains into tablib.Dataset prepared for serialization.

    Columns are the fields of FlatGain; rows represent Gain instances.

    Args:
        session: a sqlalchemy.Session instance bound to a database engine.
        gains: sequence of Gain instances.
        consolidate: if True, sum all Lots for each (account, security) position.
    """
    if consolidate:

        def keyfunc(gain):
            return gain.transaction.security.id

        def accum(report0, report1):
            assert report0.ticker == report1.ticker
            assert report0.secname == report1.secname
            #  FIXME - convert currency?
            assert report0.currency == report1.currency
            return FlatGain(
                brokerid=None,
                acctid=None,
                ticker=report0.ticker,
                secname=report0.secname,
                opendt=None,
                opentxid=None,
                gaindt=None,
                gaintxid=None,
                units=report0.units + report1.units,
                proceeds=report0.proceeds + report1.proceeds,
                cost=report0.cost + report1.cost,
                currency=report0.currency,
                longterm=None,
                disallowed=None,
            )

        flatgains = []

        for secid, gs in itertools.groupby(sorted(gains, key=keyfunc), key=keyfunc):
            reports_ = (flatten_gain(session, gain) for gain in gs)
            totals = itertools.accumulate(reports_, accum)
            flatgains.append(list(totals)[-1])
    else:
        flatgains = [flatten_gain(session, gain) for gain in gains]

    data = tablib.Dataset(headers=FlatGain._fields)
    for item in flatgains:
        data.append(item)
    return data


def flatten_gain(session, gain: inventory.types.Gain) -> FlatGain:
    """Construct a FlatGain from a Gain instance.

    Translate currency of opening/closing transactions to functional currency as needed.

    Args:
        session: a sqlalchemy.Session instance bound to a database engine.
        gain: Gain instance to flatten.
    """
    gain = translate_gain(session, gain)
    gaintx = gain.transaction
    lot = gain.lot
    units = lot.units

    fiaccount = gaintx.fiaccount
    security = gaintx.security
    opentx = lot.opentransaction

    # Short sales never get long-term capital gains treatment
    gaindt = gaintx.datetime
    opendt = opentx.datetime
    longterm = (units > 0) and (gaindt - opendt >= timedelta(days=366))

    return FlatGain(
        brokerid=fiaccount.fi.brokerid,
        acctid=fiaccount.number,
        ticker=security.ticker,
        secname=security.name,
        opendt=opendt,
        opentxid=opentx.uniqueid,
        gaindt=gaindt,
        gaintxid=gaintx.uniqueid,
        units=units,
        proceeds=units * gain.price,
        cost=units * lot.price,
        currency=lot.currency,
        longterm=longterm,
        disallowed=None,
    )


FUNCTIONAL_CURRENCY = getattr(models.Currency, CONFIG["books"]["functional_currency"])


def translate_gain(session, gain: inventory.types.Gain) -> inventory.types.Gain:
    """Translate Gain instance's realizing transaction to functional currency.

    26 CFR Section 1.988-2(a)(2)(iv)
    '''
    (A) Amount realized. If stock or securities traded on an established securities
    market are sold by a cash basis taxpayer for nonfunctional currency, the amount
    realized with respect to the stock or securities (as determined on the trade date)
    shall be computed by translating the units of nonfunctional currency received into
    functional currency at the spot rate on the _settlement date_ of the sale.
    ...
    (B) Basis. If stock or securities traded on an established securities market are
    purchased by a cash basis taxpayer for nonfunctional currency, the basis of the
    stock or securities shall be determined by translating the units of nonfunctional
    currency paid into functional currency at the spot rate on the _settlement date_
    of the purchase.
    '''

    Args:
        session: a sqlalchemy.Session instance bound to a database engine.
        gain: Gain instance to translate.
    """
    lot, gaintx, gainprice = gain.lot, gain.transaction, gain.price

    if lot.currency != FUNCTIONAL_CURRENCY:
        opentx = lot.opentransaction
        dtsettle = getattr(opentx, "dtsettle", opentx.datetime) or opentx.datetime
        date_settle = date(dtsettle.year, dtsettle.month, dtsettle.day)
        exchange_rate = models.CurrencyRate.get_rate(
            session,
            fromcurrency=lot.currency,
            tocurrency=FUNCTIONAL_CURRENCY,
            date=date_settle,
        )
        opentx_translated = translate_transaction(
            opentx, FUNCTIONAL_CURRENCY, exchange_rate
        )
        lot = lot._replace(
            opentransaction=opentx_translated,
            price=lot.price * exchange_rate,
            currency=FUNCTIONAL_CURRENCY,
        )

    gaintx_currency = gaintx.currency or lot.currency
    if gaintx_currency != FUNCTIONAL_CURRENCY:
        dtsettle = gaintx.dtsettle or gaintx.datetime
        date_settle = date(dtsettle.year, dtsettle.month, dtsettle.day)
        exchange_rate = models.CurrencyRate.get_rate(
            session,
            fromcurrency=gaintx_currency,
            tocurrency=FUNCTIONAL_CURRENCY,
            date=date_settle,
        )

        gaintx = translate_transaction(gaintx, FUNCTIONAL_CURRENCY, exchange_rate)
        gainprice = gainprice * exchange_rate

    return inventory.Gain(lot, gaintx, gainprice)


@functools.singledispatch
def translate_transaction(
    transaction: inventory.types.TransactionType,
    currency: models.Currency,
    rate: Decimal,
) -> inventory.types.TransactionType:
    """Translate a transaction into a different currency for reporting purposes.

    By default, return the transaction unmodified.

    Args:
        transaction: transaction instance to translate.
        currency: destination currency (i.e. desired currency post-translation)
        rate: numerator is destination currency, denominator is source currency.
    """
    return transaction


CashTransaction = Union[inventory.Trade, inventory.ReturnOfCapital, inventory.Exercise]


@translate_transaction.register(inventory.Trade)
@translate_transaction.register(inventory.ReturnOfCapital)
@translate_transaction.register(inventory.Exercise)
def translate_cash_currency(
    transaction: CashTransaction, currency: models.Currency, rate: Decimal
) -> CashTransaction:
    """Translate transaction cash into a different currency.

    Args:
        cf. translate_transaction() docstring.
    """
    cash = _scaleAttr(transaction, "cash", rate)
    assert cash is not None
    return transaction._replace(cash=cash, currency=currency)


@translate_transaction.register
def translate_security_pricing(
    transaction: inventory.Spinoff, currency: models.Currency, rate: Decimal
) -> inventory.Spinoff:
    """Translate transaction security pricing into a different currency.

    Args:
        cf. translate_transaction() docstring.
    """

    return transaction._replace(
        securityprice=_scaleAttr(transaction, "securityprice", rate),
        fromsecurityprice=_scaleAttr(transaction, "fromsecurityprice", rate),
    )


@translate_transaction.register
def translate_model(
    transaction: models.Transaction, currency: models.Currency, rate: Decimal
) -> inventory.types.DummyTransaction:
    """Translate a transaction into a different currency for reporting purposes.

    Args:
        cf. translate_transaction() docstring
    """

    return inventory.types.DummyTransaction(
        uniqueid=transaction.uniqueid,
        datetime=transaction.datetime,
        dtsettle=transaction.dtsettle,
        type=transaction.type,
        memo=transaction.memo,
        currency=currency,
        cash=_scaleAttr(transaction, "cash", rate),
        fiaccount=transaction.fiaccount,
        security=transaction.security,
        units=transaction.units,
        securityprice=_scaleAttr(transaction, "securityprice", rate),
        fromfiaccount=transaction.fromfiaccount,
        fromunits=transaction.fromunits,
        fromsecurityprice=_scaleAttr(transaction, "fromsecurityprice", rate),
        numerator=transaction.numerator,
        denominator=transaction.denominator,
        sort=transaction.sort,
    )


def _scaleAttr(instance: object, attr: str, coefficient: Decimal) -> Optional[Decimal]:
    """Multiply an object attribute value by some scaling factor.

    Args:
        instance: object instance.
        attr: name of attribute holding value to scale.
        coefficient: the scaling factor.
    """
    value = getattr(instance, attr)
    if value is not None:
        value *= coefficient
    return value
