# coding: utf-8
"""Data structures and functions for inventory Lots and Gains to prepare for
serialization and recover from deserialization.

Conversion for serialization is a two-step process.

First each Lot or Gain (nested with references to opening/closing Transactions/Lots)
is "flattened" into an un-nested intermediate sequence (FlatLot or FlatGain) holding
information needed to recreate the source instance.

Next each FlatLot or FlatGain is "exported", i.e. attributes are formatted as desired.

The export()ed data is packed (along with metadata mapping FlatLot/FlatGain attributes
to columns) into a tablib.Dataset container that provides serialization/deserialization.

Deserialization is the inverse.  Dataset rows are "imported", i.e. type-converted
from strings, then "unflattened" to reconstitute inventory Lots and Gains.

This module doesn't perform the actual reading or writing; callers handle that by
working with tablib.Dataset instances passed into/out of these functions.
"""
__all__ = [
    "FlatLot",
    "FlatGain",
    "flatten_portfolio",
    "unflatten_portfolio",
    "consolidate_lots",
    "flatten_lot",
    "unflatten_lot",
    "export_flatlot",
    "import_flatlot",
    "flatten_gains",
    "flatten_gain",
    "export_flatgain",
    "translate_gain",
    "translate_transaction",
]

# stdlib imports
from decimal import Decimal
import datetime as _datetime
from datetime import date, timedelta
import itertools
import functools
from typing import (
    Tuple,
    NamedTuple,
    Mapping,
    MutableMapping,
    Sequence,
    Union,
    Optional,
)

# 3rd part imports
import tablib
import sqlalchemy

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
    longterm: Optional[bool]
    disallowed: Optional[bool] = None


def flatten_portfolio(
    portfolio: inventory.api.PortfolioType, *, consolidate: Optional[bool] = False
) -> tablib.Dataset:
    """Convert a Portfolio into tablib.Dataset prepared for serialization.

    Columns are the fields of FlatLot; rows represent Lot instances.

    Args:
        portfolio: a mapping of (FiAccount, Security) to a sequence of Lot instances.
        consolidate: if True, sum all Lots for each (account, security) position.
    """
    dataset = tablib.Dataset(headers=FlatLot._fields)
    for (acc, sec), position in portfolio.items():
        if consolidate:
            flatlots = consolidate_lots(acc, sec, position)
        else:
            flatlots = [flatten_lot(acc, sec, lot) for lot in position]
        for flatlot in flatlots:
            row = export_flatlot(flatlot)
            units = row[6]
            if units != 0:
                dataset.append(row)
    return dataset


def unflatten_portfolio(
    session: sqlalchemy.orm.session.Session, dataset: tablib.Dataset
) -> inventory.api.Portfolio:
    """Convert a freshly-deserialized tablib.Dataset into a Portfolio.

    Args:
        session: a sqlalchemy.Session instance bound to a database engine.
        dataset: a tablib.Dataset with headers set to FlatLot._fields,
                 and all values as strings.
    """
    portfolio = inventory.api.Portfolio()
    for row in dataset:
        flatlot = import_flatlot(row)
        if flatlot.units:
            account, security, lot = unflatten_lot(session, flatlot)
            portfolio[(account, security)].append(lot)

    return portfolio


def consolidate_lots(
    account: models.FiAccount,
    security: models.Security,
    position: Sequence[inventory.types.Lot],
) -> Sequence[FlatLot]:
    """Condense a portfolio position into a single-element FlatLot sequence.

    Note:
        This function is completely irreversible; it loses all information about
        holding period and portfolio pocket.

    Args:
        account: FiAccount of position "pocket" (portfolio key).
        security: Security of position "pocket" (portfolio key).
        position: sequence of Lot instances to report.
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
    extract = [(lot.units, lot.units * lot.price, lot.currency) for lot in position]
    if not extract:
        return []
    units, cost, currency = zip(*extract)
    currency = tuple(currency)
    assert utils.all_equal(currency)  # FIXME - translate currency?

    flatlot = FlatLot(
        opendt=None,
        opentxid=None,
        units=sum(units),
        cost=sum(cost),
        currency=currency[0],
        **common_attrs,
    )
    return [flatlot]


def flatten_lot(
    account: models.FiAccount, security: models.Security, lot: inventory.types.Lot
) -> FlatLot:
    """Convert a Lot instance into unnested intermediate FlatLot representation.

    Args:
        account: FiAccount of position "pocket" (portfolio key) holding the Lot.
        security: Security of position "pocket" (portfolio key) holding the Lot.
        lot: Lot instance being flattened.
    """
    sec_attrs = {secid.uniqueidtype: secid.uniqueid for secid in security.ids}
    return FlatLot(
        brokerid=account.fi.brokerid,
        acctid=account.number,
        ticker=security.ticker,
        secname=security.name,
        opendt=lot.opentransaction.datetime,
        opentxid=lot.opentransaction.uniqueid,
        units=lot.units,
        cost=lot.units * lot.price,
        currency=lot.currency,
        **sec_attrs,
    )


def unflatten_lot(
    session: sqlalchemy.orm.session.Session, flatlot: FlatLot
) -> Tuple[models.FiAccount, models.Security, inventory.types.Lot]:
    """Convert an unnested intermediate FlatLot representation into a Lot instance.

    Args:
        session: a sqlalchemy.Session instance bound to a database engine.
        flatlot: FlatLot instance holding the import()ed Lot data
                 (already type-converted from strings).
    """
    account = models.FiAccount.merge(
        session, brokerid=flatlot.brokerid, number=flatlot.acctid
    )
    assert flatlot.opentxid is not None
    assert flatlot.opendt is not None

    # Create mock opentransaction
    opentransaction = inventory.types.DummyTransaction(
        uniqueid=flatlot.opentxid,
        datetime=flatlot.opendt,
        fiaccount=None,
        security=None,
        type=models.TransactionType.TRADE,
    )

    for uniqueidtype in ("CUSIP", "ISIN", "CONID", "TICKER"):
        uniqueid = getattr(flatlot, uniqueidtype)
        if uniqueid:
            security = models.Security.merge(
                session,
                uniqueidtype=uniqueidtype,
                uniqueid=uniqueid,
                ticker=flatlot.ticker,
                name=flatlot.secname,
            )

    assert isinstance(flatlot.currency, models.Currency)
    lot = inventory.types.Lot(
        units=flatlot.units,
        price=flatlot.cost / flatlot.units,
        opentransaction=opentransaction,
        createtransaction=opentransaction,
        currency=flatlot.currency,
    )

    return account, security, lot


def export_flatlot(flatlot: FlatLot) -> Tuple:
    """Convert FlatLot into a row (tuple) ready for serialization.

    Do the minimum work such that the values look right when tablib.Dataset
    type-converts them during serialization.

    Args:
        flatlot: fully-populated FlatLot instance.
    """
    # As of Python 3.7:
    #  """Dictionaries preserve insertion order. Note that updating a key does not
    #  affect the order."""
    #  https://docs.python.org/3.7/library/stdtypes.html#dict
    attrs = flatlot._asdict()
    attrs.update(
        {
            "units": utils.round_decimal(attrs["units"], power=-2),
            "cost": utils.round_decimal(attrs["cost"], power=-2),
            "currency": attrs["currency"].name,
        }
    )
    row = tuple(attrs.values())
    return row


def import_flatlot(row: Tuple) -> FlatLot:
    """Convert a freshly-deserialized tablib.Dataset row into an intermediate FlatLot.

    Note:
        This function is not the inverse of export_flatlot().  Input tuple may not
        have the right types for numeric and date/time values, whereas the output of
        export_flatlot() retains the FlatLot attribute type (except currrency).

    Args:
        row: tuple whose values correspond to FlatLot._fields.
    """
    attrs = dict(zip(FlatLot._fields, row))

    opendt = attrs["opendt"]
    if isinstance(opendt, _datetime.datetime):
        opendt_ = opendt
    else:
        assert isinstance(opendt, str)
        opendt_ = _datetime.datetime.fromisoformat(opendt)

    attrs.update(
        {
            "opendt": opendt_,
            "units": Decimal(attrs["units"]),
            "cost": Decimal(attrs["cost"]),
            "currency": getattr(models.Currency, attrs["currency"]),
        }
    )
    return FlatLot(**attrs)


def flatten_gains(
    session: sqlalchemy.orm.Session,
    gains: Sequence[inventory.api.Gain],
    *,
    consolidate: Optional[bool] = False,
) -> tablib.Dataset:
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

        rows = []

        for secid, gs in itertools.groupby(sorted(gains, key=keyfunc), key=keyfunc):
            flatgains = (flatten_gain(session, gain) for gain in gs)
            totals = itertools.accumulate(flatgains, accum)
            rows.append(export_flatgain(list(totals)[-1]))
    else:
        rows = (export_flatgain(flatten_gain(session, gain)) for gain in gains)

    data = tablib.Dataset(headers=FlatGain._fields)
    for row in rows:
        units = row[8]
        assert FlatGain._fields[8] == "units"
        if units != 0:
            data.append(row)
    return data


def flatten_gain(
    session: sqlalchemy.orm.session.Session, gain: inventory.types.Gain
) -> FlatGain:
    """Construct an unnested intermediate FlatGain from a Gain instance.

    Translate currency of opening/closing transactions to functional currency as needed.

    Args:
        session: a sqlalchemy.Session instance bound to a database engine.
        gain: Gain instance to flatten.
    """
    gain = translate_gain(session, gain)
    gaintx = gain.transaction
    fiaccount = gaintx.fiaccount
    security = gaintx.security

    lot = gain.lot
    units = lot.units
    opentx = lot.opentransaction

    opendt = opentx.datetime
    gaindt = gaintx.datetime

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
        longterm=utils.realize_longterm(units, opendt, gaindt),
        disallowed=None,
    )


def export_flatgain(flatgain: FlatGain) -> Tuple:
    """Convert FlatGain into a row (tuple) ready for serialization.

    Do the minimum work such that the values look right when tablib.Dataset
    type-converts them during serialization.

    Args:
        flatgain: fully-populated FlatGain instance.
    """
    # As of Python 3.7:
    #  """Dictionaries preserve insertion order. Note that updating a key does not
    #  affect the order."""
    #  https://docs.python.org/3.7/library/stdtypes.html#dict
    attrs = flatgain._asdict()
    attrs.update(
        {
            "units": utils.round_decimal(attrs["units"], power=-2),
            "proceeds": utils.round_decimal(attrs["proceeds"], power=-2),
            "cost": utils.round_decimal(attrs["cost"], power=-2),
            "currency": attrs["currency"].name,
        }
    )
    row = tuple(attrs.values())
    return row


FUNCTIONAL_CURRENCY = getattr(models.Currency, CONFIG["books"]["functional_currency"])


def translate_gain(
    session: sqlalchemy.orm.session.Session, gain: inventory.types.Gain
) -> inventory.types.Gain:
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

    Args - cf. translate_transaction() docstring.
    """
    cash = _scaleAttr(transaction, "cash", rate)
    assert cash is not None
    return transaction._replace(cash=cash, currency=currency)


@translate_transaction.register
def translate_security_pricing(
    transaction: inventory.Spinoff, currency: models.Currency, rate: Decimal
) -> inventory.Spinoff:
    """Translate transaction security pricing into a different currency.

    Args - cf. translate_transaction() docstring.
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

    Args - cf. translate_transaction() docstring
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
