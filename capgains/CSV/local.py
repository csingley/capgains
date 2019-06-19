"""
CSV readers/writers for internal use by capgains,
not to import data from external sources.

This module provides the ability to dump/load Transactions, Lots, and Gains
to & from CSV files.
"""
# stdlib imports
import csv
from decimal import Decimal, ROUND_HALF_UP
import datetime as _datetime
from datetime import datetime, date, timedelta
import functools
import itertools
from typing import Tuple, NamedTuple, Sequence, Mapping, Callable, Any, Union, Optional


# local imports
from capgains import models, inventory, CONFIG


PocketType = Tuple[models.FiAccount, models.Security]
PositionType = Sequence[inventory.Lot]
PortfolioType = Mapping[PocketType, PositionType]
CsvDictRowRead = Mapping[str, Optional[str]]
CsvDictRowWrite = Mapping[str, Union[None, str, Decimal, datetime, bool]]


class CsvTransactionReader(csv.DictReader):
    def __init__(self, session, csvfile):
        self.session = session
        super(CsvTransactionReader, self).__init__(csvfile)

    def read(self):
        return [self.read_row(row) for row in self]

    def read_row(self, row: Mapping[str, Optional[str]]) -> models.Transaction:
        row = {k: v or None for k, v in row.items()}
        attrs = {
            attr: converter(row, attr) for attr, converter in self.converters.items()
        }
        return models.Transaction.merge(self.session, **attrs)

    def convertString(self, row: CsvDictRowRead, attr: str) -> Optional[str]:
        return row[attr.lower()] or None

    def convertDecimal(self, row: CsvDictRowRead, attr: str) -> Decimal:
        return self._convertItem(row, attr, Decimal)

    def convertDatetime(self, row: CsvDictRowRead, attr: str) -> datetime:
        return self._convertItem(row, attr, datetime.fromisoformat)

    def convertType(self, row: CsvDictRowRead, attr: str) -> models.TransactionType:
        return getattr(models.TransactionType, attr)

    def convertSort(self, row: CsvDictRowRead, attr: str) -> models.TransactionSort:
        return getattr(models.TransactionSort, attr)

    def convertAccount(
        self, row: CsvDictRowRead, attr: str
    ) -> Optional[models.FiAccount]:
        value = None
        csv_attr = attr.lower()
        attrs = {att: row["_".join((csv_attr, att))] for att in ("brokerid", "number")}
        if attrs["brokerid"] is not None:
            value = models.FiAccount.merge(self.session, **attrs)

        return value

    def convertSecurity(
        self, row: CsvDictRowRead, attr: str
    ) -> Optional[models.Security]:
        value = None
        csv_attr = attr.lower()
        attrs = {
            att: row["_".join((csv_attr, att))]
            for att in ("uniqueidtype", "uniqueid", "ticker", "name")
        }
        if attrs["uniqueidtype"] is not None:
            value = models.Security.merge(self.session, **attrs)

        return value

    def _convertItem(self, row: CsvDictRowRead, attr: str, fn: Callable) -> Any:
        value = row[attr.lower()]
        if value is not None:
            value = fn(value)
        return value

    converters = {
        "uniqueid": convertString,
        "datetime": convertDatetime,
        "dtsettle": convertDatetime,
        "type": convertType,
        "memo": convertString,
        "currency": convertString,
        "cash": convertDecimal,
        "fiaccount": convertAccount,
        "security": convertSecurity,
        "units": convertDecimal,
        "securityprice": convertDecimal,
        "fromfiaccount": convertAccount,
        "securityFrom": convertSecurity,
        "fromunits": convertDecimal,
        "fromsecurityprice": convertDecimal,
        "numerator": convertDecimal,
        "denominator": convertDecimal,
        "sort": convertSort,
    }


class CsvTransactionWriter(csv.DictWriter):
    csvFields = [
        "uniqueid",
        "datetime",
        "dtsettle",
        "type",
        "memo",
        "currency",
        "cash",
        "fiaccount_brokerid",
        "fiaccount_number",
        "security_uniqueidtype",
        "security_uniqueid",
        "security_ticker",
        "security_name",
        "units",
        "securityprice",
        "fromfiaccount_brokerid",
        "fromfiaccount_number",
        "fromsecurity_uniqueidtype",
        "fromsecurity_uniqueid",
        "fromsecurity_ticker",
        "fromsecurity_name",
        "fromunits",
        "fromsecurityprice",
        "numerator",
        "denominator",
        "sort",
    ]

    def __init__(self, session, csvfile):
        self.session = session
        self.csvfile = csvfile

        super(CsvTransactionWriter, self).__init__(
            csvfile, self.csvFields, delimiter=",", quoting=csv.QUOTE_NONNUMERIC
        )

    def writerows(  # type: ignore
        self, transactions: Sequence[models.Transaction]
    ) -> None:
        """ """
        for transaction in transactions:
            # Mandatory fields
            row = self.unconvert(transaction)
            self.writerow(row)

    def unconvert(self, transaction: models.Transaction) -> CsvDictRowWrite:
        row = {
            "uniqueid": transaction.uniqueid,
            "datetime": transaction.datetime.isoformat(),
            "type": transaction.type.name,
            "memo": transaction.memo,
            "currency": transaction.currency,
            "cash": transaction.cash,
            "units": transaction.units,
            "securityprice": transaction.securityprice,
            "fromunits": transaction.fromunits,
            "fromsecurityprice": transaction.fromsecurityprice,
            "numerator": transaction.numerator,
            "denominator": transaction.denominator,
        }
        row.update(self.unconvert_account(transaction, "fiaccount", required=True))
        row.update(self.unconvert_security(transaction, "security", required=True))
        row.update(self.unconvert_account(transaction, "fromfiaccount"))
        row.update(self.unconvert_security(transaction, "securityFrom"))

        sort = transaction.sort
        if sort is not None:
            row.update({"sort": sort.name})

        return row

    def unconvert_account(
        self, transaction: models.Transaction, attr: str, required: bool = False
    ) -> Mapping[str, str]:
        account = getattr(transaction, attr)
        if account is None:
            if required:
                raise ValueError(f"Empty {attr} in {transaction}")
            return {}
        csv_attr = attr.lower()
        return {
            f"{csv_attr}_brokerid": account.fi.brokerid,
            f"{csv_attr}_number": account.number,
        }

    def unconvert_security(
        self, transaction: models.Transaction, attr: str, required: bool = False
    ) -> Mapping[str, str]:
        security = getattr(transaction, attr)
        if security is None:
            if required:
                raise ValueError(f"Empty {attr} in {transaction}")
            return {}

        # Prefer any uniqueidtype other than `TICKER`
        security_ids = sorted(
            security.ids, key=lambda x: x.uniqueidtype.lower() == "ticker"
        )
        security_id = security_ids[0]

        csv_attr = attr.lower()
        return {
            f"{csv_attr}_uniqueidtype": security_id.uniqueidtype,
            f"{csv_attr}_uniqueid": security_id.uniqueid,
            f"{csv_attr}_ticker": security.ticker,
            f"{csv_attr}_name": security.name,
        }


class CsvLotReader(csv.DictReader):
    def __init__(self, session, csvfile):
        self.session = session
        # Transaction ID for mock Lot.opentransaction
        self.transaction_id = 999999
        super(CsvLotReader, self).__init__(csvfile)

    def __next__(self):
        row = super(CsvLotReader, self).__next__()

        acct_attrs = {attr: row.pop(attr) for attr in ("brokerid", "acctid")}
        sec_attrs = {attr: row.pop(attr) for attr in ("ticker", "secname")}
        lot_attrs = {attr: row.pop(attr) for attr in ("units", "cost", "currency")}
        # Create mock opentransaction
        opendt = datetime.strptime(row.pop("opendt"), "%Y-%m-%d %H:%M:%S")
        opentxid = row.pop("opentxid")
        opentransaction = inventory.Trade(
            uniqueid=opentxid,
            datetime=opendt,
            fiaccount=None,
            security=None,
            currency=None,
            cash=None,
            units=None,
        )

        # Leftovers in row are SecurityId
        for uniqueidtype, uniqueid in row.items():
            if uniqueid:
                security = models.Security.merge(
                    self.session,
                    uniqueidtype=uniqueidtype,
                    uniqueid=uniqueid,
                    ticker=sec_attrs["ticker"],
                    name=sec_attrs["secname"],
                )
        account = models.FiAccount.merge(
            self.session, brokerid=acct_attrs["brokerid"], number=acct_attrs["acctid"]
        )
        lot_attrs["units"] = Decimal(lot_attrs["units"])
        lot_attrs["price"] = Decimal(lot_attrs.pop("cost")) / lot_attrs["units"]
        lot_attrs["opentransaction"] = opentransaction
        lot_attrs["createtransaction"] = opentransaction

        # `yield` returns a generator object; if you want to use it directly
        # instead of iterating over it, you need to call list() or tuple()
        # or somesuch.
        #
        #  `yield account, security, lot_attrs` gives one tuple, whereas
        #  `yield account; yield security; yield lot_attrs` gives the
        #  individual objects.  Since we're going to be calling tuple() on
        #  the returned generator object, we'll use the latter format in order
        #  to avoid annoying nested tuples.
        #  yield account, security, inventory.Lot(**lot_attrs)
        yield account
        yield security
        yield inventory.Lot(**lot_attrs)


class CsvLotWriter(csv.DictWriter):
    csvFields = [
        "brokerid",
        "acctid",
        "ticker",
        "secname",
        "opendt",
        "opentxid",
        "units",
        "cost",
        "currency",
    ]

    def __init__(self, session, csvfile):
        self.session = session
        self.csvfile = csvfile

        uniqueidtypes = [
            d[0] for d in session.query(models.SecurityId.uniqueidtype).distinct()
        ]
        fieldnames = self.csvFields + uniqueidtypes
        super(CsvLotWriter, self).__init__(
            csvfile,
            fieldnames,
            delimiter=",",
            quoting=csv.QUOTE_NONNUMERIC,
            extrasaction="ignore",
        )

    def writerows(  # type: ignore
        self, portfolio: PortfolioType, consolidate: bool = False
    ) -> None:
        """ """
        for (account, security), position in portfolio.items():
            if not position:
                continue
            self.session.add_all([account, security])

            rows = [self.row_for_lot(account, security, lot) for lot in position]

            if consolidate:
                rows = [functools.reduce(self.consolidate_lots, rows)]

            for row in rows:
                self.writerow(row)

    def row_for_lot(
        self, account: models.FiAccount, security: models.Security, lot: inventory.Lot
    ) -> CsvDictRowWrite:

        row = {secid.uniqueidtype: secid.uniqueid for secid in security.ids}
        row.update(
            {
                "brokerid": account.fi.brokerid,
                "acctid": account.number,
                "ticker": security.ticker,
                "secname": security.name,
            }
        )

        def decimal_round(number: Decimal) -> Decimal:
            PRECISION = Decimal("0.0001")
            return Decimal(number.quantize(PRECISION, rounding=ROUND_HALF_UP))

        row.update(
            {
                "units": decimal_round(lot.units),
                "cost": decimal_round(lot.units * lot.price),
                "currency": lot.currency,
                "opendt": lot.opentransaction.datetime,
                "opentxid": lot.opentransaction.uniqueid,
            }
        )
        return row

    @staticmethod
    def consolidate_lots(
        lot0: CsvDictRowWrite, lot1: CsvDictRowWrite
    ) -> CsvDictRowWrite:
        assert isinstance(lot0["units"], Decimal)
        assert isinstance(lot0["cost"], Decimal)
        assert isinstance(lot1["units"], Decimal)
        assert isinstance(lot1["cost"], Decimal)

        assert lot0["currency"] == lot1["currency"]  # FIXME

        consolidated_lot = lot0.copy()
        consolidated_lot.update(
            {
                "brokerid": "",
                "acctid": "",
                "ticker": lot0["ticker"],
                "secname": lot0["secname"],
                "units": lot0["units"] + lot1["units"],
                "cost": lot0["cost"] + lot1["cost"],
                "currency": lot0["currency"],
                "opendt": "",
                "opentxid": "",
            }
        )

        return consolidated_lot


class CsvGainWriter(csv.DictWriter):
    fieldnames = [
        "brokerid",
        "acctid",
        "ticker",
        "secname",
        "gaindt",
        "gaintxid",
        "ltcg",
        "opendt",
        "opentxid",
        "units",
        "proceeds",
        "cost",
        "currency",
        "realized",
        "disallowed",
    ]

    def __init__(self, session, csvfile):
        self.session = session
        self.csvfile = csvfile
        super(CsvGainWriter, self).__init__(
            csvfile, self.fieldnames, delimiter=",", quoting=csv.QUOTE_NONNUMERIC
        )

    def writerows(  # type: ignore
        self, gains: Sequence[inventory.Gain], consolidate: bool = False
    ) -> None:
        """ """
        if consolidate:

            def keyfunc(gain):
                return gain.transaction.security.id

            for k, gs in itertools.groupby(sorted(gains, key=keyfunc), key=keyfunc):
                row = self._gains2row(list(gs))
                self.writerow(row)

        else:
            for gain in gains:
                row = self._gain2row(gain)
                self.writerow(row)

    def _gain2row(self, gain: inventory.Gain) -> CsvDictRowRead:
        """
        Transform a single Gain into a dict suitable to hand to self.writerow()
        """
        report = report_gain(self.session, gain)

        # FIXME
        disallowed = None

        row = {
            "brokerid": report.fiaccount.fi.brokerid,
            "acctid": report.fiaccount.number,
            "ticker": report.security.ticker,
            "secname": report.security.name,
            "gaindt": report.gaintx.datetime,
            "gaintxid": report.gaintx.uniqueid,
            "ltcg": report.longterm,
            "opendt": report.opentx.datetime,
            "opentxid": report.opentx.uniqueid,
            "units": report.units,
            "proceeds": report.proceeds,
            "cost": report.cost,
            "currency": report.currency,
            "realized": report.proceeds - report.cost,
            "disallowed": disallowed,
        }
        return row

    def _gains2row(self, gains: Sequence[inventory.Gain]) -> CsvDictRowWrite:
        """
        Sum a list of Gains and transform into a dict suitable to hand to
        self.writerow()
        """
        reports = [report_gain(self.session, gain) for gain in gains]

        # input gains have identical security ( itertools.groupby() )
        security = reports[0].security

        # FIXME - do currency conversions
        currency = reports[0].currency
        assert all(report.currency == currency for report in reports)

        running_totals = itertools.accumulate(
            reports,
            lambda r0, r1: GainReport(
                fiaccount=None,
                security=security,
                opentx=None,
                gaintx=None,
                units=r0.units + r1.units,
                currency=currency,
                cost=r0.cost + r1.cost,
                proceeds=r0.proceeds + r1.proceeds,
                longterm=None,
            ),
        )
        total = list(running_totals)[-1]

        return {
            "brokerid": None,
            "acctid": None,
            "ticker": security.ticker,
            "secname": security.name,
            "gaindt": None,
            "gaintxid": None,
            "ltcg": None,
            "opendt": None,
            "opentxid": None,
            "units": total.units,
            "proceeds": total.proceeds,
            "cost": total.cost,
            "currency": currency,
            "realized": total.proceeds - total.cost,
            "disallowed": None,
        }


#######################################################################################
# REPORTING FUNCTIONS
#######################################################################################
class GainReport(NamedTuple):
    """ Data container for reporting gain """

    fiaccount: Any
    security: Any
    opentx: Any
    gaintx: Any
    units: Decimal
    currency: str
    cost: Decimal
    proceeds: Decimal
    longterm: bool


class Transaction(NamedTuple):
    """Nonpersistent implementation of the models.Transaction interface.
    """

    id: int
    uniqueid: str
    datetime: _datetime.datetime
    fiaccount: Any
    security: Any
    type: models.TransactionType
    dtsettle: Optional[_datetime.datetime] = None
    cash: Optional[Decimal] = None
    currency: Optional[str] = None
    units: Optional[Decimal] = None
    securityprice: Optional[Decimal] = None
    fromfiaccount: Any = None
    securityFrom: Any = None
    fromunits: Optional[Decimal] = None
    fromsecurityprice: Optional[Decimal] = None
    numerator: Optional[Decimal] = None
    denominator: Optional[Decimal] = None
    memo: Optional[str] = None
    #  sort: Optional[Mapping[str, Union[bool, Callable[[Any], Tuple]]]] = None
    sort: Optional[inventory.SortType] = None


def report_gain(session, gain: inventory.Gain) -> GainReport:
    """ Extract a GainReport from a Gain instance. """
    gain = translate_gain(session, gain)
    gaintx = gain.transaction
    lot = gain.lot
    units = lot.units

    # Short sales never get long-term capital gains treatment
    gaindt = gaintx.datetime
    opendt = lot.opentransaction.datetime
    longterm = (units > 0) and (gaindt - opendt >= timedelta(days=366))

    return GainReport(
        fiaccount=gaintx.fiaccount,
        security=gaintx.security,
        opentx=lot.opentransaction,
        gaintx=gaintx,
        units=units,
        currency=lot.currency,
        cost=units * lot.price,
        proceeds=units * gain.price,
        longterm=longterm,
    )


def translate_gain(session, gain: inventory.Gain) -> inventory.Gain:
    """
    Translate Gain instance's realizing transaction to functional currency.
    """
    # 26 CFR §1.988-2(a)(2)(iv)
    # (A)Amount realized. If stock or securities traded on an established
    # securities market are sold by a cash basis taxpayer for nonfunctional
    # currency, the amount realized with respect to the stock or securities
    # (as determined on the trade date) shall be computed by translating
    # the units of nonfunctional currency received into functional currency
    # at the spot rate on the _settlement date_ of the sale.  [...]
    #
    # (B)Basis. If stock or securities traded on an established securities
    # market are purchased by a cash basis taxpayer for nonfunctional
    # currency, the basis of the stock or securities shall be determined
    # by translating the units of nonfunctional currency paid into
    # functional currency at the spot rate on the _settlement date_ of the
    # purchase.
    lot, gaintx, gainprice = gain.lot, gain.transaction, gain.price

    functional_currency = CONFIG["books"]["functional_currency"]

    if lot.currency != functional_currency:
        opentx = lot.opentransaction
        if isinstance(
            opentx, (models.Transaction, inventory.Trade, inventory.ReturnOfCapital)
        ):
            dtsettle = opentx.dtsettle or opentx.datetime
        else:
            dtsettle = opentx.datetime
        date_settle = date(dtsettle.year, dtsettle.month, dtsettle.day)
        exchange_rate = models.CurrencyRate.get_rate(
            session,
            fromcurrency=lot.currency,
            tocurrency=functional_currency,
            date=date_settle,
        )
        opentx_translated = translate_transaction(
            opentx, functional_currency, exchange_rate
        )
        lot = lot._replace(
            opentransaction=opentx_translated,
            price=lot.price * exchange_rate,
            currency=functional_currency,
        )

    gaintx_currency = gaintx.currency or lot.currency
    if gaintx_currency != functional_currency:
        dtsettle = gaintx.dtsettle or gaintx.datetime
        date_settle = date(dtsettle.year, dtsettle.month, dtsettle.day)
        exchange_rate = models.CurrencyRate.get_rate(
            session,
            fromcurrency=gaintx_currency,
            tocurrency=functional_currency,
            date=date_settle,
        )

        gaintx = translate_transaction(gaintx, functional_currency, exchange_rate)
        gainprice = gainprice * exchange_rate

    return inventory.Gain(lot, gaintx, gainprice)


@functools.singledispatch
def translate_transaction(transaction, currency: str, rate: Decimal):
    """
    Translate a transaction into a different currency for reporting purposes.

    By default, return the transaction unmodified.
    """
    return transaction


@translate_transaction.register(inventory.Trade)
@translate_transaction.register(inventory.ReturnOfCapital)
@translate_transaction.register(inventory.Exercise)
def translate_cash_currency(
    transaction: Union[inventory.Trade, inventory.ReturnOfCapital, inventory.Exercise],
    currency: str,
    rate: Decimal,
):
    """
    Translate a transaction into a different currency for reporting purposes.
    """

    return transaction._replace(
        cash=_scaleAttr(transaction, "cash", rate), currency=currency
    )


@translate_transaction.register
def translate_security_pricing(
    transaction: inventory.Spinoff, currency: str, rate: Decimal
):
    """
    Translate a transaction into a different currency for reporting purposes.
    """

    return transaction._replace(
        securityprice=_scaleAttr(transaction, "securityprice", rate),
        fromsecurityprice=_scaleAttr(transaction, "fromsecurityprice", rate),
    )


@translate_transaction.register
def translate_model(transaction: models.Transaction, currency: str, rate: Decimal):
    """
    Translate a transaction into a different currency for reporting purposes.
    """

    return Transaction(
        id=transaction.id,
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


def _scaleAttr(instance: object, attr: str, coefficient: Decimal) -> Decimal:
    """
    """
    value = getattr(instance, attr)
    if value is not None:
        value *= coefficient
    return value
