# coding: utf-8
""" """
# stdlib imports
import enum
import logging


# 3rd party imports
from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Date,
    Numeric,
    ForeignKey,
    Enum,
)
from sqlalchemy.orm import relationship
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.schema import UniqueConstraint, CheckConstraint
from ofxtools.models.i18n import CURRENCY_CODES


# Local imports
from capgains.database import Base


class ModelError(Exception):
    """ Base class for exceptions raised by this module.  """

    pass


@enum.unique
class TransactionType(enum.Enum):
    # Postgres sorts Enums by listed order of type definition
    # To ensure reorgs get processed correctly, trade & transfer
    # must come after return of capital & spinoff.
    RETURNCAP = 1
    SPLIT = 2
    SPINOFF = 3
    TRANSFER = 4
    TRADE = 5
    EXERCISE = 6


@enum.unique
class TransactionSort(enum.Enum):
    FIFO = 1
    LIFO = 2
    MAXGAIN = 3
    MINGAIN = 4


CurrencyType = Enum(*CURRENCY_CODES, name="currency_type")


class Mergeable(object):
    """Mixin implementing merge() classmethod.
    """

    signature = NotImplemented

    @classmethod
    def merge(cls, session, **kwargs):
        """
        Query DB for unique persisted instance matching given values for
        signature attributes; if not found, insert a new instance with
        all attributes from kwargs.
        """
        if cls.signature is NotImplemented:
            raise NotImplementedError
        sig = {k: v for k, v in kwargs.items() if k in cls.signature}
        instance = session.query(cls).filter_by(**sig).one_or_none()
        msg = "Existing {} loaded from DB".format(instance)
        if instance is None:
            instance = cls(**kwargs)
            msg = "Created {}".format(instance)
        logging.info(msg)
        session.add(instance)
        return instance


class Fi(Base, Mergeable):
    """A financial institution (e.g. brokerage).
    """

    id = Column(Integer, primary_key=True)
    brokerid = Column(
        String, nullable=False, unique=True, comment="OFX <INVACCTFROM><BROKERID> value"
    )
    name = Column(String)

    accounts = relationship("FiAccount", back_populates="fi")

    __table_args__ = ({"comment": "Financial Institution (e.g. Brokerage)"},)

    signature = ("brokerid",)


class FiAccount(Base, Mergeable):
    """A financial institution (e.g. brokerage) account.
    """

    id = Column(Integer, primary_key=True)
    fi_id = Column(
        ForeignKey("fi.id"), nullable=False, comment="Financial institution (FK fi.id)"
    )
    fi = relationship("Fi", back_populates="accounts")
    number = Column(
        String, nullable=False, comment="account# (OFX <INVACCTFROM><ACCTID> value"
    )
    name = Column(String)

    __table_args__ = {"comment": "Financial Institution (e.g. Brokerage) Account"}

    signature = ("fi", "number")

    @classmethod
    def merge(cls, session, **kwargs):
        if "fi" not in kwargs:
            brokerid = kwargs.pop("brokerid")
            kwargs["fi"] = Fi.merge(session, brokerid=brokerid)
        instance = super(FiAccount, cls).merge(session, **kwargs)
        return instance


class Security(Base):
    """Market-traded security.
    """

    id = Column(Integer, primary_key=True)
    name = Column(String)
    ticker = Column(String)

    ids = relationship("SecurityId", back_populates="security")

    def __getitem__(self, uniqueidtype):
        ids = [id for id in self.ids if id.uniqueidtype == uniqueidtype]
        if len(ids) == 0:
            return None
        elif len(ids) == 1:
            return ids[0]
        else:
            msg = "For Security {}, muliple uniqueidtype='{}':  {}"
            raise ValueError(msg.format(self.id), uniqueidtype, ids)

    @classmethod
    def merge(cls, session, uniqueidtype, uniqueid, name=None, ticker=None):
        def matchTickerName(ticker, name):
            sec = (
                session.query(Security)
                .filter_by(ticker=ticker, name=name)
                .one_or_none()
            )
            if sec:
                # Matching ticker/name, different uniqueid=> probably same security
                # Insert a new SecurityId holding the alternate id
                secid = SecurityId(
                    security=sec, uniqueidtype=uniqueidtype, uniqueid=uniqueid
                )
                session.add(secid)
                return secid

        def matchTicker(ticker):
            sec = session.query(Security).filter_by(ticker=ticker).one_or_none()
            if sec:
                # Matching ticker, different uniqueid => probably same security
                # Insert a new SecurityId holding the alternate id
                secid = SecurityId(
                    security=sec, uniqueidtype=uniqueidtype, uniqueid=uniqueid
                )
                session.add(secid)
                return secid

        secid = (
            session.query(SecurityId)
            .filter_by(uniqueidtype=uniqueidtype, uniqueid=uniqueid)
            .one_or_none()
            or matchTickerName(ticker, name)
            or matchTicker(ticker)
        )

        if secid:
            sec = secid.security
        else:
            sec = Security(name=name, ticker=ticker)
            secid = SecurityId(
                security=sec, uniqueidtype=uniqueidtype, uniqueid=uniqueid
            )
            session.add_all([sec, secid])

        return sec

    def __repr__(self):
        repr = "Security(id={}, name='{}', ticker='{}')"
        return repr.format(self.id, self.name, self.ticker)


class SecurityId(Base):
    """Unique identifier for security.
    """

    id = Column(Integer, primary_key=True)
    security_id = Column(
        Integer,
        ForeignKey("security.id", onupdate="CASCADE"),
        nullable=False,
        comment="FK security.id",
    )
    security = relationship("Security", back_populates="ids")
    uniqueidtype = Column(String, nullable=False, comment="CUSIP, ISIN, etc.")
    uniqueid = Column(String, nullable=False, comment="CUSIP, ISIN, etc.")

    __table_args__ = (
        UniqueConstraint("uniqueidtype", "uniqueid"),
        {"comment": "Unique Identifiers for Securities"},
    )

    def __repr__(self):
        rp = "SecurityId(id={}, uniqueidtype='{}', uniqueid='{}', security={})"
        return rp.format(self.id, self.uniqueidtype, self.uniqueid, self.security)


#  This unholy mess of a check constraint mimics the API of invventory.types.
#  The several securities transaction subtypes don't cleanly map even to SQLAlchemy
#  single-table inheritance scheme, as far as I can tell.  Instead we encode the
#  transaction type in the `type` column (an enum), and enforce a constraint that
#  values (i.e. columns/attributes) required for the type must be non-null, while
#  values not applicable to the type must be null.
TRADE_CONSTRAINT = (
    "type='TRADE' "
    "AND currency IS NOT NULL "
    "AND cash IS NOT NULL "
    "AND units IS NOT NULL "
    "AND securityprice IS NULL "
    "AND fromfiaccount_id IS NULL "
    "AND fromsecurity_id IS NULL "
    "AND fromunits IS NULL "
    "AND fromsecurityprice IS NULL "
    "AND numerator IS NULL "
    "AND denominator IS NULL"
)
ROC_CONSTRAINT = (
    "type='RETURNCAP' "
    "AND currency is NOT NULL "
    "AND cash IS NOT NULL "
    "AND units IS NULL "
    "AND securityprice IS NULL "
    "AND fromfiaccount_id IS NULL "
    "AND fromsecurity_id IS NULL "
    "AND fromunits IS NULL "
    "AND fromunits IS NULL "
    "AND fromsecurityprice IS NULL "
    "AND numerator IS NULL "
    "AND denominator IS NULL"
)
TRANSFER_CONSTRAINT = (
    "type='TRANSFER' "
    "AND units IS NOT NULL "
    "AND fromfiaccount_id IS NOT NULL "
    "AND fromsecurity_id IS NOT NULL "
    "AND fromunits IS NOT NULL "
    "AND currency IS NULL "
    "AND cash IS NULL "
    "AND securityprice IS NULL "
    "AND fromsecurityprice IS NULL "
    "AND numerator IS NULL "
    "AND denominator IS NULL"
)
SPLIT_CONSTRAINT = (
    "type='SPLIT' "
    "AND units IS NOT NULL "
    "AND numerator IS NOT NULL "
    "AND denominator IS NOT NULL "
    "AND currency IS NULL "
    "AND cash is NULL "
    "AND securityprice IS NULL "
    "AND fromsecurityprice IS NULL "
    "AND fromfiaccount_id IS NULL "
    "AND fromsecurity_id IS NULL "
    "AND fromunits IS NULL"
)
SPINOFF_CONSTRAINT = (
    "type='SPINOFF' "
    "AND units IS NOT NULL "
    "AND fromsecurity_id IS NOT NULL "
    "AND numerator IS NOT NULL "
    "AND denominator IS NOT NULL "
    "AND currency IS NULL "
    "AND cash IS NULL "
    "AND fromfiaccount_id IS NULL "
    "AND fromunits IS NULL"
)
EXERCISE_CONSTRAINT = (
    "type='EXERCISE' "
    "AND units IS NOT NULL "
    "AND security_id IS NOT NULL "
    "AND fromunits IS NOT NULL "
    "AND fromsecurity_id IS NOT NULL "
    "AND currency IS NOT NULL "
    "AND cash IS NOT NULL "
    "AND numerator IS NULL "
    "AND denominator IS NULL "
    "AND fromfiaccount_id IS NULL"
)
#  N.B. boolean OR operator has lower precedence than boolean AND
#  https://www.postgresql.org/docs/11/sql-syntax-lexical.html#SQL-PRECEDENCE
TRANSACTION_CONSTRAINT = (
    f"{TRADE_CONSTRAINT} OR {ROC_CONSTRAINT} OR {TRANSFER_CONSTRAINT} OR "
    f"{SPLIT_CONSTRAINT} OR {SPINOFF_CONSTRAINT} OR {EXERCISE_CONSTRAINT}"
)


class Transaction(Base, Mergeable):
    """Securities transaction.
    """

    id = Column(Integer, primary_key=True)
    type = Column(
        Enum(TransactionType, name="transaction_type"),
        nullable=False,
        comment=f"One of {tuple(TransactionType.__members__.keys())}",
    )
    uniqueid = Column(
        String, nullable=False, unique=True, comment="FI transaction unique identifier"
    )
    datetime = Column(
        DateTime,
        nullable=False,
        comment="Effective date/time: ex-date for reorgs, return of capital",
    )
    fiaccount_id = Column(
        Integer,
        ForeignKey("fiaccount.id", onupdate="CASCADE"),
        nullable=False,
        comment=(
            "Financial institution account (for transfers, destination FI account)"
            " - FK fiaccount.id"
        ),
    )
    # Multiple join paths from Transaction to FiAccount (fiaccount; fiaccountfrom)
    # so can't use relationship(back_populates) on both sides of the the join;
    # must use relationship(backref) on the ForeignKey side.
    fiaccount = relationship(
        "FiAccount", foreign_keys=[fiaccount_id], backref="transactions"
    )
    security_id = Column(
        Integer,
        ForeignKey("security.id", onupdate="CASCADE"),
        nullable=False,
        comment="FK security.id",
    )
    # Multiple join paths from Transaction to Security (security; securityfrom)
    # so can't use relationship(back_populates) on both sides of the the join;
    # must use relationship(backref) on the ForeignKey side.
    security = relationship(
        "Security", foreign_keys=[security_id], backref="transactions"
    )
    # Currency denomination of Transaction.cash
    currency = Column(Enum(*CURRENCY_CODES, name="transaction_currency"))
    # Change in money amount caused by Transaction
    cash = Column(Numeric)
    units = Column(
        Numeric,
        CheckConstraint("units <> 0", name="units_nonzero"),
        comment=(
            "Change in shares, contracts, etc. caused by Transaction "
            "(for splits, transfers, exercise: destination security "
            "change in units)"
        ),
    )
    dtsettle = Column(
        DateTime, comment="Settlement date: pay date for return of capital"
    )
    memo = Column(Text)
    securityprice = Column(
        "securityprice",
        Numeric,
        CheckConstraint("securityprice >= 0", name="securityprice_not_negative"),
        comment="For spinoffs: unit price used to fair-value destination security",
    )
    fromfiaccount_id = Column(
        "fromfiaccount_id",
        Integer,
        ForeignKey("fiaccount.id", onupdate="CASCADE"),
        comment="For transfers: source FI account (FK fiaccount.id)",
    )
    # Multiple join paths from Transaction to FiAccount (fiaccount; fiaccountfrom)
    # so can't use relationship(back_populates) on both sides of the the join;
    # must use relationship(backref) on the ForeignKey side.
    fromfiaccount = relationship(
        "FiAccount", foreign_keys=[fromfiaccount_id], backref="fromtransactions"
    )
    fromsecurity_id = Column(
        Integer,
        ForeignKey("security.id", onupdate="CASCADE"),
        comment="For transfers, spinoffs, exercise: source security (FK security.id)",
    )
    # Multiple join paths from Transaction to Security (security; securityfrom)
    # so can't use relationship(back_populates) on both sides of the the join;
    # must use relationship(backref) on the ForeignKey side.
    fromsecurity = relationship(
        "Security", foreign_keys=[fromsecurity_id], backref="transactionsFrom"
    )
    fromunits = Column(
        "fromunits",
        Numeric,
        comment="For splits, transfers, exercise: source security change in units",
    )
    #  "It should be noted that a check constraint is satisfied if the check expression
    #  evaluates to true or the null value. Since most expressions will evaluate to the
    #  null value if any operand is null, they will not prevent null values in the
    #  constrained columns."
    #  https://www.postgresql.org/docs/11/ddl-constraints.html#DDL-CONSTRAINTS-CHECK-CONSTRAINTS
    fromsecurityprice = Column(
        "fromsecurityprice",
        Numeric,
        CheckConstraint("fromsecurityprice >= 0", name="fromsecurityprice_not_negative"),
        comment="For spinoffs: unit price used to fair-value source security",
    )
    numerator = Column(
        Numeric,
        CheckConstraint("numerator > 0", name="numerator_positive"),
        comment="For splits, spinoffs: normalized units of destination security",
    )
    denominator = Column(
        Numeric,
        CheckConstraint("denominator > 0", name="denominator_positive"),
        comment="For splits, spinoffs: normalized units of source security",
    )
    sort = Column(
        Enum(TransactionSort, name="transaction_sort"),
        comment="Sort algorithm for gain recognition",
    )

    __table_args__ = (
        CheckConstraint(TRANSACTION_CONSTRAINT, name="enforce_subtype_nulls"),
        {"comment": "Securities Transactions"}
    )

    signature = ("fiaccount", "uniqueid")


class CurrencyRate(Base, Mergeable):
    """Exchange rate for currency pair.
    """

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    fromcurrency = Column(
        CurrencyType,
        nullable=False,
        comment="Currency of exchange rate denominator (ISO4217)",
    )
    tocurrency = Column(
        CurrencyType,
        nullable=False,
        comment="Currency of exchange rate numerator (ISO417)",
    )
    rate = Column(
        Numeric,
        nullable=False,
        comment="Multiply this rate by fromcurrency amount to yield tocurrency amount",
    )

    __table_args__ = (
        UniqueConstraint("date", "fromcurrency", "tocurrency"),
        {"comment": "Exchange Rates for Currency Pairs"},
    )

    signature = ("date", "fromcurrency", "tocurrency")

    @classmethod
    def get_rate(cls, session, fromcurrency, tocurrency, date):
        """
        Returns `rate` (type decimal.Decimal)  as `tocurrency` / `fromcurrency`
        i.e. `fromCurrency` * `rate` == 'tocurrency`
        """
        if fromcurrency is None or tocurrency is None or date is None:
            msg = (
                "CurrencyRate.get_rate(): missing argument in "
                "(fromcurrency='{}', tocurrency='{}', date={}"
            )
            raise ValueError(msg.format(fromcurrency, tocurrency, date))
        try:
            instance = (
                session.query(cls)
                .filter_by(fromcurrency=fromcurrency, tocurrency=tocurrency, date=date)
                .one()
            )
            rate = instance.rate
        except NoResultFound:
            try:
                instance = (
                    session.query(cls)
                    .filter_by(
                        fromcurrency=tocurrency, tocurrency=fromcurrency, date=date
                    )
                    .one()
                )
            except NoResultFound:
                msg = (
                    "CurrencyRate.get_rate(): no DB record for "
                    "(fromcurrency='{}', tocurrency='{}', date={})"
                )
                raise ValueError(msg.format(fromcurrency, tocurrency, date))
            rate = 1 / instance.rate

        return rate
