# coding: utf-8
""" """
# stdlib imports
import functools
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
    event,
)
from sqlalchemy.orm import relationship
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql.schema import UniqueConstraint
from ofxtools.models.i18n import CURRENCY_CODES


# Local imports
from capgains.database import Base


class ModelError(Exception):
    """ Base class for exceptions raised by this module.  """

    pass


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


class TransactionSort(enum.Enum):
    FIFO = 1
    LIFO = 2
    MAXGAIN = 3
    MINGAIN = 4


class Mergeable(object):
    """ Mixin implementing merge() classmethod """

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
    """ A financial institution """

    id = Column(Integer, primary_key=True)
    brokerid = Column(String, nullable=False, unique=True)
    name = Column(String)

    fiaccounts = relationship("FiAccount", back_populates="fi")

    signature = ("brokerid",)


class FiAccount(Base, Mergeable):
    """ A financial institution account """

    id = Column(Integer, primary_key=True)
    fi_id = Column(ForeignKey("fi.id"), nullable=False)
    number = Column(String, nullable=False)
    name = Column(String)

    fi = relationship("Fi", back_populates="fiaccounts")

    signature = ("fi", "number")

    @classmethod
    def merge(cls, session, **kwargs):
        if "fi" not in kwargs:
            brokerid = kwargs.pop("brokerid")
            kwargs["fi"] = Fi.merge(session, brokerid=brokerid)
        instance = super(FiAccount, cls).merge(session, **kwargs)
        return instance


class Security(Base):
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
    id = Column(Integer, primary_key=True)
    security_id = Column(
        Integer, ForeignKey("security.id", onupdate="CASCADE"), nullable=False
    )
    uniqueidtype = Column(String, nullable=False)
    uniqueid = Column(String, nullable=False)

    security = relationship("Security", back_populates="ids")

    __table_args__ = (UniqueConstraint("uniqueidtype", "uniqueid"),)

    def __repr__(self):
        rp = "SecurityId(id={}, uniqueidtype='{}', uniqueid='{}', security={})"
        return rp.format(self.id, self.uniqueidtype, self.uniqueid, self.security)


class Transaction(Base, Mergeable):
    """
    """

    id = Column(Integer, primary_key=True)
    # FI transaction unique identifier
    uniqueid = Column(String, nullable=False, unique=True)
    # Effective date/time
    datetime = Column(DateTime, nullable=False)
    # The payment for cash distributions (datetime field records the ex-date)
    dtsettle = Column(DateTime)
    type = Column(Enum(TransactionType, name="transaction_type"), nullable=False)
    memo = Column(Text)
    # Currency denomination of Transaction.cash
    currency = Column(Enum(*CURRENCY_CODES, name="transaction_currency"))
    # Change in money amount caused by Transaction
    cash = Column(Numeric)
    # Financial institution account
    fiaccount_id = Column(
        Integer, ForeignKey("fiaccount.id", onupdate="CASCADE"), nullable=False
    )
    # Multiple join paths from Transaction to FiAccount (fiaccount; fiaccountFrom)
    # so can't use relationship(back_populates) on both sides of the the join;
    # must use relationship(backref) on the ForeignKey side.
    fiaccount = relationship(
        "FiAccount", foreign_keys=[fiaccount_id], backref="transactions"
    )
    # Security or other asset
    security_id = Column(
        Integer, ForeignKey("security.id", onupdate="CASCADE"), nullable=False
    )
    # Multiple join paths from Transaction to Security (security; securityFrom)
    # so can't use relationship(back_populates) on both sides of the the join;
    # must use relationship(backref) on the ForeignKey side.
    security = relationship(
        "Security", foreign_keys=[security_id], backref="transactions"
    )
    # Change in Security quantity caused by Transaction
    units = Column(Numeric)
    # For spinoffs: FMV of source security post-spin
    securityPrice = Column(Numeric)
    # For transfers: source FI acount
    fiaccountFrom_id = Column(Integer, ForeignKey("fiaccount.id", onupdate="CASCADE"))
    # Multiple join paths from Transaction to FiAccount(
    # fiaccount; fiaccountFromFrom)  so can't use relationship(back_populates)
    # on both sides of the the join; must use relationship(backref) on the
    # ForeignKey side.
    fiaccountFrom = relationship(
        "FiAccount", foreign_keys=[fiaccountFrom_id], backref="transactionsFrom"
    )
    # For transfers, spinoffs, exercise: source Security
    securityFrom_id = Column(Integer, ForeignKey("security.id", onupdate="CASCADE"))
    # Multiple join paths from Transaction to Security (security; securityFrom)
    # so can't use relationship(back_populates) on both sides of the the join;
    # must use relationship(backref) on the ForeignKey side.
    securityFrom = relationship(
        "Security", foreign_keys=[securityFrom_id], backref="transactionsFrom"
    )
    # For splits, transfers, exercise: change in quantity of source Security
    # caused by Transaction
    unitsFrom = Column(Numeric)
    # For spinoffs: FMV of destination Security post-spin
    securityFromPrice = Column(Numeric)
    # For splits, spinoffs: normalized units of destination Security
    numerator = Column(Numeric)
    # For splits, spinoff: normalized units of source Security
    denominator = Column(Numeric)
    # Sort algorithm for gain recognition
    sort = Column(Enum(TransactionSort, name="transaction_sort"))

    signature = ("fiaccount", "uniqueid")


class CurrencyRate(Base, Mergeable):
    """
    """

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    fromcurrency = Column(Enum(*CURRENCY_CODES, name="fromcurrency"), nullable=False)
    tocurrency = Column(Enum(*CURRENCY_CODES, name="tocurrency"), nullable=False)
    rate = Column(Numeric, nullable=False)

    __table_args__ = (UniqueConstraint("date", "fromcurrency", "tocurrency"),)

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


class ModelConstraintError(ModelError):
    """
    Exception raised upon violation of a model constraint (outside sqlalchemy)
    """

    pass


@event.listens_for(Transaction, "before_insert")
@event.listens_for(Transaction, "before_update")
def enforce_type_constraints(mapper, connection, instance):
    enforcers = {
        TransactionType.TRADE: enforce_trade_constraints,
        TransactionType.RETURNCAP: enforce_returnofcapital_constraints,
        TransactionType.TRANSFER: enforce_transfer_constraints,
        TransactionType.SPLIT: enforce_split_constraints,
        TransactionType.SPINOFF: enforce_spinoff_constraints,
        TransactionType.EXERCISE: enforce_exercise_constraints,
    }
    enforcers[instance.type](instance)


def enforce_constraints(instance, isNone=(), notNone=(), isPositive=(), nonZero=()):
    for seq, predicate, err_msg in (
        (isNone, lambda x: x is None, "None"),
        (notNone, lambda x: x is not None, "not None"),
        (isPositive, lambda x: x > 0, "positive"),
        (nonZero, lambda x: x != 0, "nonzero"),
    ):
        for attr in seq:
            if not predicate(getattr(instance, attr)):
                msg = "Transaction.{} must be {} if type='{}': {}"
                raise ModelConstraintError(
                    msg.format(attr, err_msg, instance.type, instance)
                )


enforce_trade_constraints = functools.partial(
    enforce_constraints,
    isNone=(
        "securityPrice",
        "fiaccountFrom",
        "securityFrom",
        "unitsFrom",
        "securityFromPrice",
        "numerator",
        "denominator",
    ),
    notNone=("cash", "units"),
    nonZero=("units",),
)


enforce_returnofcapital_constraints = functools.partial(
    enforce_constraints,
    isNone=(
        "units",
        "securityPrice",
        "fiaccountFrom",
        "securityFrom",
        "unitsFrom",
        "securityFromPrice",
        "numerator",
        "denominator",
    ),
    notNone=("cash",),
)


enforce_transfer_constraints = functools.partial(
    enforce_constraints,
    isNone=("cash", "securityPrice", "securityFromPrice", "numerator", "denominator"),
    notNone=("units", "fiaccountFrom", "securityFrom", "unitsFrom"),
)


enforce_split_constraints = functools.partial(
    enforce_constraints,
    isNone=(
        "cash",
        "securityPrice",
        "securityFromPrice",
        "fiaccountFrom",
        "securityFrom",
        "unitsFrom",
    ),
    notNone=("units", "numerator", "denominator"),
    isPositive=("numerator", "denominator"),
)


enforce_spinoff_constraints = functools.partial(
    enforce_constraints,
    isNone=("cash", "fiaccountFrom", "unitsFrom"),
    notNone=("units", "securityFrom", "numerator", "denominator"),
    isPositive=("numerator", "denominator"),
)


enforce_exercise_constraints = functools.partial(
    enforce_constraints,
    isNone=("numerator", "denominator", "fiaccountFrom"),
    notNone=("units", "security", "unitsFrom", "securityFrom", "cash"),
)
