# coding: utf-8
""" """
# stdlib imports
import logging


# 3rd party imports
from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Numeric,
    ForeignKey,
    Enum,
    event,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import UniqueConstraint
from ofxtools.models.i18n import CURRENCY_CODES


# Local imports
from capgains.database import Base


class ModelError(Exception):
    """ Base class for exceptions raised by this module.  """
    pass


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

    fiaccounts = relationship('FiAccount',
                              back_populates='fi')

    signature = ('brokerid', )


class FiAccount(Base, Mergeable):
    """ A financial institution account """
    id = Column(Integer, primary_key=True)
    fi_id = Column(ForeignKey('fi.id'), nullable=False)
    number = Column(String, nullable=False)
    name = Column(String)

    fi = relationship('Fi', back_populates='fiaccounts')

    signature = ('fi', 'number')

    @classmethod
    def merge(cls, session, **kwargs):
        if 'fi' not in kwargs:
            brokerid = kwargs.pop('brokerid')
            kwargs['fi'] = Fi.merge(session, brokerid=brokerid)
        instance = super(FiAccount, cls).merge(session, **kwargs)
        return instance


class Security(Base):
    id = Column(Integer, primary_key=True)
    name = Column(String)
    ticker = Column(String)

    ids = relationship('SecurityId', back_populates='security')

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
            sec = session.query(Security).filter_by(
                ticker=ticker, name=name).one_or_none()
            if sec:
                # Matching ticker/name, different uniqueid=> probably same security
                # Insert a new SecurityId holding the alternate id
                secid = SecurityId(security=sec,
                                   uniqueidtype=uniqueidtype,
                                   uniqueid=uniqueid)
                session.add(secid)
                return secid

        def matchTicker(ticker):
            sec = session.query(Security).filter_by(ticker=ticker).one_or_none()
            if sec:
                # Matching ticker, different uniqueid => probably same security
                # Insert a new SecurityId holding the alternate id
                secid = SecurityId(security=sec,
                                   uniqueidtype=uniqueidtype,
                                   uniqueid=uniqueid)
                session.add(secid)
                return secid

        secid = session.query(SecurityId).filter_by(
                uniqueidtype=uniqueidtype, uniqueid=uniqueid).one_or_none() \
            or matchTickerName(ticker, name) \
            or matchTicker(ticker)

        if secid:
            sec = secid.security
        else:
            sec = Security(name=name, ticker=ticker)
            secid = SecurityId(security=sec, uniqueidtype=uniqueidtype,
                               uniqueid=uniqueid)
            session.add_all([sec, secid])

        return sec

    def __repr__(self):
        repr = "Security(id={}, name='{}', ticker='{}')"
        return repr.format(self.id, self.name, self.ticker)


class SecurityId(Base):
    id = Column(Integer, primary_key=True)
    security_id = Column(Integer,
                         ForeignKey('security.id', onupdate='CASCADE'),
                         nullable=False)
    uniqueidtype = Column(String, nullable=False)
    uniqueid = Column(String, nullable=False)

    security = relationship('Security', back_populates='ids')

    __table_args__ = (UniqueConstraint('uniqueidtype', 'uniqueid'), )

    def __repr__(self):
        rp = "SecurityId(id={}, uniqueidtype='{}', uniqueid='{}', security={})"
        return rp.format(self.id, self.uniqueidtype, self.uniqueid,
                         self.security)


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
    # Postgres sorts Enums by listed order of type definition
    # To ensure reorgs get processed correctly, trade & transfer come last
    type = Column(Enum('returnofcapital', 'split', 'spinoff', 'transfer',
                       'trade', 'exercise', name='transaction_type'),
                  nullable=False)
    memo = Column(Text)
    # Currency denomination of Transaction.cash 
    currency = Column(Enum(*CURRENCY_CODES, name='transaction_currency'))
    # Change in money amount caused by Transaction
    cash = Column(Numeric)
    # Financial institution account
    fiaccount_id = Column(Integer,
                          ForeignKey('fiaccount.id', onupdate='CASCADE'),
                          nullable=False)
    # Multiple join paths from Transaction to FiAccount(
    # fiaccount; fiaccountFromFrom)  so can't use relationship(back_populates)
    # on both sides of the the join; must use relationship(backref) on the
    # ForeignKey side.
    fiaccount = relationship('FiAccount', foreign_keys=[fiaccount_id],
                             backref='transactions')
    # Security or other asset
    security_id = Column(Integer,
                         ForeignKey('security.id', onupdate='CASCADE'),
                         nullable=False)
    # Multiple join paths from Transaction to Security (security; securityFrom)
    # so can't use relationship(back_populates) on both sides of the the join;
    # must use relationship(backref) on the ForeignKey side.
    security = relationship('Security', foreign_keys=[security_id],
                            backref='transactions')
    # Change in Security quantity caused by Transaction
    units = Column(Numeric)
    # For spinoffs: FMV of source security post-spin
    securityPrice = Column(Numeric)
    # For transfers: source FI acount
    fiaccountFrom_id = Column(Integer,
                              ForeignKey('fiaccount.id', onupdate='CASCADE'), )
    # Multiple join paths from Transaction to FiAccount(
    # fiaccount; fiaccountFromFrom)  so can't use relationship(back_populates)
    # on both sides of the the join; must use relationship(backref) on the
    # ForeignKey side.
    fiaccountFrom = relationship('FiAccount', foreign_keys=[fiaccountFrom_id],
                                 backref='transactionsFrom')
    # For transfers, spinoffs, exercise: source Security
    securityFrom_id = Column(Integer,
                             ForeignKey('security.id', onupdate='CASCADE'), )
    # Multiple join paths from Transaction to Security (security; securityFrom)
    # so can't use relationship(back_populates) on both sides of the the join;
    # must use relationship(backref) on the ForeignKey side.
    securityFrom = relationship('Security', foreign_keys=[securityFrom_id],
                                backref='transactionsFrom')
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
    sort = Column(Enum('FIFO', 'LIFO', 'MAXGAIN', 'MINGAIN',
                       name='transaction_sort'), )

    signature = ('fiaccount', 'uniqueid')


class ModelConstraintError(ModelError):
    """
    Exception raised upon violation of a model constraint (outside sqlalchemy)
    """
    pass


@event.listens_for(Transaction, 'before_insert')
@event.listens_for(Transaction, 'before_update')
def enforce_type_constraints(mapper, connection, instance):
    enforcers = {'trade': TradeConstraint,
                 'returnofcapital': ReturnOfCapitalConstraint,
                 'transfer': TransferConstraint,
                 'split': SplitConstraint,
                 'spinoff': SpinoffConstraint,
                 'exercise': ExerciseConstraint, }
    enforcers[instance.type].enforce(instance)


class Constraint(object):
    isNone = ()
    notNone = ()

    @classmethod
    def enforce(cls, instance):
        cls.enforceNone(instance)
        cls.enforceNotNone(instance)

    @classmethod
    def enforceNone(cls, instance):
        for attr in cls.isNone:
            if getattr(instance, attr) is not None:
                msg = "Transaction.{} must be None if type='{}': {}"
                raise ModelConstraintError(msg.format(
                    attr, instance.type, instance))

    @classmethod
    def enforceNotNone(cls, instance):
        for attr in cls.notNone:
            if getattr(instance, attr) is None:
                msg = "Transaction.{} must not be None if type='{}': {}"
                raise ModelConstraintError(msg.format(
                    attr, instance.type, instance))


class TradeConstraint(Constraint):
    isNone = ('securityPrice', 'fiaccountFrom', 'securityFrom', 'unitsFrom',
              'securityFromPrice', 'numerator', 'denominator')
    notNone = ('cash', 'units')

    @classmethod
    def enforce(cls, instance):
        super(TradeConstraint, cls).enforce(instance)
        if instance.units == 0:
            msg = "Transaction.units must be zero if type='{}': {}"
            raise ModelConstraintError(msg.format(
                instance.type, instance))


class ReturnOfCapitalConstraint(Constraint):
    isNone = ('units', 'securityPrice', 'fiaccountFrom', 'securityFrom',
              'unitsFrom', 'securityFromPrice', 'numerator', 'denominator')
    notNone = ('cash', )


class TransferConstraint(Constraint):
    isNone = ('cash',  'securityPrice', 'securityFromPrice', 'numerator',
              'denominator')
    notNone = ('units', 'fiaccountFrom', 'securityFrom', 'unitsFrom', )


class SplitConstraint(Constraint):
    isNone = ('cash',  'securityPrice', 'securityFromPrice',
              'fiaccountFrom', 'securityFrom', 'unitsFrom', )
    notNone = ('units', 'numerator', 'denominator')


class SpinoffConstraint(Constraint):
    isNone = ('cash', 'fiaccountFrom', 'unitsFrom', )
    notNone = ('units', 'securityFrom', 'numerator', 'denominator')

    @classmethod
    def enforce(cls, instance):
        super(SpinoffConstraint, cls).enforce(instance)
        if not instance.numerator > 0:
            msg = "Transaction.numerator must be >0 if type='{}': {}"
            raise ModelConstraintError(msg.format(
                instance.type, instance))
        if not instance.denominator > 0:
            msg = "Transaction.denominator must be >0 if type='{}': {}"
            raise ModelConstraintError(msg.format(
                instance.type, instance))


class ExerciseConstraint(Constraint):
    isNone = ('numerator', 'denominator', 'fiaccountFrom', )
    notNone = ('units', 'security', 'unitsFrom', 'securityFrom', 'cash', )
