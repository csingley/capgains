# coding: utf-8
"""
SQLAlchemy declarative base for model classes in this package.
"""

# stdlib imports
from contextlib import contextmanager
import itertools


# 3rd party imports
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import as_declarative, declared_attr


def init_db(db_uri, **kwargs):
    engine = create_engine(db_uri, **kwargs)
    Base.metadata.create_all(bind=engine)
    Session.configure(bind=engine)
    return engine


Session = sessionmaker()


@contextmanager
def sessionmanager(**kwargs):
    """Provide a transactional scope around a series of operations."""
    try:
        session = Session(**kwargs)
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


@as_declarative()
class Base(object):
    """
    SQLAlchemy declarative base for model classes in this package.
    """

    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    def __repr__(self):
        """
        Lists all non-NULL instance attributes.
        """
        # Last 4 classes in __mro__ are sqlalchemy.ext.declarative.api.Base,
        # capgains.database.Base, capgains.models.Mergeable, and object...
        # don't want those!
        mro = list(self.__class__.__mro__)[:-4]
        # Order from ancestor to descendant
        mro.reverse()
        # Collect all column names from ancestry, flatten, and remove dupes
        attrs = _unique(
            itertools.chain.from_iterable(
                [[col.name for col in cls.__table__.c] for cls in mro]
            )
        )
        attrs = list(attrs)  # Why is this necessary?
        # Return getattr() for all the above that aren't None
        return "<%s(%s)>" % (
            self.__class__.__name__,
            ", ".join(
                [
                    "%s=%r" % (attr, str(getattr(self, attr)))
                    for attr in attrs
                    if getattr(self, attr) is not None
                ]
            ),
        )


def _unique(iterable):
    "List unique elements, preserving order."
    seen = set()
    seen_add = seen.add
    for element in itertools.filterfalse(seen.__contains__, iterable):
        seen_add(element)
        yield element
