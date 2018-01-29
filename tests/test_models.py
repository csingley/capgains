# coding: utf-8
"""
"""
# stdlib imports
import unittest
import os


# 3rd party imports
from sqlalchemy import create_engine


# local imports
from capgains.database import Session, Base
from capgains.models.transactions import (
    Fi, FiAccount, Security, SecurityId,
    Transaction,
)


DB_URI = os.getenv('DB', 'sqlite://')


def setUpModule():
    """
    Called by unittest.TestRunner before any other tests in this module.
    """
    global engine
    engine = create_engine(DB_URI)


def tearDownModule():
    engine.dispose()


class DatabaseTest(object):
    """ Mixin providing DB setup/teardown methods """
    def setUp(self):
        self.connection = engine.connect()
        self.transaction = self.connection.begin()
        self.session = Session(bind=self.connection)
        Base.metadata.create_all(bind=self.connection)

    def tearDown(self):
        self.session.close()
        self.transaction.rollback()
        self.connection.close()


class SecurityTestCase(DatabaseTest, unittest.TestCase):
    def testMerge(self):
        sec0 = Security.merge(
            self.session, ticker='CNVR.SPO',
            name='CONVERA CORPORATION - SPINOFF',
            uniqueidtype='CONID', uniqueid='132118505')
        self.assertIsInstance(sec0, Security)
        self.assertEqual(sec0.ticker, 'CNVR.SPO')
        self.assertEqual(sec0.name, 'CONVERA CORPORATION - SPINOFF')
        self.assertEqual(len(sec0.ids), 1)
        secId0 = sec0.ids[0]
        self.assertIsInstance(secId0, SecurityId)
        self.assertEqual(secId0.uniqueidtype, 'CONID')
        self.assertEqual(secId0.uniqueid, '132118505')

        sec1 = Security.merge(
            self.session, ticker='CNVR.SPO',
            name='CONVERA CORPORATION - SPINOFF',
            uniqueidtype='CONID', uniqueid='132118505')
        self.assertIs(sec1, sec0)
        self.assertEqual(len(sec1.ids), 1)
        secId1 = sec1.ids[0]
        self.assertIs(secId1, secId0)


if __name__ == '__main__':
    unittest.main(verbosity=3)
