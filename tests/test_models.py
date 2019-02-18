# coding: utf-8
"""
"""
# stdlib imports
import unittest


# local imports
from capgains.config import CONFIG
from capgains.models.transactions import (
    Fi, FiAccount, Security, SecurityId,
    Transaction,
)
from common import (
    setUpModule,
    tearDownModule,
    RollbackMixin,
)


DB_URI = CONFIG.db_uri


class SecurityTestCase(RollbackMixin, unittest.TestCase):
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
