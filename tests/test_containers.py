# coding: utf-8
"""
"""
# stdlib imports
import unittest
import operator
import datetime
from decimal import Decimal


# local imports
from capgains.containers import GroupedList
from capgains.flex.parser import Trade


class GroupedListTestCase(unittest.TestCase):
    def setUp(self):
        self.items = GroupedList(range(10))
        self.grouped_items = self.items.groupby(lambda x: x % 3)

    def test_init(self):
        # Bare init
        instance = GroupedList()
        self.assertEqual(len(instance), 0)
        self.assertIs(instance.grouped, False)
        self.assertIs(instance.key, None)

        # init list
        instance = GroupedList([])
        self.assertEqual(len(instance), 0)
        self.assertIs(instance.grouped, False)
        self.assertIs(instance.key, None)

    def testFilter(self):
        # Flat
        items = self.items.filter(lambda x: x % 2)
        self.assertIsInstance(items, GroupedList)
        self.assertFalse(items.grouped)
        self.assertIsNone(items.key)
        self.assertEqual(list(items), [1, 3, 5, 7, 9])

        # Nested
        container = self.grouped_items.filter(lambda x: x % 2)
        self.assertIsInstance(container, GroupedList)
        self.assertTrue(container.grouped)
        self.assertIsNone(container.key)
        self.assertEqual(len(container), 3)

        subcont0, subcont1, subcont2 = container[:]
        self.assertIsInstance(subcont0, GroupedList)
        self.assertFalse(subcont0.grouped)
        self.assertEqual(subcont0.key, 0)
        self.assertEqual(list(subcont0), [3, 9])

        self.assertIsInstance(subcont1, GroupedList)
        self.assertFalse(subcont1.grouped)
        self.assertEqual(subcont1.key, 1)
        self.assertEqual(list(subcont1), [1, 7])

        self.assertIsInstance(subcont2, GroupedList)
        self.assertFalse(subcont1.grouped)
        self.assertEqual(subcont2.key, 2)
        self.assertEqual(list(subcont2), [5])

    def testMap(self):
        # Flat
        items = self.items.map(lambda x: 2 * x)
        self.assertIsInstance(items, GroupedList)
        self.assertFalse(items.grouped)
        self.assertIsNone(items.key)
        self.assertEqual(list(items),
                         [0, 2, 4, 6, 8, 10, 12, 14, 16, 18])

        # Nested
        container = self.grouped_items.map(lambda x: 2 * x)
        self.assertIsInstance(container, GroupedList)
        self.assertTrue(container.grouped)
        self.assertIsNone(container.key)
        self.assertEqual(len(container), 3)

        subcont0, subcont1, subcont2 = container[:]
        self.assertIsInstance(subcont0, GroupedList)
        self.assertFalse(subcont0.grouped)
        self.assertEqual(subcont0.key, 0)
        self.assertEqual(list(subcont0), [0, 6, 12, 18])

        self.assertIsInstance(subcont1, GroupedList)
        self.assertFalse(subcont1.grouped)
        self.assertEqual(subcont1.key, 1)
        self.assertEqual(list(subcont1), [2, 8, 14])

        self.assertIsInstance(subcont2, GroupedList)
        self.assertFalse(subcont1.grouped)
        self.assertEqual(subcont2.key, 2)
        self.assertEqual(list(subcont2), [4, 10, 16])

    def testReduce(self):
        # Flat
        items = self.items.reduce(operator.add)
        self.assertIsInstance(items, GroupedList)
        self.assertFalse(items.grouped)
        self.assertIsNone(items.key)
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item, 45)

        # Nested
        container = self.grouped_items.reduce(operator.add)
        self.assertIsInstance(container, GroupedList)
        self.assertTrue(container.grouped)
        self.assertIsNone(container.key)
        self.assertEqual(len(container), 3)

        subcont0, subcont1, subcont2 = container[:]
        self.assertIsInstance(subcont0, GroupedList)
        self.assertFalse(subcont0.grouped)
        self.assertEqual(subcont0.key, 0)
        self.assertEqual(list(subcont0), [18])

        self.assertIsInstance(subcont1, GroupedList)
        self.assertFalse(subcont1.grouped)
        self.assertEqual(subcont1.key, 1)
        self.assertEqual(list(subcont1), [12])

        self.assertIsInstance(subcont2, GroupedList)
        self.assertFalse(subcont1.grouped)
        self.assertEqual(subcont2.key, 2)
        self.assertEqual(list(subcont2), [15])

    def testGroupby(self):
        # Flat
        items = self.items.groupby(lambda x: x % 2)
        self.assertIsInstance(items, GroupedList)
        self.assertEqual(items.grouped, True)
        self.assertEqual(items.key, None)
        self.assertEqual(len(items), 2)
        item0, item1 = items
        self.assertIsInstance(item0, GroupedList)
        self.assertFalse(item0.grouped)
        self.assertFalse(item0.key)
        self.assertEqual(list(item0), [0, 2, 4, 6, 8])

        self.assertIsInstance(item1, GroupedList)
        self.assertFalse(item1.grouped)
        self.assertTrue(item1.key)
        self.assertEqual(list(item1), [1, 3, 5, 7, 9])

        # Nested
        container = self.grouped_items.groupby(lambda x: x % 2)
        self.assertIsInstance(container, GroupedList)
        self.assertEqual(container.grouped, True)
        self.assertEqual(container.key, None)
        self.assertEqual(len(container), 3)
        subcontainer0, subcontainer1, subcontainer2 = container

        self.assertIsInstance(subcontainer0, GroupedList)
        self.assertEqual(subcontainer0.grouped, True)
        self.assertEqual(subcontainer0.key, 0)
        self.assertEqual(len(subcontainer0), 2)
        items0, items1 = subcontainer0
        self.assertIsInstance(items0, GroupedList)
        self.assertEqual(items0.grouped, False)
        self.assertEqual(items0.key, 0)
        self.assertEqual(list(items0), [0, 6])
        self.assertIsInstance(items1, GroupedList)
        self.assertEqual(items1.grouped, False)
        self.assertEqual(items1.key, 1)
        self.assertEqual(list(items1), [3, 9])

        self.assertIsInstance(subcontainer1, GroupedList)
        self.assertEqual(subcontainer1.grouped, True)
        self.assertEqual(subcontainer1.key, 1)
        self.assertEqual(len(subcontainer1), 2)
        items0, items1 = subcontainer1
        self.assertIsInstance(items0, GroupedList)
        self.assertEqual(items0.grouped, False)
        self.assertEqual(items0.key, 0)
        self.assertEqual(list(items0), [4])
        self.assertIsInstance(items1, GroupedList)
        self.assertEqual(items1.grouped, False)
        self.assertEqual(items1.key, 1)
        self.assertEqual(list(items1), [1, 7])

        self.assertIsInstance(subcontainer2, GroupedList)
        self.assertEqual(subcontainer2.grouped, True)
        self.assertEqual(subcontainer2.key, 2)
        self.assertEqual(len(subcontainer2), 2)
        items0, items1 = subcontainer2
        self.assertIsInstance(items0, GroupedList)
        self.assertEqual(items0.grouped, False)
        self.assertEqual(items0.key, 0)
        self.assertEqual(list(items0), [2, 8])
        self.assertIsInstance(items1, GroupedList)
        self.assertEqual(items1.grouped, False)
        self.assertEqual(items1.key, 1)
        self.assertEqual(list(items1), [5])

    def testFlatten(self):
        # Flat
        items = self.items.flatten()
        self.assertEqual(items, self.items)

        # Nested
        items = self.grouped_items.flatten()
        self.assertIsInstance(items, GroupedList)
        self.assertEqual(items.grouped, False)
        self.assertEqual(items.key, None)
        self.assertEqual(list(items), [0, 3, 6, 9, 1, 4, 7, 2, 5, 8])

    def testSorted(self):
        # Flat
        items = self.items.sorted(lambda x: -x)
        self.assertEqual(list(items), list(range(9, -1, -1)))

        # Nested
        container = self.grouped_items.sorted(lambda x: -x)
        self.assertIsInstance(container, GroupedList)
        self.assertEqual(container.grouped, True)
        self.assertEqual(container.key, None)
        self.assertEqual(len(container), 3)
        items0, items1, items2 = container
        
        self.assertIsInstance(items0, GroupedList)
        self.assertEqual(items0.grouped, False)
        self.assertEqual(items0.key, 0)
        self.assertEqual(list(items0), [9, 6, 3, 0])

        self.assertIsInstance(items1, GroupedList)
        self.assertEqual(items1.grouped, False)
        self.assertEqual(items1.key, 1)
        self.assertEqual(list(items1), [7, 4, 1])

        self.assertIsInstance(items2, GroupedList)
        self.assertEqual(items2.grouped, False)
        self.assertEqual(items2.key, 2)
        self.assertEqual(list(items2), [8, 5, 2])

    def testCancel(self):
        # Flat
        trades = [
            Trade(fitid='3723709320', dttrade=datetime.datetime(2011, 5, 9, 0, 0), memo='CONVERA CORPORATION - SPINOFF', uniqueidtype='CONID', uniqueid='132118505', units=Decimal('-0.276942'), currency='USD', total=Decimal('0.000002769'), reportdate=datetime.datetime(2013, 8, 1, 0, 0), notes=['']),
            Trade(fitid='3831648707', dttrade=datetime.datetime(2011, 5, 9, 0, 0), memo='CONVERA CORPORATION - SPINOFF', uniqueidtype='CONID', uniqueid='132118505', units=Decimal('0.276942'), currency='USD', total=Decimal('-0.000002769'), reportdate=datetime.datetime(2013, 9, 20, 0, 0), notes=['Ca']),
            Trade(fitid='3831652905', dttrade=datetime.datetime(2011, 5, 9, 0, 0), memo='CONVERA CORPORATION - SPINOFF', uniqueidtype='CONID', uniqueid='132118505', units=Decimal('-0.276942'), currency='USD', total=Decimal('56.412710421'), reportdate=datetime.datetime(2013, 9, 20, 0, 0), notes=['']),
            Trade(fitid='3964505548', dttrade=datetime.datetime(2011, 5, 9, 0, 0), memo='CONVERA CORPORATION - SPINOFF', uniqueidtype='CONID', uniqueid='132118505', units=Decimal('0.276942'), currency='USD', total=Decimal('-56.412710421'), reportdate=datetime.datetime(2013, 11, 18, 0, 0), notes=['Ca']),
            Trade(fitid='3964508206', dttrade=datetime.datetime(2011, 5, 9, 0, 0), memo='CONVERA CORPORATION - SPINOFF', uniqueidtype='CONID', uniqueid='132118505', units=Decimal('-0.276942'), currency='USD', total=Decimal('1477.3194048'), reportdate=datetime.datetime(2013, 11, 18, 0, 0), notes=['']),
        ]
        net = GroupedList(trades).cancel(
            filterfunc=lambda tx: 'Ca' in tx.notes,
            matchfunc=lambda tx0, tx1: tx0.units == -tx1.units,
            sortfunc=None
        )
        self.assertIsInstance(net, GroupedList)
        self.assertFalse(net.grouped)
        self.assertIsNone(net.key)
        self.assertEqual(len(net), 1)
        tx = net[0]
        self.assertEqual(tx.fitid, '3964508206')
        self.assertEqual(tx.dttrade, datetime.datetime(2011, 5, 9, 0, 0))
        self.assertEqual(tx.memo, 'CONVERA CORPORATION - SPINOFF')
        self.assertEqual(tx.uniqueidtype, 'CONID')
        self.assertEqual(tx.uniqueid, '132118505')
        self.assertEqual(tx.units, Decimal('-0.276942'))
        self.assertEqual(tx.currency, 'USD')
        self.assertEqual(tx.total, Decimal('1477.3194048'))
        self.assertEqual(tx.reportdate, datetime.datetime(2013, 11, 18, 0, 0))
        self.assertEqual(tx.notes, [''])

        # Nested
        container = GroupedList([GroupedList(key=0), GroupedList(trades, key=1)],
                                grouped=True, key=None).cancel(
                                    filterfunc=lambda tx: 'Ca' in tx.notes,
                                    matchfunc=lambda tx0, tx1: tx0.units == -tx1.units,
                                    sortfunc=None
                                )
        self.assertIsInstance(container, GroupedList)
        self.assertTrue(container.grouped)
        self.assertIsNone(container.key)
        self.assertEqual(len(container), 2)
        items0, items1 = container

        self.assertIsInstance(items0, GroupedList)
        self.assertFalse(items0.grouped)
        self.assertEqual(items0.key, 0)
        self.assertEqual(len(items0), 0)

        self.assertIsInstance(items1, GroupedList)
        self.assertFalse(items1.grouped)
        self.assertEqual(items1.key, 1)
        self.assertEqual(len(items1), 1)
        tx = items1[0]
        self.assertEqual(tx.fitid, '3964508206')
        self.assertEqual(tx.dttrade, datetime.datetime(2011, 5, 9, 0, 0))
        self.assertEqual(tx.memo, 'CONVERA CORPORATION - SPINOFF')
        self.assertEqual(tx.uniqueidtype, 'CONID')
        self.assertEqual(tx.uniqueid, '132118505')
        self.assertEqual(tx.units, Decimal('-0.276942'))
        self.assertEqual(tx.currency, 'USD')
        self.assertEqual(tx.total, Decimal('1477.3194048'))
        self.assertEqual(tx.reportdate, datetime.datetime(2013, 11, 18, 0, 0))
        self.assertEqual(tx.notes, [''])


if __name__ == '__main__':
    unittest.main(verbosity=3)
