# coding: utf-8
"""
Unit tests for capgains.inventory.functions
"""
# stdlib imports
import unittest
from decimal import Decimal
from datetime import datetime


# local imports
from capgains.inventory import (
    Trade,
    Lot,
    part_units,
    part_basis,
    openAsOf,
)


class LotsMixin(object):
    def setUp(self):
        tx1 = Trade(
            datetime=datetime(2016, 1, 1),
            uniqueid="",
            fiaccount="",
            security=None,
            units=Decimal("100"),
            cash=Decimal("1000"),
            currency="USD",
        )
        self.lot1 = Lot(
            opentransaction=tx1,
            createtransaction=tx1,
            units=tx1.units,
            price=abs(tx1.cash / tx1.units),
            currency=tx1.currency,
        )

        tx2 = Trade(
            datetime=datetime(2016, 1, 2),
            uniqueid="",
            fiaccount="",
            security=None,
            units=Decimal("200"),
            cash=Decimal("2200"),
            currency="USD",
        )
        self.lot2 = Lot(
            opentransaction=tx2,
            createtransaction=tx2,
            units=tx2.units,
            price=abs(tx2.cash / tx2.units),
            currency=tx2.currency,
        )

        tx3 = Trade(
            datetime=datetime(2016, 1, 3),
            uniqueid="",
            fiaccount="",
            security=None,
            units=Decimal("300"),
            cash=Decimal("3600"),
            currency="USD",
        )
        self.lot3 = Lot(
            opentransaction=tx3,
            createtransaction=tx3,
            units=tx3.units,
            price=abs(tx3.cash / tx3.units),
            currency=tx3.currency,
        )

        self.lots = [self.lot1, self.lot2, self.lot3]


class PartUnitsTestCase(LotsMixin, unittest.TestCase):
    def testPartUnits(self):
        """
        part_units() removes Lots from the beginning, partioning Lots as needed.
        """
        self.assertEqual(len(self.lots), 3)
        taken, left = part_units(self.lots, max_units=Decimal("150"))

        self.assertEqual(len(taken), 2)
        self.assertEqual(taken[0], self.lot1)
        self.assertEqual(
            taken[1], self.lot2._replace(units=Decimal("50"), price=Decimal("11"))
        )

        self.assertEqual(len(left), 2)
        self.assertEqual(
            left[0], self.lot2._replace(units=Decimal("150"), price=Decimal("11"))
        )
        self.assertEqual(left[1], self.lot3)

    def testPartUnitsMaxUnitsNone(self):
        """
        part_units() takes all matches if max_units is None
        """
        self.assertEqual(len(self.lots), 3)
        taken, left = part_units(self.lots)

        self.assertEqual(len(taken), 3)
        self.assertEqual(taken, self.lots)

        self.assertEqual(len(left), 0)

    def testPartUnitsPredicate(self):
        """
        part_units() respects lot selection criteria
        """
        predicate = openAsOf(datetime(2016, 1, 2))
        taken_lots, left_lots = part_units(self.lots, predicate=predicate)

        self.assertEqual(len(taken_lots), 2)
        self.assertEqual(len(left_lots), 1)

        self.assertEqual(taken_lots, [self.lot1, self.lot2])
        self.assertEqual(left_lots, [self.lot3])

    def testPartUnitsMaxUnitsPredicate(self):
        """
        part_units() respects predicate and max_units args together
        """
        predicate = openAsOf(datetime(2016, 1, 2))
        taken_lots, left_lots = part_units(
            self.lots, max_units=Decimal("150"), predicate=predicate
        )

        self.assertEqual(len(taken_lots), 2)
        self.assertEqual(len(left_lots), 2)

        self.assertEqual(
            taken_lots, [self.lot1, self.lot2._replace(units=Decimal("50"))]
        )
        self.assertEqual(
            sorted(left_lots),
            sorted([self.lot2._replace(units=Decimal("150")), self.lot3]),
        )


class PartBasisTestCase(LotsMixin, unittest.TestCase):
    def testPartBasis(self):
        """
        Position.part_basis() takes cost from all Lots w/o changing units/date
        """
        orig_cost = sum([(l.units * l.price) for l in self.lots])
        fraction = Decimal("0.25")
        taken_lots, left_lots = part_basis(self.lots, predicate=None, fraction=fraction)

        left_cost = sum([(l.units * l.price) for l in left_lots])
        taken_cost = sum([(l.units * l.price) for l in taken_lots])
        self.assertEqual(taken_cost + left_cost, orig_cost)
        self.assertEqual(taken_cost / (taken_cost + left_cost), fraction)

        for i, lot in enumerate(left_lots):
            takenLot = taken_lots[i]
            self.assertEqual(lot.opentransaction, takenLot.opentransaction)
            self.assertEqual(lot.createtransaction, takenLot.createtransaction)
            self.assertEqual(lot.units, takenLot.units)

    def testPartBasisBadFraction(self):
        """
        part_basis() only accepts fraction between 0 and 1 inclusive
        """
        with self.assertRaises(ValueError):
            part_basis(self.lots, predicate=None, fraction=Decimal("-0.1"))
        with self.assertRaises(ValueError):
            part_basis(self.lots, predicate=None, fraction=Decimal("1.01"))

    def testPartBasisPredicate(self):
        """
        part_basis() respects lot selection criteria
        """
        predicate = openAsOf(datetime(2016, 1, 2))

        orig_cost = sum([(l.units * l.price) for l in self.lots])
        fraction = Decimal("0.25")
        taken_lots, left_lots = part_basis(
            self.lots, predicate=predicate, fraction=fraction
        )

        self.assertEqual(len(taken_lots), 2)
        self.assertEqual(len(left_lots), 3)

        taken_cost = sum([(l.units * l.price) for l in taken_lots])
        left_cost = sum([(l.units * l.price) for l in left_lots])
        self.assertEqual(taken_cost + left_cost, orig_cost)

        affected_cost = sum([l.units * l.price for l in (self.lot1, self.lot2)])
        self.assertEqual(taken_cost / affected_cost, fraction)

        self.assertEqual(
            taken_lots,
            [
                self.lot1._replace(price=fraction * self.lot1.price),
                self.lot2._replace(price=fraction * self.lot2.price),
            ],
        )

        self.assertEqual(
            left_lots,
            [
                self.lot1._replace(price=(1 - fraction) * self.lot1.price),
                self.lot2._replace(price=(1 - fraction) * self.lot2.price),
                self.lot3,
            ],
        )


if __name__ == "__main__":
    unittest.main()
