# coding: utf-8
"""
Unit tests for capgains.inventory.sortkeys
"""
# stdlib imports
import unittest
from decimal import Decimal
from datetime import datetime


# local imports
from capgains.inventory import (
    FIFO,
    LIFO,
    MINGAIN,
    MAXGAIN,
    Lot,
    Trade,
)


class SortTestCase(unittest.TestCase):
    def testFifoSort(self):
        """
        FIFO sorts first by Lot.opentransaction.datetime,
        then by Lot.opentransaction.uniqueid
        """
        tx1 = Trade(
            id=1,
            uniqueid="b",
            datetime=datetime(2005, 10, 3),
            fiaccount="",
            security="",
            cash=None,
            currency=None,
            units=None,
        )
        lot1 = Lot(
            opentransaction=tx1,
            createtransaction=tx1,
            units=None,
            price=None,
            currency=None,
        )
        tx2 = Trade(
            id=2,
            uniqueid="c",
            datetime=datetime(2005, 10, 4),
            fiaccount="",
            security="",
            cash=None,
            currency=None,
            units=None,
        )
        lot2 = Lot(
            opentransaction=tx2,
            createtransaction=tx2,
            units=None,
            price=None,
            currency=None,
        )
        tx3 = Trade(
            id=3,
            uniqueid="a",
            datetime=datetime(2005, 10, 3),
            fiaccount="",
            security="",
            cash=None,
            currency=None,
            units=None,
        )
        lot3 = Lot(
            opentransaction=tx3,
            createtransaction=tx3,
            units=None,
            price=None,
            currency=None,
        )
        position = [lot2, lot1, lot3]
        position.sort(**FIFO)
        self.assertEqual(position, [lot3, lot1, lot2])

    def testLifoSort(self):
        """
        LIFO sorts first by Lot.opentransaction.datetime,
        then by Lot.opentransaction.uniqueid
        """
        tx1 = Trade(
            id=1,
            uniqueid="b",
            datetime=datetime(2005, 10, 3),
            fiaccount="",
            security="",
            cash=None,
            currency=None,
            units=None,
        )
        lot1 = Lot(
            opentransaction=tx1,
            createtransaction=tx1,
            units=None,
            price=None,
            currency=None,
        )
        tx2 = Trade(
            id=2,
            uniqueid="c",
            datetime=datetime(2005, 10, 4),
            fiaccount="",
            security="",
            cash=None,
            currency=None,
            units=None,
        )
        lot2 = Lot(
            opentransaction=tx2,
            createtransaction=tx2,
            units=None,
            price=None,
            currency=None,
        )
        tx3 = Trade(
            id=3,
            uniqueid="a",
            datetime=datetime(2005, 10, 3),
            fiaccount="",
            security="",
            cash=None,
            currency=None,
            units=None,
        )
        lot3 = Lot(
            opentransaction=tx3,
            createtransaction=tx3,
            units=None,
            price=None,
            currency=None,
        )
        position = [lot3, lot1, lot2]
        position.sort(**LIFO)
        self.assertEqual(position, [lot2, lot1, lot3])

    def testMinGainSort(self):
        """
        MINGAIN sorts first by Lot.price, then by Lot.opentransaction.uniqueid
        """
        tx1 = Trade(
            id=1,
            uniqueid="b",
            datetime=datetime(2005, 10, 3),
            fiaccount="",
            security="",
            cash=None,
            currency=None,
            units=None,
        )
        lot1 = Lot(
            opentransaction=tx1,
            createtransaction=None,
            units=None,
            price=Decimal("10"),
            currency="USD",
        )
        tx2 = Trade(
            id=1,
            uniqueid="c",
            datetime=datetime(2005, 10, 3),
            fiaccount="",
            security="",
            cash=None,
            currency=None,
            units=None,
        )
        lot2 = Lot(
            opentransaction=tx2,
            createtransaction=None,
            units=None,
            price=Decimal("9.5"),
            currency="USD",
        )
        tx3 = Trade(
            id=1,
            uniqueid="a",
            datetime=datetime(2005, 10, 3),
            fiaccount="",
            security="",
            cash=None,
            currency=None,
            units=None,
        )
        lot3 = Lot(
            opentransaction=tx3,
            createtransaction=None,
            units=None,
            price=Decimal("10"),
            currency="USD",
        )
        position = [lot1, lot2, lot3]
        position.sort(**MINGAIN)
        self.assertEqual(position, [lot3, lot1, lot2])

    def testMaxGainSort(self):
        """
        MAXGAIN sorts first by Lot.price, then by Lot.opentransaction.uniqueid
        """
        tx1 = Trade(
            id=1,
            uniqueid="b",
            datetime=datetime(2001, 1, 1),
            fiaccount="",
            security="",
            cash=None,
            currency=None,
            units=None,
        )
        lot1 = Lot(
            opentransaction=tx1,
            createtransaction=None,
            units=None,
            price=Decimal("10"),
            currency="USD",
        )
        tx2 = Trade(
            id=1,
            uniqueid="c",
            datetime=datetime(2001, 1, 1),
            fiaccount="",
            security="",
            cash=None,
            currency=None,
            units=None,
        )
        lot2 = Lot(
            opentransaction=tx2,
            createtransaction=None,
            units=None,
            price=Decimal("9.5"),
            currency="USD",
        )
        tx3 = Trade(
            id=1,
            uniqueid="a",
            datetime=datetime(2001, 1, 2),
            fiaccount="",
            security="",
            cash=None,
            currency=None,
            units=None,
        )
        lot3 = Lot(
            opentransaction=tx3,
            createtransaction=None,
            units=None,
            price=Decimal("10"),
            currency="USD",
        )
        position = [lot1, lot2, lot3]
        position.sort(**MAXGAIN)
        self.assertEqual(position, [lot2, lot3, lot1])


if __name__ == "__main__":
    unittest.main()
