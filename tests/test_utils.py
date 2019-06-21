# coding: utf-8
"""
Unit tests for capgains.utils
"""
import unittest
import datetime

from capgains import utils


class RealizeLongTermTestCase(unittest.TestCase):
    def test_date_or_datetime(self):
        oneday = datetime.timedelta(days=1)

        # Any combination of datetime.date and datetime.datetime works
        opendt = datetime.datetime(2014, 1, 1)
        closedt = datetime.datetime(2015, 1, 1)

        self.assertFalse(utils.realize_longterm(1, opendt, closedt))
        self.assertTrue(utils.realize_longterm(1, opendt, closedt + oneday))

        self.assertFalse(utils.realize_longterm(1, opendt.date(), closedt))
        self.assertTrue(utils.realize_longterm(1, opendt.date(), closedt + oneday))

        self.assertFalse(utils.realize_longterm(1, opendt, closedt.date()))
        self.assertTrue(utils.realize_longterm(1, opendt, (closedt + oneday).date()))

        self.assertFalse(utils.realize_longterm(1, opendt.date(), closedt.date()))
        self.assertTrue(utils.realize_longterm(1, opendt.date(), (closedt + oneday).date()))

    def test_leapyear(self):
        # Elapsed days are NOT used to determine long-term status.
        # Just compare calendar pages.

        # Normal year
        opendate = datetime.date(2014, 2, 28)
        closedate = datetime.date(2015, 2, 28)
        self.assertLessEqual((closedate - opendate).days, 365)  # This is different!
        self.assertFalse(utils.realize_longterm(1, opendate, closedate))

        closedate += datetime.timedelta(days=1)
        self.assertTrue(utils.realize_longterm(1, opendate, closedate))

        # Leap year
        opendate = datetime.date(2016, 2, 28)
        closedate = datetime.date(2017, 2, 28)
        self.assertGreater((closedate - opendate).days, 365)  # This is different!
        self.assertFalse(utils.realize_longterm(1, opendate, closedate))

        closedate += datetime.timedelta(days=1)
        self.assertTrue(utils.realize_longterm(1, opendate, closedate))


if __name__ == "__main__":
    unittest.main()
