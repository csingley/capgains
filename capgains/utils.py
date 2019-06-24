"""
Utility functions used by capgains modules
"""
import itertools
from decimal import Decimal, ROUND_HALF_UP
import datetime
from typing import Any, Tuple, Iterable, Callable, Union


def partition(
    pred: Callable[[Any], bool], iterable: Iterable
) -> Tuple[Iterable, Iterable]:
    """Use a predicate to partition entries into false entries and true entries

    https://docs.python.org/3/library/itertools.html#itertools-recipes
    """
    # partition(is_odd, range(10)) --> 0 2 4 6 8   and  1 3 5 7 9
    t1, t2 = itertools.tee(iterable)
    return itertools.filterfalse(pred, t1), filter(pred, t2)


def all_equal(iterable):
    """Returns True if all the elements are equal to each other

    https://docs.python.org/3/library/itertools.html#itertools-recipes
    """
    g = itertools.groupby(iterable)
    return next(g, True) and not next(g, False)


def first_true(iterable, default=False, pred=None):
    """Returns the first true value in the iterable.

    If no true value is found, returns *default*

    If *pred* is not None, returns the first item
    for which pred(item) is true.

    https://docs.python.org/3/library/itertools.html#itertools-recipes
    """
    # first_true([a,b,c], x) --> a or b or c or x
    # first_true([a,b], x, f) --> a if f(a) else b if f(b) else x
    return next(filter(pred, iterable), default)


def matchEverything(element: Any) -> bool:
    """Degenerate predicate that always return True"""
    return True


def sign(x) -> int:
    """Extract the sign of a number (+1, -1, or 0)"""
    return (x != 0) and (1, -1)[x < 0]


def round_decimal(number: Union[int, Decimal], power: int = -4) -> Decimal:
    """Convert to Decimal; round to units if possible, else round to desired exponent.
    """
    d = Decimal(number)
    return (
        d.quantize(Decimal(1))
        if d == d.to_integral_value()
        else d.quantize(Decimal("10") ** power, rounding=ROUND_HALF_UP)
    )


MATERIALITY_TOLERANCE = Decimal("0.001")


def almost_equal(number0: Union[int, Decimal], number1: Union[int, Decimal]) -> bool:
    return abs(number0 - number1) < MATERIALITY_TOLERANCE  # type: ignore


def realize_longterm(
    units: Union[float, Decimal],
    opendt: Union[datetime.date, datetime.datetime],
    closedt: Union[datetime.date, datetime.datetime],
) -> bool:
    """Returns True if a realization is eligible for long-term capital gains treatment.

    IRS Pub 550
    '''
    If you hold investment property more than 1 year, any capital gain or loss is a
    long-term capital gain or loss. If you hold the property 1 year or less, any capital
    gain or loss is a short-term capital gain or loss.  To determine how long you held
    the investment property, begin counting on the date after the day you acquired the
    property. The day you disposed of the property is part of your holding period.
    ...
    For securities traded on an established securities market, your holding period
    begins the day after the trade date you bought the securities, and ends on the
    trade date you sold them.
    ...
    Your gain, if any, when you close the short sale is a short-term capital gain
    '''

    Args:
        units: amount of asset being realized (+ for sale of long, - for closing short)
        opendt: trade date (not settlement date) of the opening transaction.
        closedt: trade date (not settlement date) of the realizing transaction.
    """
    if units < 0:
        return False

    opendt_ = opendt + datetime.timedelta(days=1)

    period_months = 12 * (closedt.year - opendt_.year) + (closedt.month - opendt_.month)
    if period_months > 12 or period_months == 12 and closedt.day >= opendt_.day:
        return True

    return False
