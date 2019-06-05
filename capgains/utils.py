"""
Utility functions used by capgains modules
"""
import itertools


def partition(pred, iterable):
    """
    Use a predicate to partition entries into false entries and true entries

    https://docs.python.org/3/library/itertools.html#itertools-recipes
    """
    # partition(is_odd, range(10)) --> 0 2 4 6 8   and  1 3 5 7 9
    t1, t2 = itertools.tee(iterable)
    return itertools.filterfalse(pred, t1), filter(pred, t2)
