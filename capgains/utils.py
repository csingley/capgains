"""
Utility functions used by capgains modules
"""
import itertools
import numbers
from typing import Any, Tuple, Iterable, Callable, Optional


def partition(
    pred: Callable[[Any], bool], iterable: Iterable
) -> Tuple[Iterable, Iterable]:
    """Use a predicate to partition entries into false entries and true entries

    https://docs.python.org/3/library/itertools.html#itertools-recipes
    """
    # partition(is_odd, range(10)) --> 0 2 4 6 8   and  1 3 5 7 9
    t1, t2 = itertools.tee(iterable)
    return itertools.filterfalse(pred, t1), filter(pred, t2)


def matchEverything(element: Any) -> bool:
    """Degenerate predicate that always return True"""
    return True


def sign(x: numbers.Real) -> int:
    """Extract the sign of a number (+1, -1, or 0)"""
    return (x != 0) and (1, -1)[x < 0]
