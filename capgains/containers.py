# coding: utf-8
"""Container types implementing fluent interfaces.

'Fluent interface' here means processing pipelines implemented by method chaining, e.g.

    GroupedList(transactions)
    .filter(isInteresting)
    .groupby(keyFunc)
    .reduce(netTransactions)
    .filter(operator.attrgetter("total"))
    .map(sum)
"""
from __future__ import annotations

import functools
import itertools
from dataclasses import dataclass
from typing import Callable, Iterable, Optional, Any


@dataclass(frozen=True, init=False)
class FirstResult:
    """Container that composes a series of functions taking the same input
    args, applying each function in sequence until it receives a result that
    isn't None.  The first non-None result is stored, after which the
    remainder of function chain isn't evaluated.

    From a performance standpoint it would be better to make use of
    short-circuiting, using e.g. boolean functions or the "first_true"
    recipe from the itertools module.  However, this approach is much
    more readable.
    """
    args: tuple
    result: Any

    __slots__ = "args", "result"

    def __init__(self, *args, result=None) -> None:
        object.__setattr__(self, "args", args)
        object.__setattr__(self, "result", result)

    def attempt(self, func: Callable) -> FirstResult:
        if self.result:
            return self
        return self.__class__(*self.args, result=func(*self.args))


class GroupedList(list):
    """Container that groups items and composes operations on those groups/items.

    List items must either be all data, or all nested GroupedList instances.

    Public methods all transform the contained data or grouping structure,
    and return a GroupedList instance containing the transformed grouping/data,
    so that method chaining may be applied.

    Attributes:
        grouped: if False, the instance's list contents are data items.
                 If True, the instance contains other GroupedList instances.
        key: the output value of the function used to form the group.
             This value is None for the root instance.

    >>> grp = GroupedList(range(8))
    >>> grp.groupby(lambda x: x >= 4)
    GroupedList([GroupedList([0, 1, 2, 3], grouped=False, key=False), GroupedList([4, 5, 6, 7], grouped=False, key=True)], grouped=True, key=None)
    >>> grp_ = grp.groupby(lambda x: x >= 4).groupby(lambda x: x % 2)
    >>> for g in grp_:
    ...     print(g)
    ...
    GroupedList([GroupedList([0, 2], grouped=False, key=0), GroupedList([1, 3], grouped=False, key=1)], grouped=True, key=False)
    GroupedList([GroupedList([4, 6], grouped=False, key=0), GroupedList([5, 7], grouped=False, key=1)], grouped=True, key=True)
    >>> grp_.flatten()
    GroupedList([0, 2, 1, 3, 4, 6, 5, 7], grouped=False, key=None)
    >>> grp_.flatten().sort() == grp
    True
    >>> # bind() operates on innermost GroupedList; takes function that returns an iterable
    ... for g in grp_.bind(lambda l: l + [99]):
    ...     print(g)
    ...
    GroupedList([GroupedList([0, 2, 4, 99], grouped=False, key=0), GroupedList([1, 3, 99], grouped=False, key=1)], grouped=True, key=False)
    GroupedList([GroupedList([6, 8, 99], grouped=False, key=0), GroupedList([5, 7, 99], grouped=False, key=1)], grouped=True, key=True)
    >>> grp_.bind(lambda l: l + [99]).filter(lambda x: x < 99) == grp_
    True
    >>> # map() and reduce() operate on data items
    >>> grp_.map(str).flatten()
    GroupedList(['0', '2', '4', '1', '3', '6', '8', '5', '7'], grouped=False, key=None)
    >>> grp_.reduce(lambda x, y: x + y).flatten()[:]
    [6, 4, 14, 12]
    """

    def __init__(self, *args, **kwargs):
        self.grouped = kwargs.pop("grouped", False)
        self.key = kwargs.pop("key", None)
        list.__init__(self, *args, **kwargs)

    def __repr__(self):
        return f"GroupedList({list(self)}, grouped={self.grouped}, key={self.key})"

    def groupby(self, func: Callable) -> GroupedList:
        """Group bottom-level data items with function, preserving structure above.

        Increases nesting depth by 1,
        """
        if self.grouped:
            return self.__class__(
                [item.groupby(func) for item in self],
                grouped=self.grouped,
                key=self.key,
            )
        else:
            items = [
                self.__class__(v, grouped=False, key=k)
                for k, v in itertools.groupby(sorted(self, key=func), key=func)
            ]
            return self.__class__(items, grouped=True, key=self.key)

    def flatten(self) -> GroupedList:
        """Chain bottom-level data items.  Reduces nesting depth to zero.
        """
        if self.grouped:
            return type(self)(
                itertools.chain.from_iterable(item.flatten() for item in self),
                grouped=False,
                key=None,
            )
        else:
            return type(self)(self, grouped=False, key=None)

    def bind(self, func: Callable[[Iterable], Iterable]) -> GroupedList:
        """Applies function to bottom-level GroupedList instances (data-bearing).

        Args:
            func: function accepting and returning a list.
        """
        if self.grouped:
            items = [item.bind(func) for item in self]
        else:
            items = list(func(self))
        return self.__class__(items, grouped=self.grouped, key=self.key)

    def sort(self, func: Optional[Callable] = None) -> GroupedList:  # type: ignore
        return self.bind(functools.partial(sorted, key=func))

    def filter(self, func: Optional[Callable[[Any], bool]] = None) -> GroupedList:
        # "If function is None, the identity function is assumed,
        # that is, all elements of iterable that are false are removed."
        # https://docs.python.org/3/library/functions.html#filter
        return self.bind(functools.partial(filter, func))

    def map(self, func: Callable[[Any], Any]) -> GroupedList:
        return self.bind(functools.partial(map, func))

    def reduce(self, func: Callable[[Any, Any], Any]) -> GroupedList:
        """Bind function with functools.reduce() to bottom-level GroupedList instances.

        Args:
            func: function accepting a pair of data items and returning a single
                  transformed data item.
        """
        return self.bind(
            lambda items: [functools.reduce(func, items)] if items else []
        )
