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
# stdlib imports
import functools
import itertools
from typing import Callable, Iterable, Optional, Any


# local imports
from capgains import models, utils


ListFunction = Callable[[Iterable], Iterable]


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
    """

    def __init__(self, *args, **kwargs):
        self.grouped = kwargs.pop("grouped", False)
        self.key = kwargs.pop("key", None)
        list.__init__(self, *args, **kwargs)

    def __repr__(self):
        return f"GroupedList({list(self)}, grouped={self.grouped}, key={self.key})"

    def groupby(self, func: Callable) -> "GroupedList":
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

    def flatten(self) -> "GroupedList":
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

    def bind(self, func: ListFunction) -> "GroupedList":
        """Applies function to bottom-level GroupedList instances (data-bearing).

        Args:
            func: function accepting and returning a list.
        """
        if self.grouped:
            items = [item.bind(func) for item in self]
        else:
            items = list(func(self))
        return self.__class__(items, grouped=self.grouped, key=self.key)

    def sort(self, func: ListFunction) -> "GroupedList":
        return self.bind(functools.partial(sorted, key=func))

    def filter(self, func: Optional[ListFunction] = None) -> "GroupedList":
        # "If function is None, the identity function is assumed,
        # that is, all elements of iterable that are false are removed."
        # https://docs.python.org/3/library/functions.html#filter
        return self.bind(functools.partial(filter, func))

    def map(self, func: ListFunction) -> "GroupedList":
        return self.bind(functools.partial(map, func))

    def reduce(self, func: Callable[[Any, Any], Any]) -> "GroupedList":
        """Bind function with functools.reduce() to bottom-level GroupedList instances.

        Args:
            func: function accepting a pair of data items and returning a single
                  transformed data item.
        """
        return self.bind(
            lambda items: [functools.reduce(func, items)] if items else []
        )
