# coding: utf-8
"""
"""
# stdlib imports
import functools
import itertools


# local imports
from capgains import utils


class GroupedList(list):
    """
    Container that groups items and composes operations on those groups/items.

    If grouped=False, then it directly contains data (and key is also =False).

    If grouped=True, then contents are GroupedList instances whose key
    attribute is set to the output value of the function used to form the
    group.  These contained GroupedLists may contain data (in which case they
    have grouped=False), or they may in turn contain further nested
    GroupedList instance (in which case they have grouped=True).

    Methods defined here all return GroupedList instances with methods applied,
    for easy function composition.
    """

    def __init__(self, *args, **kwargs):
        self.grouped = kwargs.pop("grouped", False)
        self.key = kwargs.pop("key", None)
        list.__init__(self, *args, **kwargs)

    def __repr__(self):
        return "GroupedList({}, grouped={}, key={})".format(
            list(self), self.grouped, self.key
        )

    def bind_data(self, func):
        """
        Applies input function to data items (i.e my own items if they're data,
        or my children's items if I contain GroupedLists).
        """
        if self.grouped:
            items = [item.bind_data(func) for item in self]
        else:
            items = func(self)
        return self.__class__(items, grouped=self.grouped, key=self.key)

    def sorted(self, func):
        return self.bind_data(functools.partial(sorted, key=func))

    def filter(self, func=None):
        # "If function is None, the identity function is assumed,
        # that is, all elements of iterable that are false are removed."
        # https://docs.python.org/3/library/functions.html#filter
        return self.bind_data(functools.partial(filter, func))

    def map(self, func):
        return self.bind_data(functools.partial(map, func))

    def reduce(self, func):
        return self.bind_data(
            lambda items: [functools.reduce(func, items)] if items else []
        )

    def flatten(self):
        if self.grouped:
            items = itertools.chain.from_iterable(item.flatten() for item in self)
        else:
            items = self
        return self.__class__(items, grouped=False, key=None)

    def groupby(self, func):
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

    def cancel(self, filterfunc, matchfunc, sortfunc):
        """
        Identify and apply cancelling transactions.

        Specialized method for reversing/cancelled transactions e.g.
        dividend adjustments & trades.

        Args: filterfunc - function consuming Transaction and returning bool.
                             Judges whether a given transaction is a cancelling
                             transaction.
              matchfunc - function consuming (Transaction, Transaction) and
                      returning bool.  Judges whether two transactions cancel
                      each other out.
              sortfunc - function consuming Transaction and returning sort key.
                     Used to sort original transactions, against which
                     matching cancelling transacions will be applied in order.
        """

        def applyCancel(items):
            originals, cancels = utils.partition(filterfunc, items)
            originals = sorted(originals, key=sortfunc)

            for cancel in cancels:
                canceled = first_true(
                    originals, pred=functools.partial(matchfunc, cancel)
                )
                if canceled is False:
                    raise ValueError(
                        "Can't find Transaction canceled by {}".format(cancel)
                    )
                # N.B. must remove canceled transaction from further iterations
                # to avoid multiple cancels matching the same original, thereby
                # leaving subsequent original(s) uncanceled when they should be
                originals.remove(canceled)
            return originals

        return self.bind_data(applyCancel)


def first_true(iterable, default=False, pred=None):
    """Returns the first true value in the iterable.

    If no true value is found, returns *default*

    If *pred* is not None, returns the first item
    for which pred(item) is true.

    """
    # first_true([a,b,c], x) --> a or b or c or x
    # first_true([a,b], x, f) --> a if f(a) else b if f(b) else x
    return next(filter(pred, iterable), default)
