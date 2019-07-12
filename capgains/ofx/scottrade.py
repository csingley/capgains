# coding: utf-8
""" """
# Local imports
from capgains.ofx import reader


BROKERID = "www.scottrade.com"


class OfxStatementReader(reader.OfxStatementReader):
    @staticmethod
    def is_trade_cancel(transaction: reader.Trade) -> bool:
        cancel = False
        memo = transaction.memo
        #  if memo and 'to cancel a previous' in memo.lower():
        if memo and "cancel" in memo.lower():
            cancel = True
        return cancel
