# coding: utf-8
""" """
# Local imports
from capgains.ofx import reader


BROKERID = "etrade.com"


class OfxStatementReader(reader.OfxStatementReader):
    @staticmethod
    def is_retofcap(transaction: reader.CashTransaction) -> bool:
        memo = transaction.memo.lower()
        return "ret cap" in memo or "liqd" in memo
