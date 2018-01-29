# coding: utf-8
""" """
# Local imports
from capgains.ofx import reader


BROKERID = 'etrade.com'


class OfxStatementReader(reader.OfxStatementReader):
    @staticmethod
    def filterCashTransactions(transaction):
        """
        Judge whether a transaction represents a return of capital.

        Args: transaction -
        Returns: bool
        """
        memo = transaction.memo.lower()
        return 'ret cap' in memo or 'liqd' in memo
