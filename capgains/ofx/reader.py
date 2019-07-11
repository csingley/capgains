# coding: utf-8
"""
Creates model instances from OFX downloads.
"""
# stdlib imports
from datetime import datetime
from decimal import Decimal
import functools
import itertools
import operator
import hashlib
import logging
import warnings
from typing import (
    TYPE_CHECKING,
    Tuple,
    List,
    MutableMapping,
    Callable,
    Optional,
    Any,
    Union,
    Iterable,
)


# 3rd party imports
import sqlalchemy
import ofxtools


# Local imports
from capgains import models, utils
from capgains.containers import GroupedList, ListFunction
from capgains.database import Base, sessionmanager
from capgains import flex

if TYPE_CHECKING:
    Statement = Union[
        ofxtools.models.INVSTMTRS,
        flex.Types.FlexStatement,
    ]

    Trade = Union[
        ofxtools.models.BUYDEBT,
        ofxtools.models.SELLDEBT,
        ofxtools.models.BUYMF,
        ofxtools.models.SELLMF,
        ofxtools.models.BUYOPT,
        ofxtools.models.SELLOPT,
        ofxtools.models.BUYOTHER,
        ofxtools.models.SELLOTHER,
        ofxtools.models.BUYSTOCK,
        ofxtools.models.SELLSTOCK,
        flex.Types.Trade,
    ]

    Transaction = Union[
        Trade,
        ofxtools.models.INCOME,
        ofxtools.models.INVEXPENSE,
        ofxtools.models.TRANSFER,
        flex.Types.Transaction,
    ]

    CashTransaction = Union[
        ofxtools.models.INCOME,
        ofxtools.models.INVEXPENSE,
        flex.Types.CashTransaction,
    ]

    Transfer = Union[ofxtools.models.TRANSFER]

else:
    Trade = None
    Statement = None
    Transaction = None
    CashTransaction = None
    Transfer = None


SecuritiesMap = MutableMapping[Tuple[str, str], models.Security]
"""Map of (uniqueidtype, uniqueid) to ORM Security instance.
"""


#  class OfxResponseReader(object):
    #  """
    #  Processor for ofxtools.models.ofx.OFX instance
    #  """

    #  def __init__(
        #  self,
        #  session: sqlalchemy.orm.session.Session,
        #  response: Optional[ofxtools.models.ofx.OFX] = None,
    #  ) -> None:
        #  self.session = session
        #  response = response or []
        #  self.statements = [OfxStatementReader(session, stmt) for stmt in response]

    #  def read(self):
        #  for stmt in self.statements:
            #  stmt.read()


class OfxStatementReader(object):
    """Processor for ofxtools.models.INVSTMTRS instance
    """

    def __init__(
        self,
        session: sqlalchemy.orm.session.Session,
        statement: Optional[Statement] = None,
        seclist: Optional[ofxtools.models.SECLIST] = None,
    ) -> None:
        self.session = session
        self.statement = statement
        self.seclist = seclist
        #  self.index = None
        self.securities: SecuritiesMap = {}
        self.transactions: List[models.Transaction] = []

    def read(self, doTransactions: bool = True) -> None:
        assert self.statement is not None
        self.currency_default = self.read_default_currency(self.statement)
        self.account = self.read_account(self.statement, self.session)
        self.securities = self.read_securities()
        if doTransactions:
            self.read_transactions()

    @staticmethod
    def read_default_currency(statement: Statement) -> str:
        assert isinstance(statement, ofxtools.models.INVSTMTRS)
        return statement.curdef

    @staticmethod
    def read_account(
        statement: Statement,
        session: sqlalchemy.orm.session.Session,
    ) -> models.FiAccount:
        assert statement is not None
        account = statement.account
        return models.FiAccount.merge(
            session, brokerid=account.brokerid, number=account.acctid
        )

    def read_securities(self) -> SecuritiesMap:
        securities: SecuritiesMap = {}

        assert self.seclist is not None
        for sec in self.seclist:
            uniqueidtype = sec.uniqueidtype
            uniqueid = sec.uniqueid
            secname = sec.secname
            ticker = sec.ticker
            sec = models.Security.merge(
                self.session,
                uniqueidtype=uniqueidtype,
                uniqueid=uniqueid,
                name=secname,
                ticker=ticker,
            )
            securities[(uniqueidtype, uniqueid)] = sec
            # Also do ISIN; why not?
            if uniqueidtype == "CUSIP":
                try:
                    uniqueid = ofxtools.utils.cusip2isin(uniqueid)
                    uniqueidtype = "ISIN"
                    sec = models.Security.merge(
                        self.session,
                        uniqueidtype=uniqueidtype,
                        uniqueid=uniqueid,
                        name=secname,
                        ticker=ticker,
                    )
                    securities[(uniqueidtype, uniqueid)] = sec
                except ValueError:
                    pass

        return securities

    def read_transactions(self) -> None:
        """
        Group parsed statement transaction instances and dispatch groups to
        relevant handler functions
        """
        assert self.statement is not None
        self.statement.transactions.sort(key=self.groupTransactions)
        for handler_name, transactions in itertools.groupby(
            self.statement.transactions, key=self.groupTransactions
        ):
            if handler_name:
                handler = getattr(self, handler_name)
                handler(transactions)

    def groupTransactions(self, transaction: Transaction) -> str:
        """ Group parsed statement transaction instances by class name """
        return self.transaction_handlers.get(transaction.__class__.__name__, "")

    transaction_handlers = {
        "BUYDEBT": "doTrades",
        "SELLDEBT": "doTrades",
        "BUYMF": "doTrades",
        "SELLMF": "doTrades",
        "BUYOPT": "doTrades",
        "SELLOPT": "doTrades",
        "BUYOTHER": "doTrades",
        "SELLOTHER": "doTrades",
        "BUYSTOCK": "doTrades",
        "SELLSTOCK": "doTrades",
        "INCOME": "doCashTransactions",
        "INVEXPENSE": "doCashTransactions",
        "TRANSFER": "doTransfers",
    }

    ###########################################################################
    # TRADES
    ###########################################################################
    def doTrades(self, transactions: Iterable[Trade]) -> None:
        """
        Preprocess trade transactions and send to merge_trade().

        The logic here eliminates unwanted trades (e.g. FX) and groups trades
        to net out canceled trades.

        Args: transactions - a sequence of instances implementing the interface
                             of ofxtools.models.{BUY*, SELL*} (as used by methods below)
        """
        is_interesting = self.filterTrades  # override

        def group_key(transaction: Trade) -> Any:
            """
            Transactions are grouped if they have the same security/datetime
            and matching units.  abs(units) is used so that trade cancellations
            are grouped together with the trades they cancel.
            """
            return (
                transaction.uniqueidtype,
                transaction.uniqueid,
                transaction.dttrade,
                abs(transaction.units),
            )

        apply_cancels = make_canceller(
            filterfunc=self.filterTradeCancels,  # override
            matchfunc=self.matchTradeWithCancel,  # override
            sortfunc=self.sortCanceledTrades,  # override
        )

        _merge_trade = functools.partial(
            merge_trade,
            session=self.session,
            securities=self.securities,
            account=self.account,
            default_currency=self.currency_default,
            sortForTrade=self.sortForTrade,  # override
        )

        transactions = (
            GroupedList(transactions)
            .filter(is_interesting)
            .groupby(group_key)
            .bind(apply_cancels)
            .filter(operator.attrgetter("units"))  # Removes net 0 unit transactions
            .map(_merge_trade)
            .flatten()
        )[:]
        self.transactions.extend(transactions)

    @staticmethod
    def filterTrades(transaction: Trade) -> bool:
        """
        Should this trade be processed?  Implement in subclass.

        Arg: an instance implementing the interface of
              ofxtools.models.investment.{BUY*, SELL*}
        """
        return True

    @staticmethod
    def filterTradeCancels(transaction: Trade) -> bool:
        """
        Is this trade actually a trade cancellation?  Implement in subclass.

        Arg: an instance implementing the interface of
              ofxtools.models.investment.{BUY*, SELL*}
        Returns: boolean
        """
        return False

    @staticmethod
    def matchTradeWithCancel(transaction0: Trade, transaction1: Trade) -> bool:
        """
        Does one of these trades cancel the other?

        Args: two instances implementing the interface of
              ofxtools.models.investment.{BUY*, SELL*}
        Returns: boolean
        """
        return transaction0.units == -1 * transaction1.units

    @staticmethod
    def sortCanceledTrades(transaction: Trade) -> Any:
        """
        Determines order in which trades are canceled.
        """
        return transaction.fitid

    @staticmethod
    def sortForTrade(
        transaction: Transaction
    ) -> Any:
        """
        What flex.parser sort algorithm that applies to this transaction?

        Implement in subclass.
        """
        return None

    ###########################################################################
    # CASH TRANSACTIONS
    ###########################################################################
    def doCashTransactions(self, transactions: List[CashTransaction]) -> None:
        """
        Preprocess cash transactions and send to merge_retofcap().

        The logic here filters only for return of capital transactions;
        groups them to apply reversals; nets cash transactions remaining in
        each group; and persists to the database after applying any final
        preprocessing applied by fixCashTransaction().

        It's important to net cash transactions remaining in a group that
        aren't cancelled, since the cash totals of reversing transactions
        often don't match the totals of the transactions being reversed
        (e.g. partial reversals to recharacterize to/from payment in lieu).

        Args: transactions - a sequence of instances implementing the interface
                             of ofxtools.models.investment.INCOME
        """
        is_interesting = self.filterCashTransactions  # Override

        group_key = self.groupCashTransactionsForCancel  # Override

        apply_cancels = make_canceller(
            filterfunc=self.filterCashTransactionCancels,
            matchfunc=lambda x, y: x.total == -1 * y.total,
            sortfunc=self.sortCanceledCashTransactions,
        )  # override this whole thing

        _merge_retofcap = functools.partial(
            merge_retofcap,
            session=self.session,
            securities=self.securities,
            account=self.account,
            default_currency=self.currency_default,
        )  # override

        cleanup = self.fixCashTransaction  # Override

        transactions_ = (
            GroupedList(transactions)
            .filter(is_interesting)
            .groupby(group_key)
            .bind(apply_cancels)
            .reduce(net_cash)
            .filter(operator.attrgetter("total"))  # Removes net $0 transactions
            .map(cleanup)
            .map(_merge_retofcap)
            .flatten()
        )[:]

        self.transactions.extend(transactions_)

    @staticmethod
    def filterCashTransactions(transaction: CashTransaction) -> bool:
        """
        Judge whether a transaction should be processed (i.e. it's a return
        of capital).

        Implement in subclass.

        Args: transaction - instance implementing the interface of
                            ofxtools.models.investment.INCOME
        Returns: boolean
        """
        return False

    @staticmethod
    def groupCashTransactionsForCancel(transaction: CashTransaction) -> Any:
        """
        Cash transactions are grouped together for cancellation/netting
        if they're for the same security at the same time with the same memo.
        """
        security = (transaction.uniqueidtype, transaction.uniqueid)
        memo = transaction.memo
        return transaction.dttrade, security, memo

    @staticmethod
    def filterCashTransactionCancels(transaction: CashTransaction) -> Any:
        """
        Is this cash transaction actually a reversal?  Implement in subclass.
        Returns: boolean
        """
        return False

    @staticmethod
    def sortCanceledCashTransactions(transaction: CashTransaction) -> Any:
        """
        Determines order in which cash transactions are reversed.

        Implement in subclass.
        """
        return False

    def fixCashTransaction(self, transaction: CashTransaction) -> CashTransaction:
        """
        Any last processing of the transaction before it's persisted to the DB.

        Implement in subclass.
        """
        return transaction

    ###########################################################################
    # ACCOUNT TRANSFERS
    ###########################################################################
    def doTransfers(self, transactions: Iterable[Transfer]) -> None:
        """
        Preprocess transfer transactions - not handled here in the base class,
        since OFX data doesn't really give us enough information to match the
        two sides of a transfer unless we can parse the highly broker-specific
        formatting of transaction memos.

        Implement in subclass.
        """
        for tx in transactions:
            msg = "Skipping transfer {}".format(tx)
            warnings.warn(msg)


########################################################################################
#  Merge functions
########################################################################################
def merge_retofcap(
    transaction: CashTransaction,
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    account: models.FiAccount,
    default_currency: str,
    memo: Optional[str] = None
) -> models.Transaction:
    """
    Process a return of capital cash transaction into data fields to
    hand off to merge_transaction() to persist in the database.

    Args: transaction - instance implementing the interface of
                        ofxtools.models.investment.INCOME
            memo - override transaction memo (type str)
    """
    assert transaction.total > 0
    assert transaction.uniqueidtype is not None
    assert transaction.uniqueid is not None
    security = securities[(transaction.uniqueidtype, transaction.uniqueid)]
    # Work with either Flex currency attribute or OFX Currency Aggregate
    currency = transaction.currency
    if isinstance(currency, ofxtools.models.CURRENCY):
        currency = currency.cursym
    elif currency is None:
        currency = default_currency

    assert isinstance(currency, str)

    dttrade = transaction.dttrade
    dtsettle = getattr(transaction, "dtsettle", None) or dttrade
    transaction_ = merge_transaction(
        session,
        type=models.TransactionType.RETURNCAP,
        fiaccount=account,
        uniqueid=transaction.fitid,
        datetime=dttrade,
        dtsettle=dtsettle,
        memo=memo or transaction.memo,
        security=security,
        currency=models.Currency[currency],
        cash=transaction.total,
    )
    return transaction_


###############################################################################
# HELPER FUNCTIONS
###############################################################################
def make_canceller(
    filterfunc: Callable[[Any], bool],
    matchfunc: Callable[[Any, Any], bool],
    sortfunc: Callable[[Any], Any],
) -> ListFunction:
    """Factory for functions that identify and apply cancelling Transactions,
    e.g. trade cancellations or dividened reversals/reclassifications.

    Args: filterfunc - Judges whether a Transaction is a cancelling Transaction.
          matchfunc - Judges whether two Transactions cancel each other out.
          sortfunc - function consuming Transaction and returning sort key.
                     Used to sort original Transactions, against which matching
                     cancelling transacions will be applied in order.
    """

    def cancel_transactions(transactions):
        originals, cancels = utils.partition(filterfunc, transactions)
        originals = sorted(originals, key=sortfunc)

        cancels = list(cancels)

        for cancel in cancels:
            predicate = functools.partial(matchfunc, cancel)
            canceled = utils.first_true(originals, pred=predicate)
            if canceled is False:
                raise ValueError(
                    f"Can't find Transaction canceled by {cancel}"
                    f"\n in {originals}"
                    f"\n{predicate}"
                    f"\n{[predicate(o) for o in originals]}"
                )
            # N.B. must remove canceled transaction from further iterations
            # to avoid multiple cancels matching the same original, thereby
            # leaving subsequent original(s) uncanceled when they should be
            originals.remove(canceled)
        return originals

    return cancel_transactions


def net_cash(
    transaction0: CashTransaction,
    transaction1: CashTransaction
) -> flex.Types.CashTransaction:
    """
    Combine two cash transactions by summing their totals, and taking
    the earliest of their dates.

    Args: two instances implementing the interface of
        ofxtools.models.investment.INCOME
    Returns: a flex.Types.CashTransaction namedtuple instance (which implements the
            key parts of the INCOME interface)
    """

    def _minDateTime(*args: Optional[datetime]) -> Optional[datetime]:
        """Choose the earliest non-None datetime.
        If all are None, return None.
        """
        non_null = [dt for dt in args if dt is not None]
        if not non_null:
            return None
        return min(non_null)

    dttrade = _minDateTime(transaction0.dttrade, transaction1.dttrade)
    dtsettle = _minDateTime(transaction0.dtsettle, transaction1.dtsettle)
    total = transaction0.total + transaction1.total
    return flex.Types.CashTransaction(
        transaction0.fitid,
        dttrade,
        dtsettle,
        transaction0.memo,
        transaction0.uniqueidtype,
        transaction0.uniqueid,
        transaction0.incometype,
        transaction0.currency,
        total,
    )


def merge_trade(
    tx: Union[Trade, flex.Types.CorporateAction],  # q.v. flex.reader.treat_as_trade()
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    account: models.FiAccount,
    default_currency: str,
    sortForTrade: Callable[[Transaction], Any],
    memo: Optional[str] = None
) -> models.Transaction:
    """
    Process a trade into data fields to hand off to merge_transaction()
    to persist in the database.

    Args: transaction - instance implementing the interface of
                        ofxtools.models.investment.{BUY*, SELL*}
            memo - override transaction memo (type str)
    """
    security = securities[(tx.uniqueidtype, tx.uniqueid)]

    # Works with Flex currency attribute
    currency = tx.currency

    if isinstance(currency, ofxtools.models.CURRENCY):
        currency = currency.cursym
    elif currency is None:
        currency = default_currency

    assert isinstance(currency, str)

    transaction = merge_transaction(
        session,
        type=models.TransactionType.TRADE,
        fiaccount=account,
        uniqueid=tx.fitid,
        datetime=tx.dttrade,
        memo=memo or tx.memo,
        security=security,
        units=tx.units,
        currency=models.Currency[currency],
        cash=tx.total,
        sort=sortForTrade(tx),
    )
    #  self.transactions.append(transaction)
    return transaction


def merge_transaction(
    session: sqlalchemy.orm.session.Session,
    **kwargs,
) -> models.Transaction:
    """
    Persist a transaction to the database, using merge() logic
    i.e. insert if it doesn't already exist.
    """
    kwargs["uniqueid"] = kwargs["uniqueid"] or make_uid(**kwargs)
    return models.Transaction.merge(session, **kwargs)


def make_uid(
    type: models.TransactionType,
    datetime: datetime,
    fiaccount: models.FiAccount,
    security: models.SecurityId,
    units: Optional[Decimal] = None,
    currency: Optional[str] = None,
    cash: Optional[Decimal] = None,
    fromfiaccount: Optional[models.FiAccount] = None,
    fromsecurity: Optional[models.Security] = None,
    fromunits: Optional[Decimal] = None,
    numerator: Optional[Decimal] = None,
    denominator: Optional[Decimal] = None,
    **kwargs
):
    """
    We require of a Transaction.uniqueid that it be *unique* (duh) and
    *deterministic*.  Transaction.merge() uses uniqueid as a signature,
    so the same inputs must always produce the same output.

    The requirement for determinism means that uniqueids aren't going to
    sort in the order they're created, or the order they appear in the
    data stream; we can't make uniqueids increase monotonically.

    This is too bad, because the inventory module uses Transaction.uniqueid
    as a sort key (after datetime).  However, it's not as bad as all that;
    sort order really only matters for trades with identical datetimes,
    and OFX or Flex XML data always has FI-generated unique transaction IDs
    so all that's needed is to jigger the inputs for CSV trade data so
    it sorts in the desired order.
    """
    dateTime = datetime.isoformat()
    msg = (
        f"{dateTime} {type}, fiaccount={fiaccount.id}, security={security.id}, "
        f"units={units}, currency={currency}, cash={cash}, "
        f"fromfiaccount={getattr(fromfiaccount, 'id', None)}, "
        f"fromsecurity={getattr(fromsecurity, 'id', None)}, fromunits={fromunits}, "
        f"numerator={numerator}, denominator={denominator}"
    )
    uid = hashlib.sha256(msg.encode("utf-8")).hexdigest()
    return uid


##############################################################################
# CLI SCRIPT
###############################################################################
def main():
    from argparse import ArgumentParser
    from capgains.ofx import read

    argparser = ArgumentParser(description="Parse OFX data")
    argparser.add_argument("file", nargs="+", help="OFX file(s)")
    argparser.add_argument(
        "--database", "-d", default="sqlite://", help="Database connection"
    )
    argparser.add_argument("--verbose", "-v", action="count", default=0)
    args = argparser.parse_args()

    logLevel = (3 - min(args.verbose, 2)) * 10
    logging.basicConfig(level=logLevel)
    logging.captureWarnings(True)

    engine = sqlalchemy.create_engine(args.database)
    Base.metadata.create_all(bind=engine)

    for file in args.file:
        print(file)
        with sessionmanager(bind=engine) as session:
            transactions = read(session, file)
            session.add_all(transactions)

    engine.dispose()


if __name__ == "__main__":
    main()
