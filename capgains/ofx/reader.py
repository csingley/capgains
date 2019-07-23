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
from capgains import flex, models, utils
from capgains.containers import GroupedList, ListFunction
from capgains.database import Base, sessionmanager

if TYPE_CHECKING:
    import CSV
    Statement = Union[
        ofxtools.models.INVSTMTRS,
        flex.Types.FlexStatement,
        CSV.etfc.CsvStatement,
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

    Transfer = Union[ofxtools.models.TRANSFER, flex.Types.Transfer]

else:
    Trade = None
    Statement = None
    Transaction = None
    CashTransaction = None
    Transfer = None


SecuritiesMap = MutableMapping[Tuple[str, str], models.Security]
"""Map of (uniqueidtype, uniqueid) to ORM Security instance.
"""


class OfxStatementReader(object):
    """Processor for ofxtools.models.INVSTMTRS instance.

    Transaction processing breaks down into 3 stages - 'read', 'do', & 'merge'.

    The 'read' stage relies on the ofxtools.INVSTMTRS structure to gather data.
    Of the read functions, the most important is read_transactions(), which uses
    TRANSACTION_HANDLERS to group transactions by type and dispatch them to
    appropriate 'do' handler functions - doTrades(), doCashTransactions(), etc.

    The 'do' handlers use containers.GroupedList to define a standard
    functional processing pipeline.  Cf. containers module for the interface.
    Generally the pipelines filter out noise, then net remaining transactions
    for reversals, cancellations, etc.

    The 'merge' stage consists of wrappers around models.Transaction.merge(),
    using standardized mappings of processed OFX transactions to the
    models.Transaction data model to persist them to the database.

                                     +---------+
                                     |  read   |
                                     +-+-+-+-+-+
                                       | | | |
         +-----------------------------+ | | +-------------------+
         |                +--------------+ |                     |
         |                |                +--+                  |
         |                |                   |                  |
         v                v                   v                  |
    +---------+  +------------------+  +------------+            |
    |   read  |  |       read       |  |    read    |            |
    | account |  | default currency |  | securities |            |
    +----+----+  +--------+---------+  +-----+------+            v
         |                |                  |           +--------------+
         |                |                  +---------->|     read     |
         |                +----------------------------->| transactions |
         +----------------+----------------------------->|              |
                                                         +-------+------+
                                                                 |
                                                                 |
        +----------------+-----------------+-----------------+---+------------+
        |                |                 |                 |                |
        v                v                 v                 v                v
    +-------+  +-------------------+  +----------+  +------------------+  +-------+
    |  do   |  |        do         |  |    do    |  |       do         |  |  do   |
    | trade |  | return of capital |  | transfer |  | options exercise |  | reorg |
    +---+---+  +---------+---------+  +----+-----+  +--------+---------+  +---+---+
        |                |                 |                 |                |
        v                v                 |                 |                |
    +-------+  +-------------------+       |                 |                |
    | merge |  |      merge        |       |                 |                |
    | trade |  | return of capital |       |                 |                |
    +---+---+  +---------+---------+       |                 |                |
        |                |                 |                 |                |
        +-----------+    |                 |                 |                |
                    |    |                 |                 |                |
                    v    v                 v                 v                v
               +-------------+        +----------------------------------------------+
               |    merge    |        | NOT HANDLED IN OfxStatementReader BASE CLASS |
               | transaction |        |          overriden in subclasses             |
               +-------------+        +----------------------------------------------+

    Other modules within this subpackage define subclasses that override some
    of the instance methods to modify OFX processing for the quirks of
    different brokers.  Other subpackages define subclasses with more
    extensive modificatations that process data in other formats (e.g.
    CSV or Interactive Brokers Flex XML) by mapping it to mimic the ofxtools
    data model.  `ibflex.parser` and `CSV.etfc.parse()` perform this mapping.
    """

    def __init__(
        self,
        #  session: sqlalchemy.orm.session.Session.
        statement: Optional[Statement] = None,
        seclist: Optional[ofxtools.models.SECLIST] = None,
    ) -> None:
        # Store instance construction args.
        self.statement = statement
        self.seclist = seclist

        #  Initialize reading results collections.
        self.securities: SecuritiesMap = {}
        self.transactions: List[models.Transaction] = []

    def read(
        self,
        session: sqlalchemy.orm.session.Session,
        doTransactions: bool = True,
    ) -> List[models.Transaction]:
        self.session = session
        assert self.statement is not None

        # Set up the rest of the instance attributes needed globally.
        self.default_currency = self.read_default_currency(self.statement)
        self.account = self.read_account(self.statement, self.session)
        self.securities = self.read_securities(self.session)

        if doTransactions:
            transactions = self.read_transactions(
                self.statement,
                session=session,
                securities=self.securities,
                account=self.account,
                default_currency=self.default_currency,
            )
            self.transactions.extend(transactions)

        return self.transactions

    @staticmethod
    def read_default_currency(statement: Statement) -> str:
        assert isinstance(statement, ofxtools.models.INVSTMTRS)
        return statement.curdef

    @staticmethod
    def read_account(
        statement: Statement,
        session: sqlalchemy.orm.session.Session,
    ) -> models.FiAccount:
        assert isinstance(
            statement,
            (ofxtools.models.INVSTMTRS, flex.Types.FlexStatement)
        )
        account = statement.account
        return models.FiAccount.merge(
            session, brokerid=account.brokerid, number=account.acctid
        )

    def read_securities(
        self,
        session: sqlalchemy.orm.session.Session,
    ) -> SecuritiesMap:
        securities: SecuritiesMap = {}

        assert self.seclist is not None
        for sec in self.seclist:
            uniqueidtype = sec.uniqueidtype
            uniqueid = sec.uniqueid
            secname = sec.secname
            ticker = sec.ticker
            sec = models.Security.merge(
                session,
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
                        session,
                        uniqueidtype=uniqueidtype,
                        uniqueid=uniqueid,
                        name=secname,
                        ticker=ticker,
                    )
                    securities[(uniqueidtype, uniqueid)] = sec
                except ValueError:
                    pass

        return securities

    def read_transactions(
        self,
        statement: Statement,
        session: sqlalchemy.orm.session.Session,
        securities: SecuritiesMap,
        account: models.FiAccount,
        default_currency: str,
    ) -> List[models.Transaction]:
        """
        Group parsed statement transaction instances and dispatch groups to
        relevant handler functions
        """
        assert statement is not None

        transactions: List[models.Transaction] = []

        statement.transactions.sort(key=self.name_handler_for_tx)
        for handler_name, transactions_ in itertools.groupby(
            statement.transactions, key=self.name_handler_for_tx
        ):
            txs = getattr(self, handler_name)(
                transactions_,
                session=session,
                securities=securities,
                account=account,
                default_currency=default_currency,
            ) if handler_name else []
            transactions.extend(txs)

        return transactions

    def name_handler_for_tx(self, transaction: Transaction) -> str:
        """ Overridden by CsvTransactionReader. """
        return self.TRANSACTION_HANDLERS.get(type(transaction).__name__, "")

    TRANSACTION_HANDLERS = {
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
    def doTrades(
        self,
        transactions: Iterable[Trade],
        session: sqlalchemy.orm.session.Session,
        securities: SecuritiesMap,
        account: models.FiAccount,
        default_currency: str,
    ) -> List[models.Transaction]:
        """Preprocess trade transactions and send to merge_trade().

        The logic here eliminates unwanted trades (e.g. FX) and groups trades
        to net out canceled trades.
        """
        apply_cancels = make_canceller(
            filterfunc=self.is_trade_cancel,
            matchfunc=self.are_trade_cancel_pair,
            sortfunc=self.sort_trades_to_cancel,
        )

        _merge_trade = functools.partial(
            merge_trade,
            session=session,
            securities=securities,
            account=account,
            default_currency=default_currency,
            get_trade_sort_algo=self.get_trade_sort_algo,
        )

        transactions = (
            GroupedList(transactions)
            .filter(self.is_security_trade)
            .groupby(self.fingerprint_trade)
            .bind(apply_cancels)
            .filter(operator.attrgetter("units"))  # Removes net 0 unit transactions
            .map(_merge_trade)
            .flatten()
        )[:]
        return transactions

    @staticmethod
    def is_security_trade(transaction: Trade) -> bool:
        """Should this trade be processed?  Implement in subclass.
        """
        return True

    @staticmethod
    def fingerprint_trade(transaction: Trade) -> Any:
        """
        Transactions are grouped if they have the same security/datetime
        and matching units.  abs(units) is used so that trade cancellations
        are grouped together with the trades they cancel.

        Overridden by CsvTransactionReader.
        """
        return (
            transaction.uniqueidtype,
            transaction.uniqueid,
            transaction.dttrade,
            abs(transaction.units),
        )

    @staticmethod
    def is_trade_cancel(transaction: Trade) -> bool:
        """Is this trade actually a trade cancellation?  Implement in subclass.
        """
        return False

    @staticmethod
    def are_trade_cancel_pair(transaction0: Trade, transaction1: Trade) -> bool:
        """Does one of these trades cancel the other?
        """
        return transaction0.units == -1 * transaction1.units

    @staticmethod
    def sort_trades_to_cancel(transaction: Trade) -> Any:
        """Determines order in which trades are canceled.
        """
        return transaction.fitid

    @staticmethod
    def get_trade_sort_algo(
        transaction: Transaction
    ) -> Optional[models.TransactionSort]:
        """What models.TransactionSort algorithm applies to this transaction?

        Passed to merge_transaction().  This is unused in OFX; it's provided
        as an instance method so that FlexResponseReader can override it.
        """
        return None

    ###########################################################################
    # CASH TRANSACTIONS
    ###########################################################################
    def doCashTransactions(
        self,
        transactions: List[CashTransaction],
        session: sqlalchemy.orm.session.Session,
        securities: SecuritiesMap,
        account: models.FiAccount,
        default_currency: str,
    ) -> List[models.Transaction]:
        """
        Preprocess cash transactions and send to merge_retofcap().

        The logic here filters only for return of capital transactions;
        groups them to apply reversals; nets cash transactions remaining in
        each group; and persists to the database after applying any final
        preprocessing applied by cash_premerge_hook().

        It's important to net cash transactions remaining in a group that
        aren't cancelled, since the cash totals of reversing transactions
        often don't match the totals of the transactions being reversed
        (e.g. partial reversals to recharacterize to/from payment in lieu).
        """
        apply_cancels = make_canceller(
            filterfunc=self.is_cash_cancel,
            matchfunc=lambda x, y: x.total == -1 * y.total,
            sortfunc=self.sort_cash_for_cancel,
        )

        _merge_retofcap = functools.partial(
            merge_retofcap,
            session=session,
            securities=securities,
            account=account,
            default_currency=default_currency,
        )

        transactions_ = (
            GroupedList(transactions)
            .filter(self.is_retofcap)
            .groupby(self.fingerprint_cash)
            .bind(apply_cancels)
            .reduce(net_cash)
            .filter(operator.attrgetter("total"))  # Removes net $0 transactions
            .map(self.cash_premerge_hook)
            .map(_merge_retofcap)
            .flatten()
        )[:]
        return transactions_

    @staticmethod
    def is_retofcap(transaction: CashTransaction) -> bool:
        """Implement in subclass.
        """
        return False

    @staticmethod
    def fingerprint_cash(transaction: CashTransaction) -> Any:
        """
        Cash transactions are grouped together for cancellation/netting
        if they're for the same security at the same time with the same memo.
        """
        security = (transaction.uniqueidtype, transaction.uniqueid)
        memo = transaction.memo
        return transaction.dttrade, security, memo

    @staticmethod
    def is_cash_cancel(transaction: CashTransaction) -> bool:
        """Is this cash transaction actually a reversal?

        Implement in subclass.
        """
        return False

    @staticmethod
    def sort_cash_for_cancel(transaction: CashTransaction) -> Any:
        """Determines order in which cash transactions are reversed.

        Implement in subclass.
        """
        return False

    def cash_premerge_hook(self, transaction: CashTransaction) -> CashTransaction:
        """Any last preprocessing before transaction is passed to the DB merge layer.

        Implement in subclass.
        """
        return transaction

    ###########################################################################
    # ACCOUNT TRANSFERS
    ###########################################################################
    def doTransfers(
        self,
        transactions: Iterable[Transfer],
        session: sqlalchemy.orm.session.Session,
        securities: SecuritiesMap,
        account: models.FiAccount,
        default_currency: str,
    ) -> List[models.Transaction]:
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

        return []


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

    If `memo` arg is passed it, it overrides the transaction.memo
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


def merge_trade(
    tx: Union[Trade, flex.Types.CorporateAction],  # cf. flex.reader.treat_as_trade()
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    account: models.FiAccount,
    default_currency: str,
    get_trade_sort_algo: Callable[[Transaction], Any],
    memo: Optional[str] = None
) -> models.Transaction:
    """Process a trade into data fields to hand off to merge_transaction()
    to persist in the database.
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
        sort=get_trade_sort_algo(tx),
    )
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
            if canceled is False:  # first_true() default
                raise ValueError(
                    f"Can't find Transaction canceled by {cancel}"
                    f"\n in {originals}"
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

    with sessionmanager(bind=engine) as session:
        for file in args.file:
            print(file)
            transactions = read(session, file)
            session.add_all(transactions)
            for transaction in transactions:
                print(transaction)

    engine.dispose()


if __name__ == "__main__":
    main()
