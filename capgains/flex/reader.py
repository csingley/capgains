# coding: utf-8
"""
Importer for Interactive Brokers Flex XML format
"""
from __future__ import annotations

from operator import attrgetter
import datetime as datetime_
from decimal import Decimal
import functools
import warnings
import logging
from copy import copy
from typing import (
    Any,
    Union,
    Optional,
    NamedTuple,
    Tuple,
    List,
    MutableMapping,
    Iterable,
    Callable,
    Match,
    cast,
)

import sqlalchemy
from ofxtools.utils import validate_cusip, cusip2isin, validate_isin
import ibflex

from capgains import ofx, models, utils
from capgains.ofx.reader import SecuritiesMap, Statement, Trade
from capgains.flex import BROKERID, Types, regexes
from capgains.database import Base, sessionmanager
from capgains.containers import GroupedList


class ParsedCorpAct(NamedTuple):
    """Data parsed from Types.CorporateAction by parseCorporateActionMemo().

    Attrs:
        raw: original unparsed CorporateAction
        type: ibflex.enums.Reorg instance from CorporateAction
        ticker: parsed from CorporateAction.memo
        cusip: parsed from CorporateAction.memo
        secname: parsed from CorporateAction.memo
        memo: stripped CorporateAction.memo prefix (the part before parentheses)
    """
    raw: Types.CorporateAction
    type: ibflex.enums.Reorg
    ticker: str
    cusip: str
    secname: str
    memo: str


class CostBasisSuspense(NamedTuple):
    """Parked cost basis information to pass from Reorg.TENDER to Reorg.MERGER
    in a rights offering.

    cf. tender(), merge_reorg()
    """
    currency: str
    cash: Decimal
    datetime: datetime_.datetime


DividendsPaid = MutableMapping[
    Tuple[str, datetime_.date],
    ibflex.Types.ChangeInDividendAccrual
]


BasisSuspense = MutableMapping[Tuple[str, str], CostBasisSuspense]
"""Map of (uniqueidtype, uniqueid) to CostBasisSuspense instance.
"""

CorpActHandlerReturn = Tuple[List[models.Transaction], BasisSuspense]
"""Standard type signature of CorporateAction handler return values.
"""


class FlexResponseReader:
    """Processor for sequences of capgains.flex.parser.FlexStatement instances
    as created by parser.FlexResponseParser.parse().
    """

    def __init__(
        self,
        session: sqlalchemy.orm.session.Session,
        response: Iterable[Types.FlexStatement],
    ):
        """
        """
        self.session = session
        self.statements = [FlexStatementReader(session, stmt) for stmt in response]

    def read(self) -> None:
        for stmt in self.statements:
            stmt.read()


class FlexStatementReader(ofx.reader.OfxStatementReader):
    """Processor for capgains.flex.parser.FlexStatement instances.
    """

    def __init__(
        self,
        session: sqlalchemy.orm.session.Session,
        statement: Optional[Types.FlexStatement] = None,
     ):
        self.session = session
        self.statement = statement
        self.securities: SecuritiesMap = {}
        self.transactions: List[models.Transaction] = []
        self.dividendsPaid: DividendsPaid = {}

    def read(self, doTransactions: bool = True) -> List[models.Transaction]:
        """Extend OfxStatementReader superclass method - also process unparsed
        ibflex.Types.ChangeInDividendAccruals and ibflex.Types.ConversionRates.
        """
        stmt = self.statement
        assert stmt is not None
        self.dividendsPaid = self.read_change_in_dividend_accruals(stmt)
        self.read_currency_rates(stmt, self.session)
        return super(FlexStatementReader, self).read(doTransactions)

    @staticmethod
    def read_default_currency(statement: Statement) -> str:
        """Override OfxStatementReader.read_currency_default()
        """
        assert isinstance(statement, Types.FlexStatement)
        return statement.account.currency

    @staticmethod
    def read_change_in_dividend_accruals(statement: Statement) -> DividendsPaid:
        """Create a dict of ibflex.Types.ChangeInDividendAccrual instances
        keyed by (CONID, payDate).

        Used to fix the accrual dates of CashTransactions.
        """
        assert isinstance(statement, Types.FlexStatement)
        return {
            (div.conid, div.payDate): div
            for div in statement.changeInDividendAccruals
            if ibflex.enums.Code.REVERSE in div.code
            and div.conid is not None
            and div.payDate is not None
        }

    @staticmethod
    def read_currency_rates(
        statement: Statement,
        session: sqlalchemy.orm.session.Session,
    ) -> None:
        """Persist currency conversion rates to DB.

        Used in reporting, to translate inventory.Gain instances to
        functional currency.
        """
        assert isinstance(statement, Types.FlexStatement)
        rates = statement.conversionRates
        for rate in rates:
            models.CurrencyRate.merge(
                session,
                date=rate.reportDate,
                fromcurrency=rate.fromCurrency,
                tocurrency=rate.toCurrency,
                rate=rate.rate,
            )

    def read_securities(
        self,
        session: sqlalchemy.orm.session.Session,
    ) -> SecuritiesMap:
        securities: SecuritiesMap = {}
        assert isinstance(self.statement, Types.FlexStatement)
        securities = {
            (secinfo.uniqueidtype, secinfo.uniqueid): models.Security.merge(
                session, uniqueidtype=secinfo.uniqueidtype,
                uniqueid=secinfo.uniqueid,
                name=secinfo.secname,
                ticker=secinfo.ticker,
            )
            for secinfo in self.statement.securities
        }
        #  for secinfo in self.statement.securities:
            #  sec = models.Security.merge(
                #  session,
                #  uniqueidtype=secinfo.uniqueidtype,
                #  uniqueid=secinfo.uniqueid,
                #  name=secinfo.secname,
                #  ticker=secinfo.ticker,
            #  )
            #  securities[(secinfo.uniqueidtype, secinfo.uniqueid)] = sec

        return securities

    ###########################################################################
    # TRADES
    #
    # These methods override OfxStatementReader superclass methods.
    # They are used by OfxStatementReader.doTrades(), which provides
    # their context.
    ###########################################################################
    @staticmethod
    def filterTrades(transaction: ofx.reader.Trade) -> bool:
        """Should this trade be processed?  Discard FX trades.

        Overrides OfxStatementReader superclass method.
        """
        # FIXME - better way to skip currency trades
        currencyPairs = ["USD.CAD", "CAD.USD", "USD.EUR", "EUR.USD"]
        # conid for currency pairss
        # '15016062'  # USD.CAD
        # '15016251'  # CAD.USD
        # '12087792'  # EUR.USD
        return transaction.memo not in currencyPairs

    @staticmethod
    def is_trade_cancel(transaction: ofx.reader.Trade) -> bool:
        """Is this trade actually a trade cancellation?  Consult trade notes.

        Overrides OfxStatementReader superclass method.
        """
        return ibflex.enums.Code.CANCEL in transaction.notes

    @staticmethod
    def matchTradeWithCancel(
        canceler: ofx.reader.Trade,
        canceled: ofx.reader.Trade,
    ) -> bool:
        """Does one of these trades cancel the other?

        Overrides OfxStatementReader superclass method.
        """
        match = False

        if canceler.orig_tradeid not in (None, "", "0"):
            match = canceler.orig_tradeid == canceled.fitid
        else:
            match = (
                (canceler.units == -1 * canceled.units)
            )

        return match

    @staticmethod
    def sortCanceledTrades(transaction: ofx.reader.Trade) -> Any:
        """Determines order in which trades are canceled - order by transaction
        report date (i.e. the date the trade was reported, as opposed to when
        it was executed).

        Overrides OfxStatementReader superclass method.
        """
        assert transaction.reportdate is not None
        return transaction.reportdate

    @staticmethod
    def sortForTrade(
        transaction: ofx.reader.Transaction
    ) -> Optional[models.TransactionSort]:
        return sortForTrade(transaction)

    ###########################################################################
    # CASH TRANSACTIONS
    #
    # These methods override OfxStatementReader superclass methods.
    # They are used by OfxStatementReader.doCashTransactions(), which provides
    # their context.
    ###########################################################################
    @staticmethod
    def is_retofcap(transaction: ofx.reader.CashTransaction) -> bool:
        memo = transaction.memo.lower()
        return "return of capital" in memo or "interimliquidation" in memo

    @classmethod
    def groupCashTransactionsForCancel(
        cls,
        transaction: ofx.reader.CashTransaction
    ) -> Any:
        """Cash transactions are grouped together for cancellation/netting
        if they're for the same security at the same time with the same memo.
        """
        security = (transaction.uniqueidtype, transaction.uniqueid)
        memo = cls.stripCashTransactionMemo(transaction.memo)
        return transaction.dtsettle, security, memo

    @staticmethod
    def stripCashTransactionMemo(memo: str) -> str:
        """Strip "REVERSAL"/"CANCEL" from transaction description so reversals
        sort together with reversees.
        """
        memo = memo.replace(" - REVERSAL", "")
        memo = memo.replace("CANCEL ", "")
        return memo

    @staticmethod
    def filterCashTransactionCancels(transaction: ofx.reader.CashTransaction) -> bool:
        """Is this cash transaction actually a reversal?

        Overrides OfxStatementReader superclass method.
        """
        memo = transaction.memo
        return "REVERSAL" in memo or "CANCEL" in memo

    @staticmethod
    def sortCanceledCashTransactions(transaction: ofx.reader.CashTransaction) -> Any:
        """Determines order in which cash transactions are reversed.

        Overrides OfxStatementReader superclass method.
        """
        return transaction.fitid

    def fixCashTransaction(
        self,
        transaction: ofx.reader.CashTransaction
    ) -> ofx.reader.CashTransaction:
        """If we can find a matching record in ChangeInDividendAccruals,
        use the indicated ex-date for the CashTransaction.

        Overrides OfxStatementReader superclass method.
        """
        uniqueid = transaction.uniqueid
        assert isinstance(uniqueid, str)
        payDt = transaction.dtsettle
        try:
            assert isinstance(payDt, datetime_.datetime)
        except AssertionError:
            print(type(payDt), payDt)
            raise
        #  N.B. transaction.dtsettle is a datetime.datetime, but
        #  self.dividendsPaid is keyed by (uniqueid, date).
        #  Transform datetime to date for lookup.
        chg_divaccrual = self.dividendsPaid.get((uniqueid, payDt.date()), None)
        if chg_divaccrual:
            exdate = chg_divaccrual.exDate
            assert isinstance(exdate, datetime_.date)
            transaction = transaction._replace(
                dttrade=datetime_.datetime(
                    year=exdate.year,
                    month=exdate.month,
                    day=exdate.day,
                )
            )
        else:
            transaction = transaction._replace(dttrade=payDt)

        return transaction

    def doTransfers(
        self,
        transactions: Iterable[ofx.reader.Transfer],
        session: sqlalchemy.orm.session.Session,
        securities: SecuritiesMap,
        account: models.FiAccount,
        default_currency: str,
    ) -> List[models.Transaction]:
        """Only handle securities transfers"""
        _merge_acct_transfer = functools.partial(
            merge_account_transfer,
            session=session,
            securities=securities,
            account=account,
        )
        transactions = (
            GroupedList(transactions)
            .filter(attrgetter("uniqueid"))
            .map(_merge_acct_transfer)
        )[:]
        return [tx for tx in transactions if tx is not None]

    def doCorporateActions(
        self,
        transactions: Iterable[Types.CorporateAction],
        session: sqlalchemy.orm.session.Session,
        securities: SecuritiesMap,
        account: models.FiAccount,
        default_currency: str,
    ) -> List[models.Transaction]:
        """
        Group corporate actions by datetime/type/memo; net by security;
        dispatch each group to type-specific handler for further processing.
        """
        CORPACT_HANDLERS = {
            "BONDMATURITY": treat_as_trade,
            "SUBSCRIBERIGHTS": subscribe_rights,
            "ISSUECHANGE": change_security,
            "ASSETPURCHASE": treat_as_trade,
            "RIGHTSISSUE": issue_rights,
            "STOCKDIV": stock_dividend,
            "TENDER": tender,
            "DELISTWORTHLESS": treat_as_trade,
            "FORWARDSPLIT": split,
            "REVERSESPLIT": split,
            "MERGER": merger,
            "SPINOFF": spinoff,
        }

        group = FlexStatementReader.preprocessCorporateActions(
            session,
            securities,
            transactions,
        )
        #  group is now a GroupedList in `grouped` state, containing
        #  GroupedLists of ParsedCorporateAction instances
        assert group.grouped is True

        transactions_: List[models.Transaction] = []

        #  Initialize "free parking" state variable to pass partial cost basis
        #  information between different legs of reorgs.
        basis_suspense: BasisSuspense = {}

        for parsedCorpActs in group:
            assert parsedCorpActs.grouped is False
            _, type_, memo = parsedCorpActs.key
            assert isinstance(type_, str)
            assert isinstance(memo, str)

            handler = CORPACT_HANDLERS.get(type_, None)
            if not handler:
                msg = (
                    "flex.reader.CORPACT_HANDLERS doesn't know how to handle "
                    f"type='{type_}' for corporate actions {parsedCorpActs}"
                )
                raise ValueError(msg)
            txs, basis_suspense = handler(
                parsedCorpActs,
                memo,
                session,
                securities,
                account,
                default_currency,
                basis_suspense,
            )

            transactions_.extend(txs)

        return transactions_

    @staticmethod
    def preprocessCorporateActions(
        session: sqlalchemy.orm.session.Session,
        securities: SecuritiesMap,
        transactions: Iterable[Types.CorporateAction]
    ) -> GroupedList:
        """Factored out from doCorporateActions() for testing purposes.
        """
        apply_cancels = ofx.reader.make_canceller(
            filterfunc=filterCorporateActionCancels,
            matchfunc=matchCorporateActionWithCancel,
            sortfunc=lambda ca: ca.reportdate,
        )

        parse_memo = functools.partial(
            parseCorporateActionMemo,
            session,
            securities,
        )

        # See containers.GroupedList for details of this interface.
        group = (
            GroupedList(transactions)
            .groupby(group_corpacts)
            .bind(apply_cancels)
            .reduce(net_corpacts)
            .flatten()
            .map(parse_memo)  # Transform contents from CorporateAction to ParsedCorpAct
            .groupby(group_parsed_corpacts)
            .sort(sort_parsed_corpacts)
        )

        return group

    def doOptionsExercises(
        self,
        transactions: Iterable[Types.Exercise],
        session: sqlalchemy.orm.session.Session,
        securities: SecuritiesMap,
        account: models.FiAccount,
        default_currency: str,
    ) -> List[models.Transaction]:

        def doOptionsExercise(transaction):
            security = securities[
                (transaction.uniqueidtype, transaction.uniqueid)
            ]
            fromsecurity = securities[
                (transaction.uniqueidtypeFrom, transaction.uniqueidFrom)
            ]
            return ofx.reader.merge_transaction(
                session,
                uniqueid=transaction.fitid,
                datetime=transaction.dttrade,
                type=models.TransactionType.EXERCISE,
                memo=transaction.memo,
                currency=models.Currency[transaction.currency],
                cash=transaction.total,
                fiaccount=account,
                security=security,
                units=transaction.units,
                fromsecurity=fromsecurity,
                fromunits=transaction.unitsfrom,
                sort=sortForTrade(transaction),
            )

        return [doOptionsExercise(transaction) for transaction in transactions]

    TRANSACTION_HANDLERS = {
        "Trade": "doTrades",
        "CashTransaction": "doCashTransactions",
        "Transfer": "doTransfers",
        "CorporateAction": "doCorporateActions",
        "Exercise": "doOptionsExercises",
    }


########################################################################################
#  Helper functions
########################################################################################
def group_corpacts(
    corpAct: Types.CorporateAction
) -> Any:
    """Same security/date/type/memo -> same reorg in same security.

    These CorporateActions will be cancelled/netted against each other.
    """
    return (
        (corpAct.uniqueidtype, corpAct.uniqueid),
        corpAct.dttrade,
        corpAct.type.name,
        corpAct.memo,
    )

    return corpAct.raw.dttrade, corpAct.type.name, corpAct.memo


def filterCorporateActionCancels(
    transaction: Types.CorporateAction
) -> bool:
    return ibflex.enums.Code.CANCEL in transaction.code


def matchCorporateActionWithCancel(
    transaction0: Types.CorporateAction,
    transaction1: Types.CorporateAction,
):
    return transaction0.units == -1 * transaction1.units


def sortCanceledCorporateActions(corpAct: Types.CorporateAction) -> Any:
    return corpAct.reportdate


def net_corpacts(
    corpAct0: Types.CorporateAction,
    corpAct1: Types.CorporateAction,
) -> Types.CorporateAction:
    assert corpAct0.currency == corpAct1.currency
    return corpAct0._replace(
        units=corpAct0.units + corpAct1.units,
        total=corpAct0.total + corpAct1.total,
    )


def parseCorporateActionMemo(
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    transaction: Types.CorporateAction
) -> "ParsedCorpAct":
    """Parse memo; pack results in ParsedCorpAct tuple.

    Side effect:
        Adds keys to input `securities` dict for each securities identifier
        found in the memo.
    """
    memo = transaction.memo

    match = regexes.corpActRE.match(memo)
    if match is None:
        raise ValueError(
            f"On {transaction.dttrade}, can't parse corporate action '{memo}'"
        )

    # Try to extract SecurityId data from CorporateAction memo
    matchgroups = match.groupdict()
    ticker = matchgroups["ticker"]
    secname = matchgroups["secname"]
    uniqueid = matchgroups["cusip"]
    if validate_cusip(uniqueid):
        uniqueidtype: Optional[str] = "CUSIP"
        # Also do ISIN; why not?
        isin = cusip2isin(uniqueid)
        sec = models.Security.merge(
            session,
            uniqueidtype="ISIN",
            uniqueid=isin,
            name=secname,
            ticker=ticker,
        )
        securities[("ISIN", isin)] = sec
    elif validate_isin(uniqueid):
        uniqueidtype = "ISIN"
    else:
        uniqueidtype = None
    if uniqueidtype:
        sec = models.Security.merge(
            session,
            uniqueidtype=uniqueidtype,
            uniqueid=uniqueid,
            name=secname,
            ticker=ticker,
        )
        securities[(uniqueidtype, uniqueid)] = sec

    pca = ParsedCorpAct(raw=transaction, type=transaction.type, **matchgroups)
    return pca


def group_parsed_corpacts(parsedCorpAct: "ParsedCorpAct") -> Any:
    """Same date/type/memo -> same reorg.

    Differs from group_corpacts() by potentially including multiple securities
    as part of the same reorg.
    """
    return parsedCorpAct.raw.dttrade, parsedCorpAct.type.name, parsedCorpAct.memo


def sort_parsed_corpacts(parsedCorpAct: "ParsedCorpAct") -> Any:
    reportdate = parsedCorpAct.raw.reportdate
    assert reportdate is not None
    return reportdate


########################################################################################
#  Corporate Action Handlers
########################################################################################
def treat_as_trade(  # BONDMATURITY(BM),  DELISTWORTHLESS(DW), ASSETPURCHASE(OR)
    parsedCorpActs: List["ParsedCorpAct"],
    memo: str,
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    account: models.FiAccount,
    default_currency: str,
    basis_suspense: BasisSuspense,
) -> CorpActHandlerReturn:
    """
    """
    transactions = [
        ofx.reader.merge_trade(
            corpAct.raw,
            session=session,
            securities=securities,
            account=account,
            default_currency=default_currency,
            sortForTrade=sortForTrade,
            memo=memo,
        )
        for corpAct in parsedCorpActs
    ]
    return transactions, basis_suspense


def subscribe_rights(  # SUBSCRIBERIGHTS (SR)
    parsedCorpActs: List["ParsedCorpAct"],
    memo: str,
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    account: models.FiAccount,
    default_currency: str,
    basis_suspense: BasisSuspense,
) -> CorpActHandlerReturn:
    regex = regexes.subscribeRE
    match = regex.match(memo)
    assert match
    _src, _dest, spinoffs = _group_reorg(parsedCorpActs, match)
    src, dest = _src.raw, _dest.raw
    security = securities[(dest.uniqueidtype, dest.uniqueid)]
    fromsecurity = securities[(src.uniqueidtype, src.uniqueid)]

    assert isinstance(src.currency, str)

    transaction = ofx.reader.merge_transaction(
        session,
        uniqueid=src.fitid,
        datetime=src.dttrade,
        type=models.TransactionType.EXERCISE,
        memo=memo,
        currency=models.Currency[src.currency],
        cash=src.total,
        fiaccount=account,
        security=security,
        units=dest.units,
        fromsecurity=fromsecurity,
        fromunits=src.units,
    )
    transactions = [transaction]

    transactions.extend(
        [
            merge_spinoff(
                session,
                securities,
                account,
                spinoff.raw,
                fromsecurity=fromsecurity,
                numerator=cast(Types.CorporateAction, spinoff).units,
                denominator=-src.units,
                memo=memo,
            )
            for spinoff in spinoffs
        ]
    )
    return transactions, basis_suspense


def change_security(  # ISSSUECHANGE (IC)
    parsedCorpActs: List["ParsedCorpAct"],
    memo: str,
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    account: models.FiAccount,
    default_currency: str,
    basis_suspense: BasisSuspense,
) -> CorpActHandlerReturn:
    #  Book as Transfer(same account, different securities).
    regex = regexes.changeSecurityRE
    match = regex.match(memo)
    assert match
    transactions = merge_reorg(
        session,
        securities,
        account,
        parsedCorpActs,
        match,
        memo,
        default_currency,
        basis_suspense,
    )
    return transactions, basis_suspense


def issue_rights(  # RIGHTSISSUE (RI)
    parsedCorpActs: List["ParsedCorpAct"],
    memo: str,
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    account: models.FiAccount,
    default_currency: str,
    basis_suspense: BasisSuspense,
) -> CorpActHandlerReturn:
    match = regexes.rightsIssueRE.match(memo)
    assert match
    matchgroups = match.groupdict()
    # FIXME - tickerFrom could also be ticker1 !
    transactions = [
        merge_spinoff(
            session,
            securities,
            account,
            corpAct.raw,
            fromsecurity=guess_security(
                session,
                securities,
                uniqueid=matchgroups["isinFrom"],
                ticker=matchgroups["tickerFrom"]
            ),
            numerator=Decimal(matchgroups["numerator0"]),
            denominator=Decimal(matchgroups["denominator0"]),
            memo=memo,
        )
        for corpAct in parsedCorpActs
    ]
    return transactions, basis_suspense


def stock_dividend(  # STOCKDIVIDEND (SD)
    parsedCorpActs: List["ParsedCorpAct"],
    memo: str,
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    account: models.FiAccount,
    default_currency: str,
    basis_suspense: BasisSuspense,
) -> CorpActHandlerReturn:
    match = regexes.stockDividendRE.match(memo)
    assert match

    numerator = Decimal(match.group("numerator0"))
    denominator = Decimal(match.group("denominator0"))
    numerator += denominator
    transactions = [
        merge_split(
            session,
            securities,
            account,
            corpAct.raw,
            numerator=numerator,
            denominator=denominator,
            memo=memo,
        )
        for corpAct in parsedCorpActs
    ]
    return transactions, basis_suspense


def tender(  # TENDER (TO)
    parsedCorpActs: List["ParsedCorpAct"],
    memo: str,
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    account: models.FiAccount,
    default_currency: str,
    basis_suspense: BasisSuspense,
) -> CorpActHandlerReturn:
    match = regexes.tenderRE.match(memo)
    assert match

    cashportions = [corpAct for corpAct in parsedCorpActs if corpAct.raw.total != 0]
    if cashportions:
        # HACK
        # IBKR processes some rights offerings in two parts -
        # 1) a sequence of type 'TO', booking out the old security,
        #    booking in the contra, and booking out subscription cash;
        # 2) a sequence of type 'TC', booking out the contra, booking in
        #    the new security, and booking in the subscribed-for security.
        # The 'TO' series contains the cash paid for the subscription, but
        # not the units, while the 'TC' series contains the units data but
        # not the cost.  Neither series, as grouped by
        # group_parsed_corpacts(), contains all the data.
        #
        # Here we extract the cost & open date, and stash it in a dict to
        # be picked up for later processing by merge_reorg().  We assume
        # that the 'TO' series has a dateTime strictly earlier than the
        # 'TC' series, so they'll be sorted in correct time order by
        # group_parsed_corpacts() and merge_reorg() will have its
        # values ready & waiting from tender().
        assert len(cashportions) == 1
        cashportion = cashportions.pop().raw
        cash = cashportion.total
        assert cash is not None
        assert cash < 0

        src, dest, spinoffs = _group_reorg(parsedCorpActs, match)
        basis_key = (dest.raw.uniqueidtype, dest.raw.uniqueid)

        assert basis_key not in basis_suspense
        basis_suspense[basis_key] = CostBasisSuspense(
            currency=cashportion.currency, cash=cash, datetime=cashportion.dttrade
        )

    transactions = merge_reorg(
        session,
        securities,
        account,
        parsedCorpActs,
        match,
        memo,
        default_currency,
        basis_suspense,
    )
    return transactions, basis_suspense


def split(  # FORWARDSPLIT (FS), REVERSESPLIT (RS)
    parsedCorpActs: List["ParsedCorpAct"],
    memo: str,
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    account: models.FiAccount,
    default_currency: str,
    basis_suspense: BasisSuspense,
) -> CorpActHandlerReturn:
    """ """
    match = regexes.splitRE.match(memo)
    assert match

    parsedCorpActs = list(parsedCorpActs)
    assert len(parsedCorpActs) in (1, 2)

    if len(parsedCorpActs) == 1:
        # Split without CUSIP change
        transaction = merge_split(
            session,
            securities,
            account,
            parsedCorpActs.pop().raw,
            numerator=Decimal(match.group("numerator0")),
            denominator=Decimal(match.group("denominator0")),
            memo=memo,
        )
        return [transaction], basis_suspense

    elif len(parsedCorpActs) == 2:
        # Split with CUSIP change - Book as Transfer
        #
        # Of the pair, the transaction booking in the new security has
        # XML attribute 'symbol' matching the first ticker in the memo.
        # The other transaction books out the old security.
        isinFrom = match.group("isinFrom")

        parsedCorpActs = sorted(
            list(parsedCorpActs),
            key=lambda x: x.cusip in isinFrom or isinFrom in x.cusip
        )
        assert parsedCorpActs[0].cusip not in isinFrom
        assert parsedCorpActs[-1].cusip in isinFrom
        dest, src = [corpAct.raw for corpAct in parsedCorpActs]
        transaction = merge_security_transfer(
            session,
            securities,
            account,
            src,
            dest,
            memo
        )
        return [transaction], basis_suspense

    raise ValueError  # FIXME


def merger(  # MERGER (TC)
    parsedCorpActs: List["ParsedCorpAct"],
    memo: str,
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    account: models.FiAccount,
    default_currency: str,
    basis_suspense: BasisSuspense,
) -> CorpActHandlerReturn:
    """ """

    def cashMerger(memo, parsedCorpActs):
        match = regexes.cashMergerRE.match(memo)
        if match:
            if len(parsedCorpActs) != 1:
                msg = "More than one CorporateAction " "in a cash merger: {}"
                raise ValueError(msg.format([c.raw for c in parsedCorpActs]))
            return [
                ofx.reader.merge_trade(
                    parsedCorpActs.pop().raw,
                    session=session,
                    securities=securities,
                    account=account,
                    default_currency=default_currency,
                    sortForTrade=sortForTrade,
                    memo=memo)
            ]

    def kindMerger(memo, parsedCorpActs):
        match = regexes.kindMergerRE.match(memo)
        if match:
            return merge_reorg(
                session,
                securities,
                account,
                parsedCorpActs,
                match,
                memo,
                default_currency,
                basis_suspense,
            )

    def cashKindMerger(memo, parsedCorpActs):
        match = regexes.cashAndKindMergerRE.match(memo)
        if match:
            # First process cash proceeds as a return of capital
            cashportions = [
                corpAct.raw for corpAct in parsedCorpActs if corpAct.raw.total != 0
            ]
            assert len(cashportions) == 1
            cashportion = cashportions.pop()
            assert cashportion.total > 0

            txs = [
                ofx.reader.merge_retofcap(
                    cashportion,
                    session,
                    securities,
                    account,
                    default_currency,
                    memo,
                )
            ]

            # Then process in-kind merger
            txs.extend(merge_reorg(
                session,
                securities,
                account,
                parsedCorpActs,
                match,
                memo,
                default_currency,
                basis_suspense,
            ))
            return txs

    transactions = (
        cashMerger(memo, parsedCorpActs)
        or kindMerger(memo, parsedCorpActs)
        or cashKindMerger(memo, parsedCorpActs)
    )
    if not transactions:
        msg = (
            "flex.reader.FlexStatementReader.merger(): "
            "Can't parse merger memo: '{}'"
        )
        raise ValueError(msg.format(memo))

    return transactions, basis_suspense


def spinoff(  # SPINOFF (SO)
    parsedCorpActs: List["ParsedCorpAct"],
    memo: str,
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    account: models.FiAccount,
    default_currency: str,
    basis_suspense: BasisSuspense,
) -> CorpActHandlerReturn:
    match = regexes.spinoffRE.match(memo)
    if match is None:
        msg = "Couldn't parse memo for spinoff '{}'".format(memo)
        raise ValueError(msg)
    matchgroups = match.groupdict()
    try:
        fromsecurity = guess_security(
            session,
            securities,
            uniqueid=matchgroups["isinFrom"],
            ticker=matchgroups["tickerFrom"]
        )
    except ValueError as e:
        msg = "For spinoff memo '{}': ".format(memo)
        msg += e.args[0]
        raise ValueError(msg)

    transactions = [
        merge_spinoff(
            session,
            securities,
            account,
            corpAct.raw,
            fromsecurity=fromsecurity,
            numerator=Decimal(matchgroups["numerator0"]),
            denominator=Decimal(matchgroups["denominator0"]),
            memo=memo,
        )
        for corpAct in parsedCorpActs
    ]
    return transactions, basis_suspense


########################################################################################
#  Merge functions
########################################################################################
def merge_account_transfer(
    transaction,
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    account: models.FiAccount,
) -> Optional[models.Transaction]:
    if transaction.type is ibflex.TransferType.INTERNAL:
        fromacct = models.FiAccount.merge(
            session, brokerid=BROKERID, number=transaction.other_acctid
        )
    elif transaction.type is ibflex.TransferType.ACATS:
        # IBKR strips out punctuation from other brokers' acct#;
        # alphanumeric only
        accts = [
            (a.id, "".join([c for c in a.number if c.isalnum()]))
            for a in session.query(models.FiAccount).all()
        ]

        acctTuple = utils.first_true(
            accts,
            pred=functools.partial(
                lambda at, num: at[1] == num, num=transaction.other_acctid
            ),
        )
        if not acctTuple:
            msg = "Can't find FiAccount.number={}; " "skipping external transfer {}"
            warnings.warn(msg.format(transaction.other_acctid, transaction.memo))
            return None
        fromacct = session.query(models.FiAccount).get(acctTuple[0])
    else:
        raise ValueError(
            f"{transaction.type} is not a valid {ibflex.TransferType} "
            f"in {transaction}"
        )

    acct = account
    units = transaction.units
    fromunits = -units
    direction = transaction.tferaction
    assert direction in ("IN", "OUT")
    if direction == "OUT":
        fromunits, units = units, fromunits
        fromacct, acct = acct, fromacct

    security = securities[(transaction.uniqueidtype, transaction.uniqueid)]
    transaction = ofx.reader.merge_transaction(
        session,
        type=models.TransactionType.TRANSFER,
        fiaccount=acct,
        uniqueid=transaction.fitid,
        datetime=transaction.dttrade,
        memo=transaction.memo,
        security=security,
        units=units,
        fromfiaccount=fromacct,
        fromsecurity=security,
        fromunits=fromunits,
    )
    return transaction


def merge_split(
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    account: models.FiAccount,
    tx: Types.CorporateAction,
    numerator: Decimal,
    denominator: Decimal,
    memo: str
) -> models.Transaction:
    assert tx.uniqueidtype
    assert tx.uniqueid
    security = securities[(tx.uniqueidtype, tx.uniqueid)]
    transaction = ofx.reader.merge_transaction(
        session,
        type=models.TransactionType.SPLIT,
        fiaccount=account,
        uniqueid=tx.fitid,
        datetime=tx.dttrade,
        memo=memo or tx.memo,
        security=security,
        numerator=numerator,
        denominator=denominator,
        units=tx.units,
    )
    return transaction


def merge_spinoff(
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    account: models.FiAccount,
    tx: Types.CorporateAction,
    fromsecurity: models.Security,
    numerator: Decimal,
    denominator: Decimal,
    memo: str,
) -> models.Transaction:
    assert tx.uniqueidtype
    assert tx.uniqueid
    security = securities[(tx.uniqueidtype, tx.uniqueid)]
    transaction = ofx.reader.merge_transaction(
        session,
        type=models.TransactionType.SPINOFF,
        fiaccount=account,
        uniqueid=tx.fitid,
        datetime=tx.dttrade,
        memo=memo or tx.memo,
        security=security,
        numerator=numerator,
        denominator=denominator,
        units=tx.units,
        fromsecurity=fromsecurity,
    )
    return transaction


def merge_security_transfer(
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    account: models.FiAccount,
    src: Types.CorporateAction,
    dest: Types.CorporateAction,
    memo: str,
) -> models.Transaction:
    """
    Given two transactions which have already been matched, treat them
    as a transformation from one security to another within the same
    fiaccount.
    """
    security = securities[(dest.uniqueidtype, dest.uniqueid)]
    fromsecurity = securities[(src.uniqueidtype, src.uniqueid)]
    transaction = ofx.reader.merge_transaction(
        session,
        type=models.TransactionType.TRANSFER,
        fiaccount=account,
        uniqueid=dest.fitid,
        datetime=dest.dttrade,
        memo=memo,
        security=security,
        units=dest.units,
        fromfiaccount=account,
        fromsecurity=fromsecurity,
        fromunits=src.units,
    )
    return transaction


def merge_reorg(
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    account: models.FiAccount,
    parsedCorpActs: List["ParsedCorpAct"],
    match: Match,
    memo: str,
    default_currency: str,
    basis_suspense: BasisSuspense,
) -> List[models.Transaction]:
    """
    Exhange a security for one or more other securities.

    Use the passed-in regex match to identify the source transaction
    (booking out the old security) and the primary destination security
    (booking in the new security); process this pair as a Transfer.
    Any transactions remaining in the sequence of ParsedCorpActs are processed
    as Spinoffs from the new destination security.

    Side effect:
        Modifies input basis_suspense dict in place (pops matches).
    """
    src_, dest_, spinoffs = _group_reorg(parsedCorpActs, match)
    src, dest = src_.raw, dest_.raw
    transactions = [
        merge_security_transfer(
            session, securities, account, src, dest, memo,
        )
    ]

    if spinoffs:
        if len(spinoffs) > 1:
            msg = (
                "flex.reader.merge_reorg(): "
                "More than one spinoff {}"
            )
            raise ValueError(msg.format(spinoffs))
        spinoff = spinoffs.pop().raw
        #  spinoff = spinoff.raw

        # HACK
        # IBKR processes some rights offerings in two parts -
        # 1) a sequence of type 'TO', booking out the old security,
        #    booking in the contra, and booking out subscription cash;
        # 2) a sequence of type 'TC', booking out the contra, booking in
        #    the new security, and booking in the subscribed-for security.
        # The 'TO' series contains the cash paid for the subscription, but
        # not the units, while the 'TC' series contains the units data but
        # not the cost.  Neither series, as grouped by
        # group_parsed_corpacts(), contains all the data.
        #
        # Here we pick up the cost & open date from where it was earlier
        # stashed in a dict by tender().  We assume that the 'TO' series
        # has a dateTime strictly earlier than the 'TC' series, so they'll
        # be sorted in correct time order by group_parsed_corpacts()
        # and merge_reorg() will have its values ready & waiting from
        # tender().
        basis_adj = basis_suspense.pop((src.uniqueidtype, src.uniqueid), None)
        if basis_adj:
            assert spinoff.units > 0
            tx_ = Types.Trade(
                fitid=spinoff.fitid,
                dttrade=basis_adj.datetime,
                memo=memo,
                uniqueidtype=spinoff.uniqueidtype,
                uniqueid=spinoff.uniqueid,
                units=spinoff.units,
                currency=basis_adj.currency,
                total=basis_adj.cash,
                reportdate=spinoff.reportdate,
                orig_tradeid=None,
                notes=(),
            )
            tx = ofx.reader.merge_trade(
                tx_,
                session=session,
                securities=securities,
                account=account,
                default_currency=default_currency,
                sortForTrade=sortForTrade,
            )
            transactions.append(cast(models.Transaction, tx))
        else:
            transactions.append(
                merge_spinoff(
                    session,
                    securities,
                    account,
                    spinoff,
                    fromsecurity=securities[(src.uniqueidtype, src.uniqueid)],
                    numerator=spinoff.units,
                    denominator=-src.units,
                    memo=memo,
                )
            )
    return transactions


########################################################################################
#  Utility Functions
########################################################################################
def guess_security(
    session: sqlalchemy.orm.session.Session,
    securities: SecuritiesMap,
    uniqueid: str,  # CUSIP or ISIN
    ticker: str
) -> models.Security:
    """
    Given a Security.uniqueid and/or ticker, try to look up corresponding
    models.Security instance from the FlexStatement securities list
    or the database.
    """

    def lookupDbByUid(
        uniqueidtype: str,
        uniqueid: str
    ) -> Optional[models.Security]:
        secid = (
            session.query(models.SecurityId)
            .filter_by(uniqueidtype=uniqueidtype, uniqueid=uniqueid)
            .one_or_none()
        )
        if secid:
            return secid.security
        return None

    def lookupSeclistByTicker(ticker: str) -> Optional[models.Security]:
        hits = [
            sec for sec in set(securities.values()) if sec.ticker == ticker
        ]
        assert len(hits) <= 1  # triggers self.multipleMatchErrs
        if hits:
            return hits.pop()
        return None

    security = None

    #  Highest confidence - look up security by unique identifier
    #  from the current statement or, failing that, the DB.
    uniqueidtype = (
        (validate_isin(uniqueid) and "ISIN")
        or (validate_cusip(uniqueid) and "CUSIP")
        or None
    )
    if uniqueidtype:
        security = (
            securities.get((uniqueidtype, uniqueid), None)
            or lookupDbByUid(uniqueidtype, uniqueid)
        )

    #  Fallback - look up security by ticker symbol
    #  from the current statement or, failing that, the DB.
    if (not security) and ticker:
        security = (
            lookupSeclistByTicker(ticker)
            or session.query(models.Security)
            .filter_by(ticker=ticker)
            .one_or_none()
        )

    if not security:
        sec = session.query(models.Security).filter_by(ticker="ELAN.TEMP").one_or_none()
        if sec:
            for secid in sec.ids:
                print(secid)
        raise ValueError(
            (
                f"Can't find security with uniqueidtype={uniqueidtype!r}, "
                f"uniqueid={uniqueid!r}, ticker={ticker!r}"
            )
        )

    return security


def _group_reorg(
    parsedCorpActs: List["ParsedCorpAct"],
    match: Match,
) -> Tuple[
    "ParsedCorpAct",
    "ParsedCorpAct",
    List["ParsedCorpAct"]
]:
    """
    Given ParsedCorpActs representing a single reorg, group them into source
    (i.e. security being booked out in the reorg), destination (i.e. security
    being booked in by the reorg), and additional in-kind reorg consideration
    which we treat as spinoffs from the destination security.

    Args:
        parsedCorpActs: a sequence of ParsedCorpAct instances
        match: a re.match instance capturing named groups (i.e. one of
               the flex.regexes applied to a CorporateTransaction.memo)

    Returns: tuple of (ParsedCorpAct for source security,
                       ParsedCorpAct for destination security,
                       list of spinoff ParsedCorpActs)
    """
    # Avoid side effects.
    # We're going to remove items from the list, so don't touch the
    # original data; operate on a copy
    parsedCorpActs = copy(parsedCorpActs)

    matchgroups = match.groupdict()
    isinFrom = matchgroups["isinFrom"]
    tickerFrom = matchgroups["tickerFrom"]
    isinTo0 = matchgroups.get("isinTo0", None)
    tickerTo0 = matchgroups.get("tickerTo0", None)

    def matchFirst(*testFuncs: Callable) -> ParsedCorpAct:
        return utils.first_true(
            [
                utils.first_true(parsedCorpActs, pred=testFunc)
                for testFunc in testFuncs
            ]
        )

    def isinFromTestFunc(ca: ParsedCorpAct) -> bool:
        return ca.cusip in isinFrom or isinFrom in ca.cusip

    def tickerFromTestFunc(ca):
        return ca.ticker == tickerFrom

    src = matchFirst(isinFromTestFunc, tickerFromTestFunc)
    if not src:
        msg = ("Can't find source transaction for {} within {}").format(
            {k: v for k, v in match.groupdict().items() if v},
            [ca.raw for ca in parsedCorpActs],
        )
        raise ValueError(msg)
    parsedCorpActs.remove(src)

    def isinToTestFunc(ca: ParsedCorpAct) -> bool:
        return (isinTo0 is not None and ca.cusip is not None) and (
            ca.cusip in isinTo0 or isinTo0 in ca.cusip
        )

    def tickerToTestFunc(ca: ParsedCorpAct) -> bool:
        return tickerTo0 is not None and ca.ticker == tickerTo0

    def loneCandidate() -> Optional[ParsedCorpAct]:
        if len(parsedCorpActs) == 1 and parsedCorpActs[0].cusip not in "isinFrom":
            return parsedCorpActs[0]
        return None

    dest = matchFirst(isinToTestFunc, tickerToTestFunc) or loneCandidate()
    if not dest:
        msg = (
            "On {}, can't find transaction with CUSIP/ISIN "
            "or ticker matching destination security "
            "for corporate action {}"
        ).format(src.rawdttrade, src.memo)
        raise ValueError(msg)
    parsedCorpActs.remove(dest)

    # Remaining ParsedCorpActs not matched as src/dest pairs treated as spinoffs
    return src, dest, parsedCorpActs


def sortForTrade(
    transaction: ofx.reader.Transaction
) -> Optional[models.TransactionSort]:
    """What models.TransactionSort algorithm applies to this transaction?
    """
    if isinstance(transaction, Types.CorporateAction):
        return None
    assert isinstance(transaction, (Types.Trade, Types.Exercise))

    note2sort = {
        ibflex.enums.Code.MAXLOSS: models.TransactionSort.MINGAIN,
        ibflex.enums.Code.LIFO: models.TransactionSort.LIFO,
    }
    sorts = [
        note2sort[note] for note in transaction.notes
        if note in note2sort
    ] if transaction.notes else []
    assert len(sorts) in (0, 1)
    if sorts:
        return sorts[0]

    return None


###############################################################################
# CLI SCRIPT
###############################################################################
def main():
    from argparse import ArgumentParser
    from capgains import flex

    argparser = ArgumentParser(description="Parse Flex Query data")
    argparser.add_argument("file", nargs="+", help="XML file(s)")
    argparser.add_argument(
        "--database", "-d", default="sqlite://", help="Database connection"
    )
    argparser.add_argument("--verbose", "-v", action="count", default=0)
    args = argparser.parse_args()

    logLevel = (3 - min(args.verbose, 2)) * 10
    logging.basicConfig(level=logLevel)
    logging.captureWarnings(True)

    # engine = create_engine(args.database, echo=True)
    engine = sqlalchemy.create_engine(args.database)
    Base.metadata.create_all(bind=engine)

    with sessionmanager(bind=engine) as session:
        for file in args.file:
            print(file)
            transactions = flex.read(session, file)
            for transaction in transactions:
                print(transaction)
                session.add(transaction)

    engine.dispose()


if __name__ == "__main__":
    main()
