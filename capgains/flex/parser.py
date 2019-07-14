# coding: utf-8
"""
Wrapper around ibflex.parser that converts Interactive Brokers Flex XML format
to conform to ofxtools.OfxTree structures, so data can be processed similarly.

Must-have fields
====
FlexStatement:
    AccountInformation

AccountInformation:
    accountId
    currency

SecurityInfo: N.B. this is not configurable through the web interface
    conid
    cusip
    isin
    symbol
    description

Trade:  N.B. Can't select Symbol Summary, Asset Class, or Orders
    tradeID
    conid
    reportDate
    description
    quantity
    currency
    netCash
    origTradeID
    notes  "Notes/Codes"

    preferably dateTime, otherwise tradeDate/tradeTime

    transactionID  N.B. this is not configurable through the web interface


TradeTransfer:
    tradeID
    tradeDate
    tradeTime
    description
    conid
    quantity
    currency
    netCash
    reportDate
    origTradeID
    notes
    transactionType
    deliveredReceived
    direction
    brokerName
    brokerAccount

CashTransaction:
    dateTime
    transactionID
    description
    conid
    type
    currency
    amount

CorporateAction:
    dateTime
    description
    conid
    quantity
    currency
    proceeds
    type
    reportDate
    code

Transfer:
    date
    description
    conid
    quantity
    direction
    type
    account

OptionEAE:
    conid
    transactionType
    date
    description
    quantity


ChangeInDividendAccrual:
    conid
    payDate
    code

ConversionRate:
    reportDate
    fromCurrency
    toCurrency
    rate
"""
from datetime import datetime
import itertools
import functools
from typing import Tuple, List, Optional, Union, Iterable, cast

import ibflex

from capgains import utils
from capgains.flex import BROKERID, Types, regexes


###############################################################################
# PARSE STATEMENT
###############################################################################
def parse(source) -> List[Types.FlexStatement]:
    statements = []
    response = ibflex.parser.parse(source)
    for stmt in response.FlexStatements:
        assert stmt.AccountInformation is not None

        # First parse trades, so they can be associated with options activity
        trades = parse_trades(stmt.Trades)
        exercises = parse_optionEAE(stmt.OptionEAE, trades)
        transactions = (
            cast(List[Types.Transaction], trades)
            + cast(List[Types.Transaction], exercises)
        )
        # Then process the rest of the transactions
        for transactionType in [
            "TradeTransfers",
            "CashTransactions",
            "CorporateActions",
            "Transfers",
        ]:
            report = getattr(stmt, transactionType)
            if report:
                subparser = SUBPARSERS[transactionType]
                transactions.extend(subparser(report))  # type: ignore

        statements.append(
            Types.FlexStatement(
                account=parse_acctinfo(stmt.AccountInformation),
                securities=parse_securities(stmt.SecuritiesInfo),
                transactions=transactions,
                #  Data with no analog in OFX is appended unchanged
                changeInDividendAccruals=stmt.ChangeInDividendAccruals,
                conversionRates=stmt.ConversionRates,
            )
        )

    return statements


def parse_acctinfo(acctinfo: ibflex.Types.AccountInformation) -> Types.Account:
    """Map Flex AccountInformation to OFX INVACCTFROM interface.
    """
    assert acctinfo.currency is not None
    assert acctinfo.accountId is not None
    return Types.Account(
        acctid=acctinfo.accountId,
        brokerid=BROKERID,
        name=acctinfo.acctAlias,
        currency=acctinfo.currency,
    )


def parse_securities(
    secinfos: Tuple[ibflex.Types.SecurityInfo, ...]
) -> List[Types.Security]:
    return list(
        itertools.chain.from_iterable(
            parse_security(s) for s in secinfos
        )
    )


def parse_security(secinfo: ibflex.Types.SecurityInfo) -> List[Types.Security]:
    """Map Flex SecurityInfo to OFX SECINFO interface.

    Note:
        Each ibflex.Types.SecurityInfo may return multiple Types.Security
        instances (one for each securities unique identifier type).
    """
    assert secinfo.symbol is not None

    def multisec(
        secinfo: ibflex.Types.SecurityInfo,
        uniqueidtype: str
    ) -> Optional[Types.Security]:
        """Create a separate Security instance for each securities unique
        identifier attribute set in SecurityInfo.
        """
        assert secinfo.symbol is not None
        # IBKR sometimes randomly prepends time/date stamp to ticker.  Strip it.
        match = regexes.secSymbolRE.match(secinfo.symbol)
        assert match
        uniqueid = getattr(secinfo, uniqueidtype.lower())
        return Types.Security(
            uniqueidtype=uniqueidtype,
            uniqueid=uniqueid,
            ticker=match.group("ticker"),
            secname=secinfo.description,
        ) if uniqueid else None

    secs = (multisec(secinfo, idtype) for idtype in ["CONID", "ISIN", "CUSIP"])
    return [sec for sec in secs if sec is not None]


###########################################################################
# TRADES
###########################################################################
def parse_trades(
    report: Tuple[ibflex.Types.Trade, ...]
) -> List[Types.Trade]:
    return [parse_trade(tx) for tx in report]


def parse_trade(
    trade: Union[ibflex.Types.Trade, ibflex.Types.TradeTransfer],
    description: Optional[str] = None,
) -> Types.Trade:
    """Map Flex Trade or TradeTransfer to OFX INVBUY/INVSELL interface.
    """
    # IB's scheme for Trade unique identifiers is messy & inconsistent.
    #
    # Every Trade has a `transactionID` (like CashTransaction), but
    # no TradeTransfer has a `transactionID`.
    #
    # Every TradeTransfer has a `tradeID`; most Trades have a `tradeID`
    # (Trades where `exchange` is blank have empty `tradeID`).
    #
    # Options exercise transactions refer to opening trades by `tradeID`
    # not `transactionID`.
    #
    # The best approach is therefore to use `tradeID` as unique identifier
    # for Trades and TradeTransfers when it's available, which is 95% of cases.
    # For Trades where `tradeID` is not available, use `transactionID` instead
    # (rather than leaving it blank, which would cause OfxReader.make_uid()
    # to generate one by hashing the relevant data fields).
    fitid = trade.tradeID or trade.transactionID

    #  Newer versions of Flex provide `dateTime` attribute for trade in
    #  addition to the older `tradeDate` / `tradeTime` attributes.
    #  TradeTransfer still just has `tradeDate` / `tradeTime`, not `dateTime`.
    dttrade = getattr(trade, "dateTime", None)
    if dttrade is None:
        assert trade.tradeDate is not None
        assert trade.tradeTime is not None
        dttrade = datetime.combine(trade.tradeDate, trade.tradeTime)

    assert trade.conid is not None
    assert trade.quantity is not None
    assert trade.currency is not None

    if isinstance(trade, ibflex.Types.Trade):
        total = trade.netCash
    else:
        assert isinstance(trade, ibflex.Types.TradeTransfer)
        total = trade.netTradeMoney

    assert total is not None

    assert trade.reportDate is not None
    assert trade.notes is not None
    assert trade.description is not None

    description = description or trade.description

    return Types.Trade(
        fitid=fitid,
        dttrade=dttrade,
        memo=description,
        uniqueidtype="CONID",
        uniqueid=trade.conid,
        units=trade.quantity,
        currency=trade.currency,
        total=total,
        #  Extra Flex-specific fields not in OFX
        reportdate=trade.reportDate,
        orig_tradeid=trade.origTradeID,
        notes=trade.notes,
    )


###########################################################################
# TRADE TRANSFERS
###########################################################################
def parse_trade_transfers(
    report: Tuple[ibflex.Types.TradeTransfer, ...]
) -> List[Types.Trade]:
    """Map Flex TradeTransfer to OFX INVBUY/INVSELL interface.
    """
    def parse_trade_transfer(tx: ibflex.Types.TradeTransfer) -> Types.Trade:
        def blank_none(value):
            return value if value else ""

        def blank_enum(enum):
            return enum.name if enum is not None else ""

        memo = (
            f"{blank_enum(tx.transactionType)} "
            f"{blank_enum(tx.deliveredReceived)} "
            f"{blank_enum(tx.direction)} "
            f"{blank_none(tx.brokerName)} "
            f"{blank_none(tx.brokerAccount)}: "
            f"{blank_none(tx.description)}"
        )
        return parse_trade(tx, description=memo)

    return [parse_trade_transfer(tx) for tx in report]


###########################################################################
# CASH TRANSACTIONS
###########################################################################
def parse_cash_transactions(
    report: Tuple[ibflex.Types.CashTransaction, ...]
) -> List[Types.CashTransaction]:
    """IB reports return of capital distribution as dividends.
    """
    return [
        parse_cash_transaction(tx) for tx in report
        if tx.type in (ibflex.enums.CashAction.DIVIDEND, )
    ]


def parse_cash_transaction(
    tx: ibflex.Types.CashTransaction
) -> Types.CashTransaction:
    """Map Flex CashTransaction to OFX INCOME interface.

    We're really only looking for return of capital, but the base OFXReader
    class doesn't actually handle RETOFCAP since the brokers don't send it.
    Therefore we map Flex dividends to INCOME, with OFXReader does handle.
    """
    assert tx.type is ibflex.enums.CashAction.DIVIDEND
    assert tx.description is not None
    assert tx.currency is not None
    assert tx.amount is not None
    assert tx.conid is not None

    # Flex CashTransactions record payment date have no
    # information about accrual date - this is contained in the
    # ChangeInDividendAccruals, which gets linked to CashTransactions
    # by flex.reader.FlexStatementReader.
    #
    # Payment date is recorded as datetime.date not datetime.datetime;
    # convert to datetime.datetime to match OFX format
    assert tx.dateTime is not None
    txdt = tx.dateTime
    dtsettle = datetime(txdt.year, txdt.month, txdt.day)

    return Types.CashTransaction(
        fitid=tx.transactionID,
        dttrade=None,
        dtsettle=dtsettle,
        memo=tx.description,
        uniqueidtype="CONID",
        uniqueid=tx.conid,
        incometype="DIV",
        currency=tx.currency,
        total=tx.amount,
    )


###########################################################################
# CORPORATE ACTIONS
###########################################################################
def parse_corporate_actions(
    report: Tuple[ibflex.Types.CorporateAction, ...]
) -> List[Types.CorporateAction]:
    return [parse_corporate_action(tx) for tx in report]


def parse_corporate_action(tx: ibflex.Types.CorporateAction) -> Types.CorporateAction:
    """Map Flex CorporateAction to synthetic data type.
    """
    assert tx.dateTime is not None
    assert tx.description is not None
    assert tx.conid is not None
    assert tx.quantity is not None
    assert tx.currency is not None
    assert tx.reportDate is not None
    assert tx.proceeds is not None

    type_ = tx.type or infer_corporate_action_type(tx.description)
    assert type_ is not None

    # CorporateActions don't include any sort of unique transaction identifier.
    # This generally works out OK b/c they're batch-processed at end of day.
    return Types.CorporateAction(
        fitid=None,
        dttrade=tx.dateTime,
        memo=tx.description,
        uniqueidtype="CONID",
        uniqueid=tx.conid,
        units=tx.quantity,
        currency=tx.currency,
        total=tx.proceeds,
        type=type_,
        reportdate=tx.reportDate,
        code=tx.code,
    )


def infer_corporate_action_type(
    memo: str
) -> ibflex.enums.Reorg:
    """Guess corporation type from transaction memo.

    Returns:
        First hit from MEMO_SIGNATURES.

    Raises:
        ValueError, if memo doesn't match anything in MEMO_SIGNATURES.
    """
    return utils.first_true(
        MEMO_SIGNATURES,
        default=ValueError(f"Can't infer type of corporate action {memo}"),
        pred=lambda signature: signature[0] in memo,
    )[1]


###########################################################################
# TRANSFERS
###########################################################################
def parse_transfers(
    report: Tuple[ibflex.Types.Transfer, ...]
) -> List[Types.Transfer]:
    return [parse_transfer(tx) for tx in report]


def parse_transfer(tx: ibflex.Types.Transfer) -> Types.Transfer:
    """Map Flex Transfer to OFX TRANSFER interface.

    Preserve Flex `type` and `account` attributes.
    """
    txdate = tx.date
    assert txdate is not None
    dttrade = datetime(txdate.year, txdate.month, txdate.day)

    assert tx.quantity is not None
    assert tx.direction is not None

    if tx.conid is not None:
        uniqueidtype: Optional[str] = "CONID"
        uniqueid: Optional[str] = tx.conid
    else:
        uniqueidtype = None
        uniqueid = None

    # Transfers don't include any sort of unique transaction identifier.
    # This generally works out OK b/c they're batch-processed at end of day.
    return Types.Transfer(
        fitid=None,
        dttrade=dttrade,
        memo=tx.description,
        uniqueidtype=uniqueidtype,
        uniqueid=uniqueid,
        units=tx.quantity,
        tferaction=tx.direction.name,
        type=tx.type,
        other_acctid=tx.account,
    )


########################################################################################
# OPTIONS EXERCISE/ASSIGNMENT/EXPIRATION
########################################################################################
def parse_optionEAE(
    report: Iterable[ibflex.Types.OptionEAE],
    trades: List[Types.Trade]
) -> List[Types.Exercise]:
    """Map Flex OptionEAE to OFX CLOSUREOPT interface.

    Inside the <OptionEAE> container, each assignment/exercise is represented
    by a pair of consecutive <OptionEAE> data elements - the first books out
    the option, the second books in the underlying.  These two legs appear
    consecutively in the <OptionEAE> container.

    'The data is grouped by Assignments, Exercises and Expirations, then
    by currency and then by option contract. Assignments and Exercises
    also display the underlying for each contract.'
    https://www.interactivebrokers.com/en/software/reportguide/reportguide/options_exercises_expirations_fq.htm

    Each OptionEAE data element has a matching data element under Trades,
    linked by a matching `tradeID` attribute for OptionEAE and Trade.

    Note:
        parse_trade() can't recognize trades representing options exercise
        in-band.  First parse all ibflex.Types.Trade instances to get a
        consistent interface, then pass the resulting Types.Trade
        instances in here to remove Trades representing options exercise.

    Returns:
        A list of Types.Exercise instance joining the options/underlying legs.

    Side effect:
        Modifies input list of Trades in place, removing Trades representing
        options exercise so FlexStatementReader won't try to process them.
    """
    transactions = []
    wip = None
    for optionEAE in report:
        transactionType = optionEAE.transactionType
        if transactionType is ibflex.enums.OptionAction.EXPIRE:
            #  FIXME need to realize capital loss if options expire worthless.
            wip = None
            continue

        tx = pluck_trade(optionEAE, trades)

        if transactionType in (
            ibflex.enums.OptionAction.ASSIGN,
            ibflex.enums.OptionAction.EXERCISE
        ):
            assert wip is None
            wip = functools.partial(
                Types.Exercise,
                fitid=tx.fitid,
                dttrade=tx.dttrade,
                memo=f"{transactionType.name.capitalize()} {tx.units} {tx.memo}",
                uniqueidtypeFrom=tx.uniqueidtype,
                uniqueidFrom=tx.uniqueid,
                unitsfrom=tx.units,
                reportdate=tx.reportdate,
            )
        else:
            assert wip is not None
            transactions.append(
                wip(
                    uniqueidtype=tx.uniqueidtype,
                    uniqueid=tx.uniqueid,
                    units=tx.units,
                    currency=tx.currency,
                    total=tx.total,
                    notes=tx.notes,
                )
            )
            wip = None
    assert wip is None
    return transactions


def pluck_trade(
    optionEAE: ibflex.Types.OptionEAE,
    trades: List[Types.Trade],
) -> Types.Trade:
    """Find Trade referred to by OptionEAE and remove it from list of Trades.

    Returns:
        Trade that matches input OptionEAE

    Side Effect:
        Modies input list of Trades (removes matching Trade)
    """
    hits = [
        index for index, tx in enumerate(trades)
        if tx.fitid == optionEAE.tradeID
    ]
    assert len(hits) == 1
    trade = trades.pop(hits.pop())

    dttrade = trade.dttrade
    date = optionEAE.date
    assert date is not None
    assert date.year == dttrade.year
    assert date.month == dttrade.month
    assert date.day == dttrade.day

    assert optionEAE.description == trade.memo
    assert optionEAE.conid == trade.uniqueid
    assert optionEAE.quantity == trade.units

    return trade


SUBPARSERS = {
    "TradeTransfers": parse_trade_transfers,
    "CashTransactions": parse_cash_transactions,
    "CorporateActions": parse_corporate_actions,
    "Transfers": parse_transfers,
}

#  MEMO_SIGNATURES infer CorporateAction type for data from before FlexQuery
#  schema included the `type` attribute.  Order is significant; higher
#  confidence matches come first.  Since 'SPINOFF' is sometimes used in the
#  security name field of temporary placeholders (contra CUSIPs), it comes
#  toward the end to avoid false positives.
MEMO_SIGNATURES = [
    ("BOND MATURITY", ibflex.enums.Reorg.BONDMATURITY),
    ("SUBSCRIBES TO", ibflex.enums.Reorg.SUBSCRIBERIGHTS),
    ("CUSIP/ISIN CHANGE", ibflex.enums.Reorg.ISSUECHANGE),
    ("OVER SUBSCRIBE", ibflex.enums.Reorg.ASSETPURCHASE),
    ("SUBSCRIBABLE RIGHTS ISSUE", ibflex.enums.Reorg.RIGHTSISSUE),
    ("STOCK DIVIDEND", ibflex.enums.Reorg.STOCKDIV),
    ("TENDERED TO", ibflex.enums.Reorg.TENDER),
    ("DELISTED", ibflex.enums.Reorg.DELISTWORTHLESS),
    ("SPLIT", ibflex.enums.Reorg.FORWARDSPLIT),
    ("SPLIT", ibflex.enums.Reorg.REVERSESPLIT),
    ("MERGE", ibflex.enums.Reorg.MERGER),
    ("SPINOFF", ibflex.enums.Reorg.SPINOFF),
    ("ACQUIRED", ibflex.enums.Reorg.MERGER),
]


def inferCorporateActionType(description):
    """
    Assign type by matching element 'description' attr to MEMO_SIGNATURES
    """
    sig = utils.first_true(
        MEMO_SIGNATURES,
        default=ValueError(f"Can't infer type of corporate action '{description}'"),
        pred=lambda sig: sig[0] in description,
    )
    return sig[1]


def main():
    from argparse import ArgumentParser

    argparser = ArgumentParser(
        description="Quick test of Interactive Brokers Flex XML data parser"
    )
    argparser.add_argument("file", nargs="+", help="XML data file(s)")
    args = argparser.parse_args()

    for file in args.file:
        print(file)
        response = parse(file)
        for stmt in response:
            for tx in stmt.transactions:
                if isinstance(tx, Types.Trade):
                    #  print(f"{tx.dttrade} {tx.units} {tx.memo}")
                    if tx.notes:
                        print(tx.notes)


if __name__ == "__main__":
    main()
