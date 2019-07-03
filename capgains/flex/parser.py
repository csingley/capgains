# coding: utf-8
"""
Wrapper around ibflex.parser that converts Interactive Brokers Flex XML format
to conform to ofxtools.OfxTree structures, so data can be processed similarly.
"""
# stdlib imports
from datetime import datetime
from functools import partial
from typing import List, Optional


# 3rd party imports
import ibflex.parser
import ibflex.schemata


# local imports
from capgains.flex import BROKERID
from capgains.flex.regexes import secSymbolRE
from . import types


###############################################################################
# PARSE STATEMENT
###############################################################################
def parse(source) -> List[types.FlexStatement]:
    statements = []
    response = ibflex.parser.parse(source)
    for stmt in response["FlexStatements"]:
        # First parse trades, so they can be associated with options activity
        trades = parse_trades(stmt.get("Trades") or [])
        exercises = parse_optionEAE(stmt.get("OptionEAE") or [], trades)
        transactions = trades + exercises
        # Then process the rest of the transactions
        for transactionType in [
            "TradeTransfers",
            "CashTransactions",
            "CorporateActions",
            "Transfers",
        ]:
            report = stmt.get(transactionType, None)
            if report is not None:
                subparser = SUBPARSERS[transactionType]
                transactions.extend(subparser(report))

        statements.append(
            types.FlexStatement(
                account=parse_acctinfo(stmt["AccountInformation"]),
                securities=parse_securities(stmt["SecuritiesInfo"]),
                dividends=parse_dividends(stmt.get("ChangeInDividendAccruals") or []),
                transactions=transactions,
                conversionrates=parse_conversionrates(
                    stmt.get("ConversionRates") or {}
                ),
            )
        )

    return statements


def parse_acctinfo(acctinfo: dict) -> types.Account:
    return types.Account(
        acctid=acctinfo["accountId"],
        brokerid=BROKERID,
        name=acctinfo["acctAlias"],
        currency=acctinfo["currency"],
    )


def parse_securities(secinfos: List[dict]) -> List[types.Security]:
    securities = []
    for secinfo in secinfos:
        securities.extend(parse_security(secinfo))
    return securities


def parse_security(secinfo: dict) -> List[types.Security]:
    """Create a Security instance for each (uniqueidtype, uniqueid) pair.
    """
    securities = []
    match = secSymbolRE.match(secinfo["symbol"])
    assert match
    ticker = match.group("ticker")
    secname = secinfo["description"]
    for uniqueidtype in ("CONID", "ISIN", "CUSIP"):
        uniqueid = secinfo[uniqueidtype.lower()]
        if uniqueid:
            securities.append(
                types.Security(
                    uniqueidtype=uniqueidtype,
                    uniqueid=uniqueid,
                    ticker=ticker,
                    secname=secname,
                )
            )
    return securities


def parse_dividends(divs: List[dict]) -> List[types.Dividend]:
    # Dividends paid during period are marked as reversal of accrual
    dividends = [
        types.Dividend(
            conid=div["conid"],
            exDate=div["exDate"],
            payDate=div["payDate"],
            quantity=div["quantity"],
            grossRate=div["grossRate"],
            taxesAndFees=div["tax"] + div["fee"],
            total=div["netAmount"],
        )
        for div in divs
        if "Re" in div["code"]
    ]
    return dividends


def parse_conversionrates(
    rates: types.ConversionRatesDict,
) -> List[types.ConversionRate]:
    conversionrates = [
        types.ConversionRate(
            date=date, fromcurrency=fromcurrency, tocurrency=tocurrency, rate=rate
        )
        for (fromcurrency, tocurrency, date), rate in rates.items()
    ]
    return conversionrates


###########################################################################
# TRADES
###########################################################################
def parse_trades(report: List[types.TradeDict]) -> List[types.Trade]:
    return [parse_trade(tx) for tx in report]


def parse_trade(
    trade: types.TradeDict, description: Optional[str] = None
) -> types.Trade:
    # IB's scheme for Trade unique identifiers is messy & inconsistent.
    #
    # Every Trade has a `transactionID` (like CashTransactions), but
    # no TradeTransfer has a `transactionID`.
    #
    # Every TradeTransfer has a `tradeID`; most Trades have a `tradeID`
    # (Trades where `exchange` is blank have empty `tradeID`).
    #
    # Options exercise transactions refer to opening trades by `tradeID`
    # not `transactionID`.
    #
    # The best approach is therefore to use `tradeID` as unique identifier
    # for Trades and TradeTransfers when it's available, which is 99% of cases.
    # For Trades where `tradeID` is not available, use `transactionID` instead
    # (rather than leaving it blank, which would cause OfxReader.make_uid()
    # to generate one by hashing the relevant data fields).
    description = description or trade["description"]
    return types.Trade(
        fitid=trade["tradeID"] or trade["transactionID"],
        dttrade=datetime.combine(trade["tradeDate"], trade["tradeTime"]),
        memo=description,
        uniqueidtype="CONID",
        uniqueid=trade["conid"],
        units=trade["quantity"],
        currency=trade["currency"],
        total=trade["netCash"],
        reportdate=trade["reportDate"],
        orig_tradeid=trade["origTradeID"],
        notes=trade["notes"],
    )


###########################################################################
# TRADE TRANSFERS
###########################################################################
def parse_trade_transfers(report: List[types.TradeTransferDict]) -> List[types.Trade]:
    return [
        parse_trade(
            tx,
            description=(
                "{transactionType} {deliveredReceived} {direction} {brokerName} "
                "{brokerAccount}: {description}"
            ).format(**tx),
        )
        for tx in report
    ]


###########################################################################
# CASH TRANSACTIONS
###########################################################################
def parse_cash_transactions(
    report: List[types.CashTransactionDict]
) -> List[types.CashTransaction]:
    return [parse_cash_transaction(tx) for tx in report]


def parse_cash_transaction(tx: types.CashTransactionDict) -> types.CashTransaction:
    # Flex CashTransactions record payment date have no
    # information about accrual date - this is contained in the
    # ChangeInDividendAccruals, which gets linked to CashTransactions
    # by flex.reader.FlexStatementReader.
    #
    # Payment date is recorded as datetime.date not datetime.datetime;
    # convert to datetime.datetime to match OFX format
    txdt = tx["dateTime"]
    dtsettle = datetime(txdt.year, txdt.month, txdt.day)

    return types.CashTransaction(
        fitid=tx["transactionID"],
        dttrade=None,
        dtsettle=dtsettle,
        memo=tx["description"],
        uniqueidtype="CONID",
        uniqueid=tx["conid"],
        incometype=tx["type"],
        currency=tx["currency"],
        total=tx["amount"],
    )


###########################################################################
# CORPORATE ACTIONS
###########################################################################
def parse_corporate_actions(
    report: List[types.CorporateActionDict]
) -> List[types.CorporateAction]:
    return [parse_corporate_action(tx) for tx in report]


def parse_corporate_action(tx: types.CorporateActionDict) -> types.CorporateAction:
    # CorporateActions don't include any sort of unique transaction identifier.
    # This generally works out OK b/c they're batch-processed at end of day.
    return types.CorporateAction(
        fitid=None,
        dttrade=tx["dateTime"],
        memo=tx["description"],
        uniqueidtype="CONID",
        uniqueid=tx["conid"],
        units=tx["quantity"],
        currency=tx["currency"],
        total=tx["proceeds"],
        type=tx["type"],
        reportdate=tx["reportDate"],
        code=tx["code"],
    )


###########################################################################
# TRANSFERS
###########################################################################
def parse_transfers(report: List[types.TransferDict]) -> List[types.Transfer]:
    return [parse_transfer(tx) for tx in report]


def parse_transfer(tx: types.TransferDict) -> types.Transfer:
    # Transfers don't include any sort of unique transaction identifier.
    # This generally works out OK b/c they're batch-processed at end of day.
    return types.Transfer(
        fitid=None,
        dttrade=tx["date"],
        memo=tx["description"],
        uniqueidtype="CONID",
        uniqueid=tx["conid"],
        units=tx["quantity"],
        tferaction=tx["direction"],
        type=tx["type"],
        other_acctid=tx["account"],
    )


########################################################################################
# OPTIONS EXERCISE/ASSIGNMENT/EXPIRATION
########################################################################################
def parse_optionEAE(report: List[types.OptionEaeDict], trades: List[types.Trade]):
    """
    'The data is grouped by Assignments, Exercises and Expirations, then
    by currency and then by option contract. Assignments and Exercises
    also display the underlying for each contract.'
    https://www.interactivebrokers.com/en/software/reportguide/reportguide/options_exercises_expirations_fq.htm
    """
    transactions = []
    wip = None
    for elem in report:
        transactionType = elem["transactionType"]
        if transactionType == "Expiration":
            wip = None
            continue

        tx = pluck_trade(elem, trades)
        dttrade = tx.dttrade

        if transactionType in ("Assignment", "Exercise"):
            assert wip is None
            wip = partial(
                types.Exercise,
                fitid=tx.fitid,
                dttrade=dttrade,
                memo="Exercise " + tx.memo,
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


def pluck_trade(elem: types.OptionEaeDict, trades: List[types.Trade]):
    hits = [index for index, tx in enumerate(trades) if tx.fitid == elem["tradeID"]]
    assert len(hits) == 1
    trade = trades.pop(hits.pop())

    dttrade = trade.dttrade
    date = elem["date"]
    assert date.year == dttrade.year
    assert date.month == dttrade.month
    assert date.day == dttrade.day

    assert elem["description"] == trade.memo
    assert elem["conid"] == trade.uniqueid
    assert elem["quantity"] == trade.units

    return trade


SUBPARSERS = {
    "TradeTransfers": parse_trade_transfers,
    "CashTransactions": parse_cash_transactions,
    "CorporateActions": parse_corporate_actions,
    "Transfers": parse_transfers,
}
