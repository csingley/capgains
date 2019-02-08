# coding: utf-8
"""
Wrapper around ibflex.parser that converts Interactive Brokers Flex XML format
to conform to ofxtools.OfxTree structures, so data can be processed similarly.
"""
# stdlib imports
from collections import namedtuple
from datetime import datetime
from functools import partial


# 3rd party imports
import ibflex.parser


# local imports
from capgains.flex import BROKERID
from capgains.flex.regexes import secSymbolRE


def parse(source):
    statements = []
    response = ibflex.parser.parse(source)
    for stmt in response['FlexStatements']:
        # First parse trades, so they can be associated with options activity
        trades = parse_trades(stmt['Trades'])
        exercises = parse_optionEAE(stmt['OptionEAE'], trades)
        transactions = trades + exercises
        # Then process the rest of the transactions
        for transactionType in ['TradeTransfers', 'CashTransactions',
                                'CorporateActions', 'Transfers']:
            report = stmt.get(transactionType, None)
            if report is not None:
                subparser = SUBPARSERS[transactionType]
                transactions.extend(subparser(report))

        statements.append(FlexStatement(
            account=parse_acctinfo(stmt['AccountInformation']),
            securities=parse_securities(stmt['SecuritiesInfo']),
            dividends=parse_dividends(stmt['ChangeInDividendAccruals']),
            transactions=transactions))

    return statements


def parse_acctinfo(acctinfo):
    return Account(acctid=acctinfo['accountId'], brokerid=BROKERID,
                   name=acctinfo['acctAlias'],
                   currency=acctinfo['currency'])


def parse_securities(secinfos):
    securities = []
    for secinfo in secinfos:
        securities.extend(parse_security(secinfo))
    return securities


def parse_security(secinfo):
    securities = []
    ticker = secSymbolRE.match(secinfo['symbol']).group('ticker')
    secname = secinfo['description']
    for uniqueidtype in ('CONID', 'ISIN', 'CUSIP'):
        uniqueid = secinfo[uniqueidtype.lower()]
        if uniqueid:
            securities.append(Security(
                uniqueidtype=uniqueidtype, uniqueid=uniqueid,
                ticker=ticker, secname=secname))
    return securities


def parse_dividends(divs):
    # Dividends paid during period are marked as reversal of accrual
    dividends = [
        Dividend(conid=div['conid'], exDate=div['exDate'],
                 payDate=div['payDate'], quantity=div['quantity'],
                 grossRate=div['grossRate'],
                 taxesAndFees=div['tax'] + div['fee'],
                 total=div['netAmount'])
        for div in divs if 'Re' in div['code']]
    return dividends


###########################################################################
# TRADES
###########################################################################
def parse_trades(report):
    return [parse_trade(tx) for tx in report]


def parse_trade(tx, description=None):
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
    description = description or tx['description']
    return Trade(
        fitid=tx['tradeID'] or tx['transactionID'],
        dttrade=datetime.combine(tx['tradeDate'], tx['tradeTime']),
        memo=description, uniqueidtype='CONID', uniqueid=tx['conid'],
        units=tx['quantity'], currency=tx['currency'], total=tx['netCash'],
        reportdate=tx['reportDate'], notes=tx['notes'])


###########################################################################
# TRADE TRANSFERS
###########################################################################
def parse_trade_transfers(report):
    return [parse_trade(
        tx,
        description='{} {} {} {} {}: {}'.format(
            tx['transactionType'], tx['deliveredReceived'],
            tx['direction'], tx['brokerName'], tx['brokerAccount'],
            tx['description']),
    ) for tx in report]


###########################################################################
# CASH TRANSACTIONS
###########################################################################
def parse_cash_transactions(report):
    return [parse_cash_transaction(tx) for tx in report]


def parse_cash_transaction(tx):
    # Flex CashTransactions record payment date have no
    # information about accrual date - this is contained in the
    # ChangeInDividendAccruals, which gets linked to CashTransactions
    # by flex.reader.FlexStatementReader.
    #
    # Payment date is recorded as datetime.date not datetime.datetime;
    # convert to datetime.datetime to match OFX format
    txdt = tx['dateTime']
    dtsettle = datetime(txdt.year, txdt.month, txdt.day)

    return CashTransaction(
        fitid=tx['transactionID'], dttrade=None, dtsettle=dtsettle,
        memo=tx['description'], uniqueidtype='CONID', uniqueid=tx['conid'],
        incometype=tx['type'], currency=tx['currency'], total=tx['amount'])


###########################################################################
# CORPORATE ACTIONS
###########################################################################
def parse_corporate_actions(report):
    return [parse_corporate_action(tx) for tx in report]


def parse_corporate_action(tx):
    # CorporateActions don't include any sort of unique transaction identifier.
    # This generally works out OK b/c they're batch-processed at end of day.
    return CorporateAction(
        fitid=None, dttrade=tx['dateTime'], memo=tx['description'],
        uniqueidtype='CONID', uniqueid=tx['conid'], units=tx['quantity'],
        currency=tx['currency'], total=tx['proceeds'], type=tx['type'],
        reportdate=tx['reportDate'], code=tx['code'])


###########################################################################
# TRANSFERS
###########################################################################
def parse_transfers(report):
    return [parse_transfer(tx) for tx in report]


def parse_transfer(tx):
    # Transfers don't include any sort of unique transaction identifier.
    # This generally works out OK b/c they're batch-processed at end of day.
    return Transfer(
        fitid=None, dttrade=tx['date'], memo=tx['description'],
        uniqueidtype='CONID', uniqueid=tx['conid'], units=tx['quantity'],
        tferaction=tx['direction'], type=tx['type'],
        other_acctid=tx['account'])


###########################################################################
# OPTIONS EXERCISE/ASSIGNMENT/EXPIRATION
###########################################################################
def parse_optionEAE(report, trades):
    """
    'The data is grouped by Assignments, Exercises and Expirations, then
    by currency and then by option contract. Assignments and Exercises
    also display the underlying for each contract.'
    https://www.interactivebrokers.com/en/software/reportguide/reportguide/options_exercises_expirations_fq.htm
    """
    transactions = []
    wip = None
    for elem in report:
        transactionType = elem['transactionType']
        if transactionType == 'Expiration':
            wip = None
            continue

        tx = pluck_trade(elem, trades)
        dttrade = tx.dttrade

        if transactionType in ('Assignment', 'Exercise'):
            assert wip is None
            wip = partial(
                Exercise, fitid=tx.fitid, dttrade=dttrade,
                memo='Exercise '+tx.memo, uniqueidtypeFrom=tx.uniqueidtype,
                uniqueidFrom=tx.uniqueid, unitsFrom=tx.units,
                reportdate=tx.reportdate)
        else:
            transactions.append(
                wip(uniqueidtype=tx.uniqueidtype, uniqueid=tx.uniqueid,
                    units=tx.units, currency=tx.currency, total=tx.total,
                    notes=tx.notes)
            )
            wip = None
    assert wip is None
    return transactions


def pluck_trade(elem, trades):
    hits = [index for index, tx in enumerate(trades)
            if tx.fitid == elem['tradeID']]
    assert len(hits) == 1
    trade = trades.pop(hits.pop())

    dttrade = trade.dttrade
    date = elem['date']
    assert date.year == dttrade.year
    assert date.month == dttrade.month
    assert date.day == dttrade.day

    assert elem['description'] == trade.memo
    assert elem['conid'] == trade.uniqueid
    assert elem['quantity'] == trade.units

    return trade


SUBPARSERS = {'TradeTransfers': parse_trade_transfers,
              'CashTransactions': parse_cash_transactions,
              'CorporateActions': parse_corporate_actions,
              'Transfers': parse_transfers}


###############################################################################
# DATA CONTAINERS - implement ofxtools.models.investment data structures
# (plus some extra fields)
###############################################################################
FlexStatement = namedtuple('FlexStatement', [
    'account', 'securities', 'dividends', 'transactions'])
Account = namedtuple('Account', ['acctid', 'brokerid', 'name', 'currency'])
Security = namedtuple('Security',
                      ['uniqueidtype', 'uniqueid', 'ticker', 'secname'])
Dividend = namedtuple('Dividend', ['conid', 'exDate', 'payDate', 'quantity',
                                   'grossRate', 'taxesAndFees', 'total'])
Trade = namedtuple('Trade', [
    'fitid', 'dttrade', 'memo', 'uniqueidtype', 'uniqueid', 'units',
    'currency', 'total', 'reportdate', 'notes'])
CashTransaction = namedtuple('CashTransaction', [
    'fitid', 'dttrade', 'dtsettle', 'memo', 'uniqueidtype', 'uniqueid',
    'incometype', 'currency', 'total'])
Transfer = namedtuple('Transfer', [
    'fitid', 'dttrade', 'memo', 'uniqueidtype', 'uniqueid', 'units',
    'tferaction', 'type', 'other_acctid'])
CorporateAction = namedtuple('CorporateAction', [
    'fitid', 'dttrade', 'memo', 'uniqueidtype', 'uniqueid', 'units',
    'currency', 'total', 'type', 'reportdate', 'code'])
Exercise = namedtuple('Exercise', [
    'fitid', 'dttrade', 'memo', 'uniqueidtype', 'uniqueid', 'units',
    'uniqueidtypeFrom', 'uniqueidFrom', 'unitsFrom', 'currency', 'total',
    'reportdate', 'notes'])
