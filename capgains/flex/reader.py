# coding: utf-8
"""
Importer for Interactive Brokers Flex XML format
"""
# stdlib imports
from collections import (namedtuple, OrderedDict)
from operator import attrgetter
from decimal import Decimal
#  from datetime import datetime
import functools
import warnings
import logging
from copy import copy


# 3rd party imports
from sqlalchemy import create_engine
from ofxtools.utils import (validate_cusip, cusip2isin, validate_isin)


# local imports
from capgains.ofx.reader import OfxStatementReader
from capgains.database import Base, sessionmanager
from capgains.models.transactions import (
    FiAccount, Security, SecurityId, CurrencyRate, TransactionType
)
from capgains.flex import BROKERID
from capgains.flex.regexes import (
    corpActRE, changeSecurityRE, rightsIssueRE, splitRE, stockDividendRE,
    spinoffRE, subscribeRE, cashMergerRE, kindMergerRE, cashAndKindMergerRE,
    tenderRE,
)
from capgains.flex.parser import (CorporateAction, Trade)
from capgains.containers import GroupedList


class FlexResponseReader(object):
    """
    Processor for sequences of capgains.flex.parser.FlexStatement instances
    as created by capgains.flex.parser.FlexResponseParser.parse()

    """
    def __init__(self, session, response):
        """
        Args: session - sqlalchemy.orm.session.Session instance
              response - sequence of capgains.flex.parser.FlexStatement
                         instances
        """
        self.session = session
        self.statements = [FlexStatementReader(session, stmt)
                           for stmt in response]

    def read(self):
        for stmt in self.statements:
            stmt.read()


class FlexStatementReader(OfxStatementReader):
    """
    Processor for capgains.flex.parser.FlexStatement instances
    """
    def __init__(self, session, statement=None):
        """
        Args: session - sqlalchemy.orm.session.Session instance
              statement - capgains.flex.parser.FlexStatement instance
        """
        self.session = session
        self.statement = statement
        self.account = None
        self.index = None
        self.securities = {}
        self.dividends = {}
        self.transactions = []
        self._basis_stack = {} # HACK - cf. tender(), merge_reorg()

    def read(self, doTransactions=True):
        """
        Extends OfxStatementReader superclass method - also processes parsed
        Flex ChangeInDividendAccruals and ConversionRates.
        """
        self.read_dividends()
        self.read_currency_rates()
        super(FlexStatementReader, self).read(doTransactions)

    def read_header(self):
        """
        Override OfxStatementReader.read_header()
        """
        pass

    def read_dividends(self):
        """
        Create a dict of capgains.flex.parser.Dividend instances keyed
        by (CONID, payDate).

        Used to fix the accrual dates of CashTransactions.
        """
        self.dividends = {(div.conid, div.payDate): div
                          for div in self.statement.dividends
                          if div.payDate is not None}

    def read_currency_rates(self):
        """
        Persist currency conversion rates to DB.

        Used in reporting, to translate inventory.Gain instances to
        functional currency.
        """
        rates = self.statement.conversionrates
        for rate in rates:
            cr = CurrencyRate.merge(self.session, **rate._asdict())

    def read_securities(self):
        for secinfo in self.statement.securities:
            sec = Security.merge(
                self.session, uniqueidtype=secinfo.uniqueidtype,
                uniqueid=secinfo.uniqueid, name=secinfo.secname,
                ticker=secinfo.ticker)
            self.securities[(secinfo.uniqueidtype, secinfo.uniqueid)] = sec

    transaction_handlers = {'Trade': 'doTrades',
                            'CashTransaction': 'doCashTransactions',
                            'Transfer': 'doTransfers',
                            'CorporateAction': 'doCorporateActions',
                            'Exercise': 'doOptionsExercises'}

    ###########################################################################
    # TRADES
    #
    # These methods override OfxStatementReader superclass methods.
    # They are used by OfxStatementReader.doTrades(), which provides
    # their context.
    ###########################################################################
    @staticmethod
    def filterTrades(transaction):
        """
        Should this trade be processed?  Discard FX trades.

        Overrides OfxStatementReader superclass method.

        Arg: an instance implementing the interface of flex.parser.Trade
        Returns: boolean
        """
        # FIXME - better way to skip currency trades
        currencyPairs = ['USD.CAD', 'CAD.USD', 'USD.EUR', 'EUR.USD']
        # conid for currency pairss
        # '15016062'  # USD.CAD
        # '15016251'  # CAD.USD
        # '12087792'  # EUR.USD
        return transaction.memo not in currencyPairs

    @staticmethod
    def filterTradeCancels(transaction):
        """
        Is this trade actually a trade cancellation?  Consult trade notes.

        Overrides OfxStatementReader superclass method.

        Arg: an instance implementing the interface of flex.parser.Trade
        Returns: boolean
        """
        return 'Ca' in transaction.notes

    @staticmethod
    def matchTradeWithCancel(canceler, canceled):
        """
        Does one of these trades cancel the other?

        Overrides OfxStatementReader superclass method.

        Args: two instances implementing the interface of
              ofxtools.models.investment.{BUY*, SELL*}
        Returns: boolean
        """
        match = False

        if canceler.orig_tradeid not in (None, '', '0'):
            match = canceler.orig_tradeid == canceled.fitid
        else:
            match = (canceler.units == -1 * canceled.units) \
                    and (canceler.currency == canceled.currency) \
                    and (canceler.total == -1 * canceled.total)

        return match

    @staticmethod
    def sortCanceledTrades(transaction):
        """
        Determines order in which trades are canceled - order by transaction
        report date (i.e. the date the trade was reported, as opposed to when
        it was executed).

        Overrides OfxStatementReader superclass method.

        Arg: an instance implementing the interface of flex.parser.Trade
        """
        return transaction.reportdate

    @staticmethod
    def sortForTrade(transaction):
        """
        What flex.parser sort algorithm applies to this transaction?

        Overrides OfxStatementReader superclass method.

        Arg: an instance implementing the interface of flex.parser.Trade
        Returns: type str
        """
        if hasattr(transaction, 'notes'):
            note2sort = [('ML', 'MINGAIN'), ('LI', 'LIFO')]
            sorts = [sort for note, sort in note2sort
                     if transaction.notes and note in transaction.notes]
            assert len(sorts) in (0, 1)
            if sorts:
                return sorts[0]

    ###########################################################################
    # CASH TRANSACTIONS
    #
    # These methods override OfxStatementReader superclass methods.
    # They are used by OfxStatementReader.doCashTransactions(), which provides
    # their context.
    ###########################################################################
    @staticmethod
    def filterCashTransactions(transaction):
        """
        Judge whether a transaction should be processed (i.e. it's a return
        of capital).

        Overrides OfxStatementReader superclass method.

        Arg: an instance implementing the interface of
             flex.parser.CashTransaction
        Returns: boolean
        """
        memo = transaction.memo.lower()
        return transaction.incometype == 'Dividends' and (
            'return of capital' in memo or 'interimliquidation' in memo)

    @classmethod
    def groupCashTransactionsForCancel(cls, transaction):
        """
        Cash transactions are grouped together for cancellation/netting
        if they're for the same security at the same time with the same memo.

        Overrides OfxStatementReader superclass method.

        Arg: an instance implementing the interface of
             flex.parser.CashTransaction
        """
        security = (transaction.uniqueidtype, transaction.uniqueid)
        memo = cls.stripCashTransactionMemo(transaction.memo)
        return transaction.dtsettle, security, memo

    @staticmethod
    def stripCashTransactionMemo(memo):
        """
        Strip "REVERSAL"/"CANCEL" from transaction description so reversals
        sort together with reversees

        Arg: type str
        Returns: type str
        """
        memo = memo.replace(' - REVERSAL', '')
        memo = memo.replace('CANCEL ', '')
        return memo

    @staticmethod
    def filterCashTransactionCancels(transaction):
        """
        Is this cash transaction actually a reversal?

        Overrides OfxStatementReader superclass method.

        Arg: an instance implementing the interface of
             flex.parser.CashTransaction
        Returns: boolean
        """
        memo = transaction.memo
        return 'REVERSAL' in memo or 'CANCEL' in memo

    @staticmethod
    def sortCanceledCashTransactions(transaction):
        """
        Determines order in which cash transactions are reversed.

        Overrides OfxStatementReader superclass method.

        Arg: an instance implementing the interface of
             flex.parser.CashTransaction
        """
        return transaction.fitid

    def fixCashTransactions(self, transaction):
        """
        If we can find a matching record in parsed ChangeInDividendAccruals,
        use the indicated ex-date for the CashTransaction.

        Overrides OfxStatementReader superclass method.

        Arg: an instance implementing the interface of
             flex.parser.CashTransaction
        Returns: an instance implementing the interface of
                 flex.parser.CashTransaction
        """
        payDt = transaction.dtsettle
        # N.B. transaction.dtsettle is a datetime.datetime, but self.dividends
        # is keyed by (uniqueid, date).  Transform datetime to date for lookup.
        match = self.dividends.get((transaction.uniqueid, payDt.date()), None)
        if match:
            transaction = transaction._replace(dttrade=match.exDate)
        else:
            transaction = transaction._replace(dttrade=payDt)

        return transaction

    ###########################################################################
    # ACCOUNT TRANSFERS
    ###########################################################################
    def doTransfers(self, transactions):
        # Only handle securities transfers
        txs = GroupedList(transactions)\
                .filter(attrgetter('uniqueid'))\
                .map(self.merge_account_transfer)

    def merge_account_transfer(self, transaction):
        if transaction.type == 'INTERNAL':
            acctFrom = FiAccount.merge(self.session, brokerid=BROKERID,
                                       number=transaction.other_acctid)
        elif transaction.type == 'ACATS':
            # IBKR strips out punctuation from other brokers' acct#;
            # alphanumeric only
            accts = [(a.id, ''.join([c for c in a.number if c.isalnum()]))
                     for a in self.session.query(FiAccount).all()]

            acctTuple = first_true(accts, pred=functools.partial(
                lambda at, num: at[1] == num, num=transaction.other_acctid))
            if not acctTuple:
                msg = ("Can't find FiAccount.number={}; "
                       "skipping external transfer {}")
                warnings.warn(msg.format(transaction.other_acctid,
                                         transaction.memo))
                return
            acctFrom = self.session.query(FiAccount).get(acctTuple[0])
        else:
            msg = "type '{}' not one of ('INTERNAL', 'ACATS') for {}"
            raise ValueError(msg.format(transaction.type))

        acct = self.account
        units = transaction.units
        unitsFrom = -units
        direction = transaction.tferaction
        assert direction in ('IN', 'OUT')
        if direction == 'OUT':
            unitsFrom, units = units, unitsFrom
            acctFrom, acct = acct, acctFrom

        security = self.securities[
            (transaction.uniqueidtype, transaction.uniqueid)]
        transaction = self.merge_transaction(
            type=TransactionType.TRANSFER, fiaccount=acct, uniqueid=transaction.fitid,
            datetime=transaction.dttrade, memo=transaction.memo,
            security=security, units=units, fiaccountFrom=acctFrom,
            securityFrom=security, unitsFrom=unitsFrom)

        return transaction

    ###########################################################################
    # CORPORATE ACTIONS
    ###########################################################################
    def doCorporateActions(self, transactions):
        """
        Group corporate actions by datetime/type/memo; net by security;
        dispatch each group to type-specific handler for further processing.

        Args: a sequence of flex.parser.CorporateAction instances
        """
        group = self.preprocessCorporateActions(transactions)
        # group is now a GroupedList in `grouped` state, containing
        # GroupedLists of ParsedCorporateAction instances
        assert group.grouped is True
        for pcas in group:
            assert pcas.grouped is False
            dttrade, type_, memo = pcas.key
            handler_tuple = CORPACT_HANDLERS[type_]
            if handler_tuple is None:
                msg = ("flex.reader.CORPACT_HANDLERS doesn't know how to "
                       "handle type='{}' for corporate actions {}")
                raise ValueError(msg.format(type_, pcas))
            handler = getattr(self, handler_tuple[1])
            handler(pcas, memo=memo)

    def preprocessCorporateActions(self, transactions):
        # See containers.GroupedList for detailed workings
        group = GroupedList(transactions)\
                .groupby(self.groupCorporateActionsForCancel)\
                .cancel(filterfunc=self.filterCorporateActionCancels,
                        matchfunc=self.matchCorporateActionWithCancel,
                        sortfunc=self.sortCanceledCorporateActions)\
                .reduce(self.netCorporateActions)\
                .flatten()\
                .map(self.parseCorporateActionMemo)\
                .groupby(self.groupParsedCorporateActions)\
                .sorted(self.sortParsedCorporateActions)

        return group

    @staticmethod
    def groupCorporateActionsForCancel(corpAct):
        return ((corpAct.uniqueidtype, corpAct.uniqueid),
                corpAct.dttrade, corpAct.type, corpAct.memo)

    @staticmethod
    def filterCorporateActionCancels(transaction):
        return 'Ca' in transaction.code

    @staticmethod
    def matchCorporateActionWithCancel(transaction0, transaction1):
        return transaction0.units == -1 * transaction1.units

    @staticmethod
    def sortCanceledCorporateActions(corpAct):
        return corpAct.reportdate

    @staticmethod
    def netCorporateActions(corpAct0, corpAct1):
        assert corpAct0.currency == corpAct1.currency
        units = corpAct0.units + corpAct1.units
        total = corpAct0.total + corpAct1.total
        return CorporateAction(fitid=corpAct0.fitid, dttrade=corpAct0.dttrade,
                               memo=corpAct0.memo,
                               uniqueidtype=corpAct0.uniqueidtype,
                               uniqueid=corpAct0.uniqueid, units=units,
                               currency=corpAct0.currency, total=total,
                               type=corpAct0.type,
                               reportdate=corpAct0.reportdate,
                               code=corpAct0.code)

    def parseCorporateActionMemo(self, transaction):
        """
        Parse memo; pack results in ParsedCorpAct tuple.

        Args: an instance implementing the interface of
              flex.parser.CorporateAction
        Returns: a ParsedCorpAct instance
        """
        memo = transaction.memo
        typ = getattr(transaction, 'type', '') \
                or self.inferCorporateActionType(memo)

        match = corpActRE.match(memo)
        if match is None:
            dateTime = transaction.datetime
            msg = "On {}, can't parse corporate action '{}'"
            raise ValueError(msg.format(dateTime, memo))

        # Try to extract SecurityId data from CorporateAction memo
        matchgroups = match.groupdict()
        ticker = matchgroups['ticker']
        secname = matchgroups['secname']
        uniqueid = matchgroups['cusip']
        if validate_cusip(uniqueid):
            uniqueidtype = 'CUSIP'
            # Also do ISIN; why not?
            isin = cusip2isin(uniqueid)
            sec = Security.merge(self.session, uniqueidtype='ISIN',
                                 uniqueid=isin, name=secname, ticker=ticker)
            self.securities[('ISIN', isin)] = sec
        elif validate_isin(uniqueid):
            uniqueidtype = 'ISIN'
        else:
            uniqueidtype = None
        if uniqueidtype:
            sec = Security.merge(self.session, uniqueidtype=uniqueidtype,
                                 uniqueid=uniqueid, name=secname,
                                 ticker=ticker)
            self.securities[(uniqueidtype, uniqueid)] = sec

        pca = ParsedCorpAct(transaction, typ, **matchgroups)
        return pca

    @staticmethod
    def inferCorporateActionType(memo):
        """
        Assign type by matching element 'description' attr to MEMO_SIGNATURES
        """
        err_msg = "Can't infer type of corporate action '{}'".format(memo)
        sig = first_true(MEMO_SIGNATURES,
                         default=ValueError(err_msg),
                         pred=lambda sig: sig[0] in memo)
        return sig[1]

    @staticmethod
    def groupParsedCorporateActions(parsedCorpAct):
        return parsedCorpAct.raw.dttrade, parsedCorpAct.type, parsedCorpAct.memo

    @staticmethod
    def sortParsedCorporateActions(parsedCorpAct):
        return parsedCorpAct.raw.reportdate

    ### CORPORATE ACTION HANDLERS ###
    def treat_as_trade(self, corpActs, memo):  # BM, DW, OR
        return [self.merge_trade(corpAct.raw, memo) for corpAct in corpActs]

    def change_security(self, corpActs, memo):  # IC
        #  Book as Transfer(same account, different securities).
        regex = changeSecurityRE
        match = regex.match(memo)
        return self.merge_reorg(corpActs, match, memo)

    def issue_rights(self, corpActs, memo):  # RI
        match = rightsIssueRE.match(memo)
        matchgroups = match.groupdict()
        # FIXME - tickerFrom could also be ticker1 !
        return [self.merge_spinoff(
            transaction=corpAct.raw,
            securityFrom=self.guess_security(matchgroups['isinFrom'],
                                             matchgroups['tickerFrom']),
            numerator=Decimal(matchgroups['numerator0']),
            denominator=Decimal(matchgroups['denominator0']), memo=memo)
            for corpAct in corpActs]

    def split(self, corpActs, memo):  # FS, RS
        """ """
        match = splitRE.match(memo)

        corpActs = list(corpActs)
        assert len(corpActs) in (1, 2)

        if len(corpActs) == 1:
            # Split without CUSIP change
            return self.merge_split(transaction=corpActs.pop().raw,
                                    numerator=Decimal(match.group('numerator0')),
                                    denominator=Decimal(match.group('denominator0')),
                                    memo=memo)

        elif len(corpActs) == 2:
            # Split with CUSIP change - Book as Transfer
            #
            # Of the pair, the transaction booking in the new security has
            # XML attribute 'symbol' matching the first ticker in the memo.
            # The other transaction books out the old security.
            isinFrom = match.group('isinFrom')

            corpActs = sorted(
                list(corpActs),
                key=lambda x: x.cusip in isinFrom or isinFrom in x.cusip)
            assert corpActs[0].cusip not in isinFrom
            assert corpActs[-1].cusip in isinFrom
            dest, src = [corpAct.raw for corpAct in corpActs]
            return self.merge_security_transfer(src, dest, memo)

    def stock_dividend(self, corpActs, memo):  # SD
        match = stockDividendRE.match(memo)
        assert match

        numerator = Decimal(match.group('numerator0'))
        denominator = Decimal(match.group('denominator0'))
        numerator += denominator
        return [self.merge_split(transaction=corpAct.raw, numerator=numerator,
                                 denominator=denominator, memo=memo)
                for corpAct in corpActs]

    def spinoff(self, corpActs, memo):  # SO
        match = spinoffRE.match(memo)
        if match is None:
            msg = "Couldn't parse memo for spinoff '{}'".format(memo)
            raise ValueError(msg)
        matchgroups = match.groupdict()
        try:
            securityFrom = self.guess_security(matchgroups['isinFrom'],
                                               matchgroups['tickerFrom'])
        except ValueError as e:
            msg = "For spinoff memo '{}': ".format(memo)
            msg += e.args[0]
            raise ValueError(msg)

        return [self.merge_spinoff(
            transaction=corpAct.raw, securityFrom=securityFrom,
            numerator=Decimal(matchgroups['numerator0']),
            denominator=Decimal(matchgroups['denominator0']), memo=memo)
            for corpAct in corpActs]

    def subscribe_rights(self, corpActs, memo):  # SR
        regex = subscribeRE
        match = regex.match(memo)
        src, dest, spinoffs = self._group_reorg(corpActs, match)
        src, dest = src.raw, dest.raw
        security = self.securities[(dest.uniqueidtype, dest.uniqueid)]
        securityFrom = self.securities[(src.uniqueidtype, src.uniqueid)]
        txs = [self.merge_transaction(
            uniqueid=src.fitid, datetime=src.dttrade, type=TransactionType.EXERCISE,
            memo=memo, currency=src.currency, cash=src.total,
            fiaccount=self.account, security=security, units=dest.units,
            securityFrom=securityFrom, unitsFrom=src.units)]

        for spinoff in spinoffs:
            txs.extend(self.merge_spinoff(
                transaction=spinoff.raw, securityFrom=securityFrom,
                numerator=spinoff.units, denominator=-src.units, memo=memo))
        return txs

    def merger(self, corpActs, memo):  # TC
        """ """
        def cashMerger(memo, corpActs):
            match = cashMergerRE.match(memo)
            if match:
                if len(corpActs) != 1:
                    msg = ("More than one CorporateAction "
                           "in a cash merger: {}")
                    raise ValueError(msg.format([c.raw for c in corpActs]))
                return [self.merge_trade(corpActs.pop().raw, memo)]

        def kindMerger(memo, corpActs):
            match = kindMergerRE.match(memo)
            if match:
                return self.merge_reorg(corpActs, match, memo)

        def cashKindMerger(memo, corpActs):
            match = cashAndKindMergerRE.match(memo)
            if match:
                # First process cash proceeds as a return of capital
                cashportions = [corpAct.raw for corpAct in corpActs
                                if corpAct.raw.total != 0]
                assert len(cashportions) == 1
                cashportion = cashportions.pop()
                assert cashportion.total > 0

                txs = [self.merge_retofcap(cashportion, memo)]

                # Then process in-kind merger
                txs.extend(self.merge_reorg(corpActs, match, memo))
                return txs

        txs = cashMerger(memo, corpActs) or kindMerger(memo, corpActs) \
            or cashKindMerger(memo, corpActs)

        if not txs:
            msg = ("flex.reader.FlexStatementReader.merger(): "
                   "Can't parse merger memo: '{}'")
            raise ValueError(msg.format(memo))

    def tender(self, corpActs, memo):  # TO
        match = tenderRE.match(memo)
        assert match

        cashportions = [corpAct for corpAct in corpActs
                        if corpAct.raw.total != 0]
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
            # groupParsedCorporateActions(), contains all the data.
            #
            # Here we extract the cost & open date, and stash it in a dict to
            # be picked up for later processing by merge_reorg().  We assume
            # that the 'TO' series has a dateTime strictly earlier than the
            # 'TC' series, so they'll be sorted in correct time order by
            # groupParsedCorporateActions() and merge_reorg() will have its
            # values ready & waiting from tender().
            assert len(cashportions) == 1
            cashportion = cashportions.pop()
            cashportion = cashportion.raw
            cash = cashportion.total
            assert cash < 0

            src, dest, spinoffs = self._group_reorg(corpActs, match)
            dest = dest.raw
            basis_key = (dest.uniqueidtype, dest.uniqueid)
            assert basis_key not in self._basis_stack
            basis_deferral = CostBasisDeferral(currency=cashportion.currency,
                                               cash=cash,
                                               datetime=cashportion.dttrade)
            self._basis_stack[basis_key] = basis_deferral

        return self.merge_reorg(corpActs, match, memo)

    ### Transaction merge functions ###
    def merge_split(self, transaction, numerator, denominator, memo):
        security = self.securities[(transaction.uniqueidtype,
                                    transaction.uniqueid)]
        return [self.merge_transaction(
            type=TransactionType.SPLIT, fiaccount=self.account, uniqueid=transaction.fitid,
            datetime=transaction.dttrade, memo=memo or transaction.memo,
            security=security, numerator=numerator,
            denominator=denominator, units=transaction.units)]

    def merge_spinoff(self, transaction, securityFrom, numerator, denominator,
                      memo):
        security = self.securities[(transaction.uniqueidtype,
                                    transaction.uniqueid)]
        return [self.merge_transaction(
            type=TransactionType.SPINOFF, fiaccount=self.account, uniqueid=transaction.fitid,
            datetime=transaction.dttrade, memo=memo or transaction.memo,
            security=security, numerator=numerator, denominator=denominator,
            units=transaction.units, securityFrom=securityFrom)]

    def merge_reorg(self, corpActs, match, memo):
        """
        Exhange a security for one or more other securities.

        Use the passed-in regex match to identify the source transaction
        (booking out the old security) and the primary destination security
        (booking in the new security); process this pair as a Transfer.
        Any transactions remaining in the sequence of corpActs are processed
        as Spinoffs from the new destination security.
        """
        src, dest, spinoffs = self._group_reorg(corpActs, match)
        src, dest = src.raw, dest.raw
        txs = [self.merge_security_transfer(src, dest, memo)]

        if spinoffs:
            if len(spinoffs) > 1:
                msg = ("flex.reader.FlexStatementReader.merge_reorg(): "
                       "More than one spinoff {}")
                raise ValueError(msg.format(spinoffs))
            spinoff = spinoffs.pop()
            spinoff = spinoff.raw

            # HACK
            # IBKR processes some rights offerings in two parts -
            # 1) a sequence of type 'TO', booking out the old security,
            #    booking in the contra, and booking out subscription cash;
            # 2) a sequence of type 'TC', booking out the contra, booking in
            #    the new security, and booking in the subscribed-for security.
            # The 'TO' series contains the cash paid for the subscription, but
            # not the units, while the 'TC' series contains the units data but
            # not the cost.  Neither series, as grouped by
            # groupParsedCorporateActions(), contains all the data.
            #
            # Here we pick up the cost & open date from where it was earlier
            # stashed in a dict by tender().  We assume that the 'TO' series
            # has a dateTime strictly earlier than the 'TC' series, so they'll
            # be sorted in correct time order by groupParsedCorporateActions()
            # and merge_reorg() will have its values ready & waiting from
            # tender().
            basis_adj = self._basis_stack.pop((src.uniqueidtype, src.uniqueid),
                                              None)
            if basis_adj:
                assert spinoff.units > 0
                tx = Trade(fitid=spinoff.fitid, dttrade=basis_adj.datetime,
                           memo=memo, uniqueidtype=spinoff.uniqueidtype,
                           uniqueid=spinoff.uniqueid, units=spinoff.units,
                           currency=basis_adj.currency, total=basis_adj.cash,
                           reportdate=spinoff.reportdate, orig_tradeid=None,
                           notes=None)
                txs.append(self.merge_trade(tx))
            else:
                txs.append(self.merge_spinoff(
                    transaction=spinoff,
                    securityFrom=self.securities[(src.uniqueidtype, src.uniqueid)],
                    numerator=spinoff.units, denominator=-src.units, memo=memo)
                )
        return txs

    def merge_security_transfer(self, src, dest, memo):
        """
        Given two transactions which have already been matched, treat them
        as a transformation from one security to another within the same
        fiaccount.
        """
        security = self.securities[(dest.uniqueidtype, dest.uniqueid)]
        securityFrom = self.securities[(src.uniqueidtype, src.uniqueid)]
        return [self.merge_transaction(
            type=TransactionType.TRANSFER, fiaccount=self.account, uniqueid=dest.fitid,
            datetime=dest.dttrade, memo=memo, security=security,
            units=dest.units, fiaccountFrom=self.account,
            securityFrom=securityFrom, unitsFrom=src.units, )]

    ### Functions used by Corporate Action handlers ###
    def guess_security(self, uniqueid, ticker):
        """
        Given a Security.uniqueid and/or ticker, try to look up corresponding
        models.transactions.Security instance from the FlexStatement securities
        list or the database.

        Args: uniqueid - CUSIP or ISIN (type str)
              ticker - type str

        Returns: Security instance
        """
        def lookupDbByUid(uniqueidtype, uniqueid):
            secid = self.session.query(SecurityId)\
                    .filter_by(uniqueidtype=uniqueidtype, uniqueid=uniqueid)\
                    .one_or_none()
            if secid:
                return secid.security

        def lookupSeclistByTicker(ticker):
            hits = [sec for sec in set(self.securities.values())
                    if sec.ticker == ticker]
            assert len(hits) <= 1  # triggers self.multipleMatchErrs
            if hits:
                return hits.pop()

        uniqueidtype = (validate_isin(uniqueid) and 'ISIN') \
            or (validate_cusip(uniqueid) and 'CUSIP') \
            or None

        security = self.securities.get((uniqueidtype, uniqueid), None) \
            or lookupDbByUid(uniqueidtype, uniqueid) \
            or lookupSeclistByTicker(ticker) \
            or self.session.query(Security).filter_by(
                ticker=ticker).one_or_none()

        if not security:
            msg = "Can't find security with uniqueid='{}', ticker='{}'"
            raise ValueError(msg.format(uniqueid, ticker))

        return security

    def _group_reorg(self, corpActs, match):
        """
        Given CorporateActions representing a single reorg, group them into
        source (i.e. security being booked out in the reorg),
        destination (i.e. security being booked in by the reorg),
        and additional in-kind reorg consideration which we treat as spinoffs
        from the destination security.

        Args: corpActs - a sequence of CorporateAction instances
              match - a re.match instance capturing named groups (i.e. one of
                      the flex.regexes applied to a transaction memo)

        Returns: a tuple of (CorporateAction instance for source security,
                             CorporateAction instance for destination security,
                             sequence of spinoff CorporationAction instances)
        """
        # We're going to remove items from the list, so don't touch the
        # original date; operate on a copy
        corpActs = copy(corpActs)

        matchgroups = match.groupdict()
        isinFrom = matchgroups['isinFrom']
        tickerFrom = matchgroups['tickerFrom']
        isinTo0 = matchgroups.get('isinTo0', None)
        tickerTo0 = matchgroups.get('tickerTo0', None)

        def matchFirst(*testFuncs):
            return first_true([first_true(corpActs, pred=testFunc)
                               for testFunc in testFuncs])

        def isinFromTestFunc(ca):
            return ca.cusip in isinFrom or isinFrom in ca.cusip

        def tickerFromTestFunc(ca):
            return ca.ticker == tickerFrom

        src = matchFirst(isinFromTestFunc, tickerFromTestFunc)
        if not src:
            msg = ("Can't find source transaction for {} within {}").format(
                {k: v for k, v in match.groupdict().items() if v},
                [ca.raw for ca in corpActs])
            raise ValueError(msg)
        corpActs.remove(src)

        def isinToTestFunc(ca):
            return (isinTo0 is not None and ca.cusip is not None) \
                    and (ca.cusip in isinTo0 or isinTo0 in ca.cusip)

        def tickerToTestFunc(ca):
            return tickerTo0 is not None and ca.ticker == tickerTo0

        def loneCandidate():
            if len(corpActs) == 1 and corpActs[0].cusip not in 'isinFrom':
                return corpActs[0]

        dest = matchFirst(isinToTestFunc, tickerToTestFunc) or loneCandidate()
        if not dest:
            msg = ("On {}, can't find transaction with CUSIP/ISIN "
                   "or ticker matching destination security "
                   "for corporate action {}").format(src.rawdttrade, src.memo)
            raise ValueError(msg)
        corpActs.remove(dest)

        # Remaining corpActs not matched as src/dest pairs treated as spinoffs
        return src, dest, corpActs

    ###########################################################################
    # OPTIONS EXERCISE/ASSIGNMENT/EXPIRATION
    ###########################################################################
    def doOptionsExercises(self, transactions):
        for tx in transactions:
            security = self.securities[(tx.uniqueidtype, tx.uniqueid)]
            securityFrom = self.securities[(tx.uniqueidtypeFrom,
                                            tx.uniqueidFrom)]
            self.merge_transaction(
                uniqueid=tx.fitid, datetime=tx.dttrade, type=TransactionType.EXERCISE,
                memo=tx.memo, currency=tx.currency, cash=tx.total,
                fiaccount=self.account, security=security, units=tx.units,
                securityFrom=securityFrom, unitsFrom=tx.unitsFrom,
                sort=self.sortForTrade(tx.notes)
            )


###############################################################################
# CORPORATE ACTION ROUTING FUNCTIONS
###############################################################################

# Use OrderedDict instead of dict to control the order in which MEMO_SIGNATURES
# are matched - higher confidence matches toward the front.
# Since 'SPINOFF' is sometimes used in the security name field of temporary
# placeholders (contra CUSIPs), it goes toward the back.
CORPACT_HANDLERS = OrderedDict([
    ('BM', ('BOND MATURITY', 'treat_as_trade')),  # Bond Maturity
    ('SR', ('SUBSCRIBES TO', 'subscribe_rights')),  # Subscribe Rights
    ('IC', ('CUSIP/ISIN CHANGE', 'change_security')),  # Issue Change
    ('OR', ('OVER SUBSCRIBE', 'treat_as_trade')),  # Asset Purchase
    ('RI', ('SUBSCRIBABLE RIGHTS ISSUE', 'issue_rights')),  # Subscribable Rights Issue
    ('SD', ('STOCK DIVIDEND', 'stock_dividend')),  # Stock Dividend
    ('TO', ('TENDERED TO', 'tender')),
    ('DW', ('DELISTED', 'treat_as_trade')),  # Delist Worthless
    ('FS', ('SPLIT', 'split')),  # Forward Split
    ('RS', ('SPLIT', 'split')),  # Reverse Split
    ('TC', ('MERGE', 'merger')),  # Merger
    ('SO', ('SPINOFF', 'spinoff')),  # Spin Off
    ('BC', None),  # Bond Conversion
    ('CA', None),  # Contract Soulte (a type of cash settlement)
    ('CC', None),  # Contact Consolidation
    ('CD', None),  # Cash Dividend
    ('CH', None),  # Choice Dividend
    ('CI', None),  # Convertible Issue
    ('CO', None),  # Contract Spin Off
    ('CP', None),  # Coupon Payment
    ('CS', None),  # Contract Split
    ('CT', None),  # CFD Termination
    ('DI', None),  # Dividend Rights Issue
    ('ED', None),  # Expire Dividend Right
    ('FA', None),  # Fee Allocation
    ('FI', None),  # Issue Forward Split
    ('GV', None),  # Generic Voluntary
    ('HD', None),  # Choice Dividend Delivery
    ('HI', None),  # Choice Dividend Issue
    ('PI', None),  # Share Purchase Issue
    ('PV', None),  # Proxy Vote
    ('TI', None),  # Tender Issue
])

MEMO_SIGNATURES = [(v[0], k) for k, v in CORPACT_HANDLERS.items()
                   if v is not None]

# Additional memo signatures to handle data from before FlexQuery schema
# included 'type' attribute for corporate actions
MEMO_SIGNATURES.append(('ACQUIRED', 'TC'))


###############################################################################
# DATA CONTAINERS
###############################################################################
ParsedCorpAct = namedtuple('ParsedCorpAct', ['raw', 'type', 'ticker',
                                             'cusip', 'secname', 'memo', ])

# HACK - cf. tender(), merge_reorg()
CostBasisDeferral = namedtuple('CostBasisDeferral',
                               ['currency', 'cash', 'datetime'])


###############################################################################
# UTILITIES
###############################################################################
def first_true(iterable, default=False, pred=None):
    """Returns the first true value in the iterable.

    If no true value is found, returns *default*

    If *pred* is not None, returns the first item
    for which pred(item) is true.

    """
    # first_true([a,b,c], x) --> a or b or c or x
    # first_true([a,b], x, f) --> a if f(a) else b if f(b) else x
    return next(filter(pred, iterable), default)


###############################################################################
# CLI SCRIPT
###############################################################################
def main():
    from argparse import ArgumentParser
    from capgains import flex

    argparser = ArgumentParser(description='Parse Flex Query data')
    argparser.add_argument('file', nargs='+', help='XML file(s)')
    argparser.add_argument('--database', '-d', default='sqlite://',
                           help='Database connection')
    argparser.add_argument('--verbose', '-v', action='count', default=0)
    args = argparser.parse_args()

    logLevel = (3 - min(args.verbose, 2)) * 10
    logging.basicConfig(level=logLevel)
    logging.captureWarnings(True)

    # engine = create_engine(args.database, echo=True)
    engine = create_engine(args.database)
    Base.metadata.create_all(bind=engine)

    for file in args.file:
        print(file)
        with sessionmanager(bind=engine) as session:
            statements = flex.read(session, file)
            for stmt in statements:
                # for tx in stmt.transactions:
                    # print(tx)
                session.add_all(stmt.transactions)

    engine.dispose()


if __name__ == '__main__':
    main()
