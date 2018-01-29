# coding: utf-8
"""
"""
# stdlib imports
import re


# 3rd party imports
import ofxtools


# Local imports


class IBKR(object):
    transferMemoRE = re.compile(
        r"""
        (?P<memo>.+)
        \s+
        \( (?P<ticker>.+), \s+ (?P<secname>.+), \s+ (?P<uniqueid>[\w]+) \)
        """, re.VERBOSE | re.IGNORECASE
    )

    retofcapMemoRE = re.compile(
        r"""
        (?P<memo>.+)
        \s+
        \(Return\ of\ Capital\)
        """, re.VERBOSE | re.IGNORECASE
    )

    @classmethod
    def doTransfer(cls, account, invtran):
        """ """
        # This method only works on TRANSFER
        if not isinstance(invtran, ofxtools.models.TRANSFER):
            raise ValueError('%s is not a transfer' % invtran)

        # First check if we've already processed this transaction
        t = Transaction.query.filter_by(
            account=account, id=invtran.fitid
        ).one_or_none()
        if t:
            return

        return OfxImporter.doTransfer(account, invtran)

        match = cls.transferMemoRE.match(invtran.memo)
        # Sanity check memo regex match
        assert match
        #assert match.group('ticker') == invtran.secinfo.ticker
        assert match.group('uniqueid') == invtran.secinfo.uniqueid
        memo = match.group('memo')
        assert memo

        twin = TRANSFER.query.filter(
            TRANSFER.id != invtran.id, TRANSFER.acctfrom == invtran.acctfrom,
            TRANSFER.dttrade == invtran.dttrade, TRANSFER.memo.like(memo+'%'),
        ).one_or_none()

        if twin:
            t, created = OfxLog.get_or_create(twin) 
            assert created
            # The TRANSFER pair should have opposite signs
            assert invtran.units * twin.units < 0

            # Which TRANSFER (passed in or looked-up twin) corresponds to
            # a security for which we already own Lots?
            security = invtran.secinfo
            lots = Lot.asOf(invtran.dttrade,
                            account=invtran.acctfrom, security=security).all()

            if lots and invtran.units == -sum([lot.units for lot in lots]):
                units = sum([lot.units for lot in lots])
                transferOut, transferIn = invtran, twin
                newSecurity = twin.secinfo
            else:
                # Do we own the looked-up twin TRANSFER?
                newSecurity = invtran.secinfo 
                security = twin.secinfo
                lots = Lot.asOf(invtran.dttrade,
                                account=invtran.acctfrom, security=security
                               ).all()
                if not lots:
                    # We don't own either side of the pair; ignore it
                    return
                units = sum([lot.units for lot in lots])
                try:
                    assert units == -twin.units
                except AssertionError:
                    logging.critical("Inventory Lot: ticker=%s, uniqueid=%s, units=%s; incoming units=%s from transaction dttrade=%s fitid=%s" % (twin.secinfo.ticker, twin.secinfo.uniqueid, units, twin.units, twin.dttrade, twin.fitid))
                    raise
                transferOut, transferIn = twin, invtran

            ratio = transferIn.units / units
            for lot in lots:
                lot.ender = transferOut
                lot.dtend = invtran.dttrade
                newLot = Lot(
                    account=lot.account, security=newSecurity,
                    units=lot.units * ratio, cost=lot.cost,
                    washcost=lot.washcost,
                    dtopen=lot.dtopen, opener=lot.opener,
                    dtstart=invtran.dttrade, starter=transferIn,
                    predecessor=lot,
                )
                Session.add(newLot)

    @classmethod
    def doIncome(cls, account, invtran):
        """ """
        # This method only works on INCOME
        if not isinstance(invtran, ofxtools.models.INCOME):
            raise ValueError('%s is not an income' % invtran)

        return OfxImporter.doIncome(account, invtran)

        # First check if we've already processed this transaction
        t, created = OfxLog.get_or_create(invtran)
        if not created:
            return
        
        # IBKR books return of capital as INCOME rather than RETOFCAP,
        # only noting this classification in the memo field
        if 'return of capital' not in invtran.memo.lower():
            return
        match = cls.retofcapMemoRE.match(invtran.memo)
        assert match
        memo = match.group('memo')

        # Before changing Lot cost basis, check to see that the return of
        # capital hasn't been reversed - this happens often as the broker
        # first books part of the cash as payment in lieu, then later sorts
        # out the hypothecation and rebooks as return of  capital.
        #
        # INCOME transactions get reversed as INVEXPENSE, so check those
        # for matching date/total/memo.
        reversal = INVEXPENSE.query.filter(
            INVEXPENSE.dttrade == invtran.dttrade,
            INVEXPENSE.total == -invtran.total,
            INVEXPENSE.memo.like(memo+'%'),
        ).order_by(INVEXPENSE.id).first()
        if reversal:
            t, created = OfxLog.get_or_create(reversal)
            return

        # Process this INCOME as a RETOFCAP.
        # We created an OfxLog instance above for invtran;
        # disable OfxLog checks in Lot.returnOfCapital() (which would
        # flag our INCOME as already processed)
        Lot.returnOfCapital(invtran, checklog=False)

    @classmethod
    def doCash(cls, account, invbanktran):
        """ """
        return OfxImporter.doCash(account, invtran)


brokerquirks = {
    '4705': {ofxtools.models.TRANSFER: IBKR.doTransfer,
             ofxtools.models.INCOME: IBKR.doIncome,
             ofxtools.models.INVBANKTRAN: IBKR.doCash,
            },
}


#def ofximport(args):
    ##from .database import init_db
    #from database import init_db
    #init_db(args.database)

    #for ofxfile in args.file:
        #with sessionmanager() as session:
            #ofximporter = OfxImporter(ofxfile)
            #ofximporter.load()


#def main():
    #from argparse import ArgumentParser

    #argparser = ArgumentParser(description='Import OFX file(s)')
    #argparser.add_argument('-d', '--database', default='sqlite://',
                           #help='Database connection')
    #argparser.add_argument('--verbose', '-v', action='count',
                           #help='-vv for DEBUG')
    #argparser.add_argument('file', nargs='+', help='OFX file(s)')

    #args = argparser.parse_args()

    ## Set logging level
    #loglevel = {1: getattr(logging, 'INFO'), 2: getattr(logging, 'DEBUG')}.get(
        #args.verbose, getattr(logging, 'WARNING')
    #)
    #logging.basicConfig(level=loglevel)

    ## Execute 
    #ofximport(args)


#if __name__ == '__main__':
    #main()
