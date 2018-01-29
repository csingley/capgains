"""
"""
# stdlib imports
import csv
from decimal import Decimal, ROUND_HALF_UP
from datetime import (datetime, timedelta)
import functools


# local imports
from capgains.models.transactions import (Security, SecurityId, FiAccount)
from capgains.inventory import Transaction, Lot


class CsvLotReader(csv.DictReader):
    def __init__(self, session, csvfile):
        self.session = session
        # Transaction ID for mock Lot.opentransaction
        self.transaction_id = 999999
        super(CsvLotReader, self).__init__(csvfile)

    def __next__(self):
        row = super(CsvLotReader, self).__next__()
        self.transaction_id += 1
        acct_attrs = {attr: row.pop(attr)
                      for attr in ('brokerid', 'acctid')}
        sec_attrs = {attr: row.pop(attr)
                     for attr in ('ticker', 'secname')}
        lot_attrs = {attr: row.pop(attr)
                     for attr in ('units', 'cost', 'currency')}
        # Create mock opentransaction
        opendt = datetime.strptime(row.pop('opendt'), '%Y-%m-%d %H:%M:%S')
        opentxid = row.pop('opentxid')
        opentransaction = Transaction(
            id=self.transaction_id, uniqueid=opentxid, datetime=opendt)

        # Leftovers in row are SecurityId
        for uniqueidtype, uniqueid in row.items():
            if uniqueid:
                security = Security.merge(self.session,
                                          uniqueidtype=uniqueidtype,
                                          uniqueid=uniqueid,
                                          ticker=sec_attrs['ticker'],
                                          name=sec_attrs['secname'])
        account = FiAccount.merge(self.session,
                                  brokerid=acct_attrs['brokerid'],
                                  number=acct_attrs['acctid'])
        lot_attrs['units'] = Decimal(lot_attrs['units'])
        lot_attrs['price'] = Decimal(lot_attrs.pop('cost')) / lot_attrs['units']
        lot_attrs['opentransaction'] = opentransaction
        lot_attrs['createtransaction'] = opentransaction

        # `yield` returns a generator object; if you want to use it directly
        # instead of iterating over it, you need to call list() or tuple()
        # or somesuch.
        #
        #  `yield account, security, lot_attrs` gives one tuple, whereas
        #  `yield account; yield security; yield lot_attrs` gives the
        #  individual objects.  Since we're going to be calling tuple() on
        #  the returned generator object, we'll use the latter format in order
        #  to avoid annoying nested tuples.
        yield account
        yield security
        yield Lot(**lot_attrs)


class CsvLotWriter(csv.DictWriter):
    csvFields = ['brokerid', 'acctid', 'ticker', 'secname', 'opendt',
                 'opentxid', 'units', 'cost', 'currency']

    def __init__(self, session, csvfile):
        self.session = session
        self.csvfile = csvfile

        uniqueidtypes = [d[0] for d in
                         session.query(SecurityId.uniqueidtype).distinct()]
        fieldnames = self.csvFields + uniqueidtypes
        super(CsvLotWriter, self).__init__(
            csvfile, fieldnames, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)

    def writerows(self, portfolio, consolidate=False):
        """ """
        for (account, security), position in portfolio.items():
            if not position:
                continue
            self.session.add_all([account, security])
            position_attrs = {secid.uniqueidtype: secid.uniqueid
                              for secid in security.ids}
            position_attrs.update(
                {'brokerid': account.fi.brokerid, 'acctid': account.number,
                 'ticker': security.ticker, 'secname': security.name}
            )

            rows = self.rows_for_position(position, consolidate=consolidate)
            for row in rows:
                if row['units']:
                    row.update(position_attrs)
                    self.writerow(row)

    def rows_for_position(self, position, consolidate=False):
        rows = [self.row_for_lot(lot, consolidate) for lot in position]
        if consolidate:
            rows = [functools.reduce(self.add_lots, rows)]
        return rows

    def row_for_lot(self, lot, consolidate):
        row = {'units': Decimal(lot.units.quantize(Decimal('0.0001'),
                                                   rounding=ROUND_HALF_UP)),
               'cost': (lot.units * lot.price).quantize(Decimal('0.0001'),
                                                        rounding=ROUND_HALF_UP),
               'currency': lot.currency, }
        if not consolidate:
            row.update({'opendt': lot.opentransaction.datetime,
                        'opentxid': lot.opentransaction.uniqueid})
        return row

    def add_lots(self, lot0, lot1):
        assert lot0['currency'] == lot1['currency']  # FIXME
        lot0['units'] += lot1['units']
        lot0['cost'] += lot1['cost']
        return lot0


class CsvGainWriter(csv.DictWriter):
    fieldnames = ['brokerid', 'acctid', 'ticker', 'secname', 'gaindt',
                  'gaintxid', 'ltcg', 'opendt', 'opentxid', 'units', 'proceeds',
                  'cost', 'currency', 'realized', 'disallowed']

    def __init__(self, session, csvfile):
        self.session = session
        self.csvfile = csvfile
        super(CsvGainWriter, self).__init__(
            csvfile, self.fieldnames, delimiter=',',
            quoting=csv.QUOTE_NONNUMERIC)

    def writerows(self, gains, consolidate=False):
        """ """
        for gain in gains:
            account = gain.transaction.fiaccount
            security = gain.transaction.security
            lot = gain.lot
            units = lot.units
            proceeds = units * gain.price
            cost = units * lot.price
            gaindt = gain.transaction.datetime
            opendt = lot.opentransaction.datetime
            ltcg = (units > 0) and (gaindt - opendt >= timedelta(days=366))
            disallowed = None  # FIXME
            row = {'brokerid': account.fi.brokerid, 'acctid': account.number,
                   'ticker': security.ticker, 'secname': security.name,
                   'gaindt': gaindt, 'gaintxid': gain.transaction.uniqueid,
                   'ltcg': ltcg, 'opendt': opendt,
                   'opentxid': lot.opentransaction.uniqueid, 'units': units,
                   'proceeds': proceeds, 'cost': cost,
                   'currency': gain.transaction.currency, 'realized': proceeds - cost,
                   'disallowed': disallowed}

            self.writerow(row)

    def rows_for_position(self, position, consolidate=False):
        rows = [{'units': Decimal(lot.units.quantize(Decimal('0.0001'),
                                                     rounding=ROUND_HALF_UP)),
                 'opendt': lot.opendt, 'cost': lot.units * lot.unitcost,
                 'currency': lot.currency, } for lot in position]
        if consolidate:
            rows = [functools.reduce(self.add_lots, rows)]
        return rows

    def add_lots(self, lot0, lot1):
        assert lot0['currency'] == lot1['currency']  # FIXME
        lot0['units'] += lot1['units']
        lot0['cost'] += lot1['cost']
        lot0['opendt'] = None
        return lot0
