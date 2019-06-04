"""
CSV readers/writers for internal use by capgains,
not to import data from external sources.

This module provides the ability to dump/load Transactions, Lots, and Gains
to & from CSV files.
"""
# stdlib imports
import csv
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
import functools
import itertools


# local imports
from capgains.models.transactions import (
    Transaction, Security, SecurityId, FiAccount)
from capgains import inventory


class CsvTransactionReader(csv.DictReader):
    def __init__(self, session, csvfile):
        self.session = session
        super(CsvTransactionReader, self).__init__(csvfile)

    def read(self):
        return [self.read_row(row) for row in self]

    def read_row(self, row):
        row = {k: v or None for k, v in row.items()}

        self._convert_account(row, 'fiaccount', 'fiaccount')
        self._convert_account(row, 'fiaccountfrom', 'fiaccountFrom')
        self._convert_security(row, 'security', 'security')
        self._convert_security(row, 'securityfrom', 'securityFrom')

        datetimes = (
            ('datetime', 'datetime'),
            ('dtsettle', 'dtsettle'),
        )
        for fromAttr, toAttr in datetimes:
            self._convert_item(
                row, fromAttr, toAttr,
                lambda dt: datetime.strptime(dt, '%Y-%m-%dT%H:%M:%S'))

        decimals = (
            ('securityprice', 'securityPrice'), ('unitsfrom', 'unitsFrom'),
            ('securityfromprice', 'securityFromPrice'),
            ('numerator', 'numerator'), ('denominator', 'denominator'),
            ('cash', 'cash'), ('units', 'units'),
        )

        for fromAttr, toAttr in decimals:
            self._convert_item(row, fromAttr, toAttr, Decimal)

        transaction = Transaction.merge(self.session, **row)
        return transaction

    def _convert_item(self, row, fromAttr, toAttr, fn):
        value = row.pop(fromAttr)
        if value is not None:
            value = fn(value)
        row[toAttr] = value

    def _convert_account(self, row, fromAttr, toAttr):
        attrs = {attr: row.pop('_'.join((fromAttr, attr)))
                 for attr in ('brokerid', 'number')}
        if attrs['brokerid'] is not None:
            row[toAttr] = FiAccount.merge(self.session, **attrs)
        else:
            row[toAttr] = None

    def _convert_security(self, row, fromAttr, toAttr):
        attrs = {attr: row.pop('_'.join((fromAttr, attr)))
                 for attr in ('uniqueidtype', 'uniqueid', 'ticker', 'name')}
        if attrs['uniqueidtype'] is not None:
            row[toAttr] = Security.merge(self.session, **attrs)
        else:
            row[toAttr] = None


class CsvTransactionWriter(csv.DictWriter):
    csvFields = [
        'uniqueid', 'datetime', 'dtsettle', 'type', 'memo', 'currency', 'cash',
        'fiaccount_brokerid', 'fiaccount_number', 'security_uniqueidtype',
        'security_uniqueid', 'security_ticker', 'security_name', 'units',
        'securityprice', 'fiaccountfrom_brokerid', 'fiaccountfrom_number',
        'securityfrom_uniqueidtype', 'securityfrom_uniqueid',
        'securityfrom_ticker', 'securityfrom_name', 'unitsfrom',
        'securityfromprice', 'numerator', 'denominator', 'sort']

    def __init__(self, session, csvfile):
        self.session = session
        self.csvfile = csvfile

        super(CsvTransactionWriter, self).__init__(
            csvfile, self.csvFields, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)

    def writerows(self, transactions):
        """ """
        for transaction in transactions:
            # Mandatory fields
            row = {'uniqueid': transaction.uniqueid,
                   'datetime': transaction.datetime.isoformat(),
                   'type': transaction.type}

            account = transaction.fiaccount
            row.update({'fiaccount_brokerid': account.fi.brokerid,
                        'fiaccount_number': account.number})

            security = transaction.security
            # Prefer any uniqueidtype other than `TICKER`
            security_ids = sorted(security.ids,
                                  key=lambda x: x.uniqueidtype.lower() == 'ticker')
            security_id = security_ids[0]
            security_uniqueidtype = security_id.uniqueidtype
            security_uniqueid = security_id.uniqueid
            security_ticker = security.ticker
            security_name = security.name
            row.update({'security_uniqueidtype': security_uniqueidtype,
                        'security_uniqueid': security_uniqueid,
                        'security_ticker': security_ticker,
                        'security_name': security_name})

            # Optional fields that can be taken as-is
            row.update({
                'memo': transaction.memo, 'currency': transaction.currency,
                'cash': transaction.cash, 'units': transaction.units,
                'securityprice': transaction.securityPrice,
                'unitsfrom': transaction.unitsFrom,
                'securityfromprice': transaction.securityFromPrice,
                'numerator': transaction.numerator,
                'denominator': transaction.denominator,
                'sort': transaction.sort,
            })

            # Optional fields needing preprocessing
            accountFrom = transaction.fiaccountFrom
            if accountFrom is not None:
                row.update({'fiaccountfrom_brokerid': accountFrom.fi.brokerid,
                            'fiaccountfrom_number': accountFrom.number})

            securityFrom = transaction.securityFrom
            if securityFrom is not None:
                securityfrom_id = securityFrom.ids[0]
                securityfrom_uniqueidtype = securityfrom_id.uniqueidtype
                securityfrom_uniqueid = securityfrom_id.uniqueid
                securityfrom_ticker = securityFrom.ticker
                securityfrom_name = securityFrom.name
                row.update({'securityfrom_uniqueidtype': securityfrom_uniqueidtype,
                            'securityfrom_uniqueid': securityfrom_uniqueid,
                            'securityfrom_ticker': securityfrom_ticker,
                            'securityfrom_name': securityfrom_name})

            self.writerow(row)


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
        opentransaction = inventory.Transaction(
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
        #  yield account, security, inventory.Lot(**lot_attrs)
        yield account
        yield security
        yield inventory.Lot(**lot_attrs)



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
        if consolidate:
            def keyfunc(gain):
                return gain.transaction.security.id

            gains.sort(key=keyfunc)
            for k, gs in itertools.groupby(gains, keyfunc):
                #  print(k)
                row = self._gains2row(list(gs))
                self.writerow(row)

        else:
            for gain in gains:
                row = self._gain2row(gain)
                self.writerow(row)

    def _gain2row(self, gain):
        """
        Transform a single Gain into a dict suitable to hand to self.writerow()
        """
        report = inventory.report_gain(self.session, gain)

        # FIXME
        disallowed = None

        row = {'brokerid': report.fiaccount.fi.brokerid,
               'acctid': report.fiaccount.number,
               'ticker': report.security.ticker,
               'secname': report.security.name,
               'gaindt': report.gaintx.datetime,
               'gaintxid': report.gaintx.uniqueid,
               'ltcg': report.longterm, 'opendt': report.opentx.datetime,
               'opentxid': report.opentx.uniqueid, 'units': report.units,
               'proceeds': report.proceeds, 'cost': report.cost,
               'currency': report.currency,
               'realized': report.proceeds - report.cost,
               'disallowed': disallowed}
        return row

    def _gains2row(self, gains):
        """
        Sum a list of Gains and transform into a dict suitable to hand to
        self.writerow()
        """
        reports = [inventory.report_gain(self.session, gain) for gain in gains]

        # input gains have identical security ( itertools.groupby() )
        security = reports[0].security

        # FIXME - can't do currency conversions
        # input gains must have identical currency
        currency = reports[0].currency
        assert all(report.currency == currency for report in reports)

        running_totals = itertools.accumulate(
            reports, lambda r0, r1: inventory.GainReport(
                fiaccount=None, security=security, opentx=None, gaintx=None,
                units=r0.units + r1.units, currency=currency,
                cost=r0.cost + r1.cost, proceeds=r0.proceeds + r1.proceeds,
                longterm=None))
        total = list(running_totals)[-1]

        row = {'brokerid': None, 'acctid': None, 'ticker': security.ticker,
               'secname': security.name,
               'gaindt': None, 'gaintxid': None, 'ltcg': None, 'opendt': None,
               'opentxid': None, 'units': total.units,
               'proceeds': total.proceeds, 'cost': total.cost,
               'currency': currency,
               'realized': total.proceeds - total.cost,
               'disallowed': None}
        return row
