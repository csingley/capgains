# coding: utf-8
"""
"""
# stdlib imports
from argparse import ArgumentParser
from datetime import datetime


# 3rd party imports
import sqlalchemy
from sqlalchemy import and_


# Local imports
from capgains import flex, ofx, CSV
from capgains.database import Base, sessionmanager
from capgains.models.transactions import (FiAccount, Security, Transaction)
from capgains.inventory import Portfolio
from capgains.CSV.local import (CsvLotReader, CsvLotWriter, CsvGainWriter)


def create_engine(args):
    engine = sqlalchemy.create_engine(args.database)
    # Create table metadata here too
    Base.metadata.create_all(bind=engine)
    return engine


def drop_all_tables(args):
    engine = sqlalchemy.create_engine(args.database)
    print("Dropping all tables on {}...".format(args.database), end=' ')
    Base.metadata.drop_all(bind=engine)
    print("finished.")
 

def import_transactions(args):
    engine = create_engine(args)
    with sessionmanager(bind=engine) as session:
        for filename in args.file:
            # Dispatch file according to file extension
            if filename.lower().endswith('.ofx'):
                readfn = ofx.read
            elif filename.lower().endswith('.xml'):
                readfn = flex.read
            elif filename.lower().endswith('.csv'):
                readfn = CSV.read
            else:
                raise ValueError('')
            print(filename)
            transactions = readfn(session, filename)
            session.add_all(transactions)
            session.commit()


def dump_positions(args):
    engine = create_engine(args)
    with sessionmanager(bind=engine) as session:
        portfolio, gains = _process_transactions(session, args.dtstart,
                                                 args.dtend, args.loadcsv)

        with open(args.file, 'w') as csvfile:
            writer = CsvLotWriter(session, csvfile)
            if args.consolidate:
                fieldnames = writer.fieldnames
                fieldnames.remove('opendt')
                fieldnames.remove('opentxid')
                writer.fieldnames = fieldnames
            writer.writeheader()
            writer.writerows(portfolio, consolidate=args.consolidate)


def dump_gains(args):
    engine = create_engine(args)
    with sessionmanager(bind=engine) as session:
        portfolio, gainslist = _process_transactions(session, args.dtstart,
                                                     args.dtend, args.loadcsv)
        with open(args.file, 'w') as csvfile:
            writer = CsvGainWriter(session, csvfile)
            writer.writeheader()
            for gains in gainslist:
                writer.writerows(gains, consolidate=args.consolidate)


def _process_transactions(session, dtstart=None, dtend=None, loadfile=None):
    dtstart = dtstart or datetime.min
    dtend = dtend or datetime.max

    portfolio = Portfolio()

    if loadfile:
        with open(loadfile, 'r') as csvfile:
            for row in CsvLotReader(session, csvfile):
                row = tuple(row)
                # FIXME - what's up with the blank row(s) at the end?
                # Shouldn't StopIteration take care of this?
                if not row:
                    break
                account, security, lot = tuple(row)
                portfolio[(account, security)].append(lot)


    transactions = session.query(Transaction).filter(and_(
        Transaction.datetime >= dtstart,
        Transaction.datetime <= dtend,)
    ).order_by(Transaction.datetime, Transaction.type, Transaction.uniqueid)

    gains = [portfolio.processTransaction(tx) for tx in transactions]

    return portfolio, gains


def make_argparser():
    """
    Return subparsers as well, so that the ArgumentParser can be extended.
    """

    argparser = ArgumentParser(description='Lot utility')
    argparser.add_argument('-d', '--database', default='sqlite://',
                           help='Database connection')
    # argparser.add_argument('--verbose', '-v', action='count',
                           # help='-vv for DEBUG')
    argparser.set_defaults(func=None)
    subparsers = argparser.add_subparsers()

    drop_parser = subparsers.add_parser('drop', aliases=['erase'],
                                        help='Drop all database tables')
    drop_parser.set_defaults(func=drop_all_tables)

    import_parser = subparsers.add_parser('import',
                                          help='Import OFX/Flex/CSV data')
    import_parser.add_argument('file', nargs='+', help='Broker data file(s)')
    import_parser.set_defaults(func=import_transactions)

    dump_parser = subparsers.add_parser('lots', aliases=['dump'],
                                        help='Dump Lots to CSV file')
    dump_parser.add_argument('file', help='CSV file')
    dump_parser.add_argument('-s', '--dtstart', default=None,
                             help=("Start date for Transactions processed "
                                   "for Lot report"))
    dump_parser.add_argument('-e', '--dtend', default=None,
                             help=("End date for Transactions processed "
                                   "for Lot report"))
    dump_parser.add_argument('-l', '--loadcsv', default=None,
                             help="CSV dump file of Lots to load")
    dump_parser.add_argument('-c', '--consolidate', action='store_true')
    dump_parser.set_defaults(func=dump_positions, loadcsv=None)

    gain_parser = subparsers.add_parser('gains', help='Dump Gains to CSV file')
    gain_parser.add_argument('file', help='CSV file')
    gain_parser.add_argument('-s', '--dtstart', default=None,
                             help=("Start date for Transactions processed "
                                   "for Gain report"))
    gain_parser.add_argument('-e', '--dtend', default=None,
                             help=("End date for Transactions processed "
                                   "for Gain report"))
    gain_parser.add_argument('-f', '--from', default=None,
                             help="Beginning of period covered by Gain report")
    gain_parser.add_argument('-t', '--to', default=None,
                             help="End of period covered by Gain report")
    gain_parser.add_argument('-l', '--loadcsv', default=None,
                             help="CSV dump file of Lots to load")
    gain_parser.add_argument('-c', '--consolidate', action='store_true') 
    gain_parser.set_defaults(func=dump_gains)

    return argparser, subparsers


def main():
    argparser, subparsers = make_argparser()
    args = argparser.parse_args()

    # Parse datetime args
    if getattr(args, 'dtstart', None):
        args.dtstart = datetime.strptime(args.dtstart, '%Y-%m-%d')

    if getattr(args, 'dtend', None):
        args.dtend = datetime.strptime(args.dtend, '%Y-%m-%d').replace(
            hour=23, minute=59, second=59)

    # Execute selected function
    if args.func:
        args.func(args)
    else:
        argparser.print_help()


if __name__ == '__main__':
    main()
