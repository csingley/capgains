# coding: utf-8
"""
WORKFLOW
--------
0) If not the initial period, load CSV dump of previous period lots, or restore
   database from dump.
1) Import current period transaction data files.
2) Dump gains, passing in args for:
    * previous period lots CSV file
    * first day of current period
    * first day of next period
3) Dump lots, passing in args for:
    * previous period lots CSV file
    * first day of current period
    * first day of next period
4) Dump lots, passing in args for:
    * consolidate
    * previous period lots CSV file
    * first day of current period
    * first day of next period
5) (optional) pg_dump the transaction data.
"""
# stdlib imports
from argparse import ArgumentParser
from datetime import datetime

# 3rd party imports
import sqlalchemy
from sqlalchemy import and_


# Local imports
from capgains import flex, ofx, CSV, CONFIG
from capgains.database import Base, sessionmanager
from capgains.models.transactions import (Security, Transaction)
from capgains.inventory import Portfolio
from capgains.CSV.local import (CsvLotReader, CsvLotWriter, CsvGainWriter)


def create_engine():
    engine = sqlalchemy.create_engine(CONFIG.db_uri)
    # Create table metadata here too
    Base.metadata.create_all(bind=engine)
    return engine


def drop_all_tables(args):
    engine = sqlalchemy.create_engine(CONFIG.db_uri)
    print("Dropping all tables on {}...".format(CONFIG.db_uri), end=' ')
    Base.metadata.drop_all(bind=engine)
    print("finished.")


def wrapImport(args):
    engine = create_engine()
    paths = args.file
    import_transactions(engine, paths)


def import_transactions(engine, paths):
    output = []
    EXTMAP = {'ofx': ofx.read, 'qfx': ofx.read,
              'xml': flex.read, 'csv': CSV.read}
    with sessionmanager(bind=engine) as session:
        for path in paths:
            # Dispatch file according to file extension
            ext = path.split('.')[-1].lower()
            readfn = EXTMAP.get(ext, None)
            if readfn is None:
                raise ValueError('')
            print(path)
            transactions = readfn(session, path)
            session.add_all(transactions)
            output.extend(transactions)
            session.commit()
    return output


def wrapLots(args):
    engine = create_engine()
    dump_positions(engine, args.loadcsv, args.file, args.dtstart, args.dtend,
                   args.consolidate)


def dump_positions(engine, infile, outfile, dtstart, dtend, consolidate):
    with sessionmanager(bind=engine) as session:
        portfolio, gains = _process_transactions(
            session, dtstart=dtstart, dtend=dtend, begin=None, loadfile=infile)

        with open(outfile, 'w') as csvfile:
            writer = CsvLotWriter(session, csvfile)
            if consolidate:
                fieldnames = writer.fieldnames
                fieldnames.remove('opendt')
                fieldnames.remove('opentxid')
                writer.fieldnames = fieldnames
            writer.writeheader()
            writer.writerows(portfolio, consolidate=consolidate)


def wrapGains(args):
    engine = create_engine()
    dump_gains(engine, args.loadcsv, args.file, args.dtstart, args.dtend,
               args.begin, args.consolidate)


def dump_gains(engine, infile, outfile, dtstart, dtend, begin, consolidate):
    with sessionmanager(bind=engine) as session:
        portfolio, gains = _process_transactions(session, dtstart, dtend,
                                                 begin, infile)
        with open(outfile, 'w') as csvfile:
            writer = CsvGainWriter(session, csvfile)
            writer.writeheader()
            writer.writerows(gains, consolidate=consolidate)


def _process_transactions(session, dtstart=None, dtend=None, begin=None,
                          loadfile=None):
    dtstart = dtstart or datetime.min
    dtend = dtend or datetime.max
    begin = begin or datetime.min

    if loadfile:
        portfolio = load_portfolio(session, loadfile)
    else:
        portfolio = Portfolio()

    transactions = session.query(Transaction).filter(and_(
        Transaction.datetime >= dtstart,
        Transaction.datetime < dtend,)
    ).order_by(Transaction.datetime, Transaction.type, Transaction.uniqueid)

    gains = [portfolio.applyTransaction(tx) for tx in transactions]
    # Flatten nested list; filter for gains during reporting period
    #  gains = [gain for gs in gains for gain in gs]
    gains = [gain for gs in gains for gain in gs
             if gain.transaction.datetime >= begin]

    return portfolio, gains


def load_portfolio(session, path):
    portfolio = Portfolio()
    with open(path, 'r') as csvfile:
        for row in CsvLotReader(session, csvfile):
            # `row` is a generator yielded by CsvLotReader.__next__(),
            # and needs to be evaluated as a tuple.  In Python 3.7+,
            # raising StopIteration here will be reraised as RuntimeError.
            try:
                row = tuple(row)
            except RuntimeError:
                break

            account, security, lot = row
            portfolio[(account, security)].append(lot)
    return portfolio


def make_argparser():
    """
    Return subparsers as well, so that the ArgumentParser can be extended.
    """
    argparser = ArgumentParser(description='Lot utility')
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
    import_parser.set_defaults(func=wrapImport)

    dump_parser = subparsers.add_parser('lots', aliases=['dump'],
                                        help='Dump Lots to CSV file')
    dump_parser.add_argument('file', help='CSV file')
    dump_parser.add_argument('-s', '--dtstart', default=None,
                             help=("Start date for Transactions processed "
                                   "for Lot report (included)"))
    dump_parser.add_argument('-e', '--dtend', default=None,
                             help=("End date for Transactions processed "
                                   "for Lot report (excluded)"))
    dump_parser.add_argument('-l', '--loadcsv', default=None,
                             help="CSV dump file of Lots to load")
    dump_parser.add_argument('-c', '--consolidate', action='store_true')
    dump_parser.set_defaults(func=wrapLots, loadcsv=None)

    gain_parser = subparsers.add_parser('gains', help='Dump Gains to CSV file')
    gain_parser.add_argument('file', help='CSV file')
    gain_parser.add_argument('-s', '--dtstart', default=None,
                             help=("Start date for Transactions processed "
                                   "for Gain report (included)"))
    gain_parser.add_argument('-e', '--dtend', default=None,
                             help=("End date for Transactions processed "
                                   "for Gain report (excluded)"))
    gain_parser.add_argument('-b', '--begin', default=None,
                             help="Start date for Gain report period (included)")
    gain_parser.add_argument('-l', '--loadcsv', default=None,
                             help="CSV dump file of Lots to load")
    gain_parser.add_argument('-c', '--consolidate', action='store_true')
    gain_parser.set_defaults(func=wrapGains)

    return argparser, subparsers


def run(argparser):
    args = argparser.parse_args()

    # Parse datetime args
    if getattr(args, 'dtstart', None):
        args.dtstart = datetime.strptime(args.dtstart, '%Y-%m-%d')

    if getattr(args, 'dtend', None):
        args.dtend = datetime.strptime(args.dtend, '%Y-%m-%d').replace(
            hour=23, minute=59, second=59)

    if getattr(args, 'begin', None):
        args.begin = datetime.strptime(args.begin, '%Y-%m-%d')

    # Execute selected function
    if args.func:
        args.func(args)
    else:
        argparser.print_help()


def main():
    argparser, subparsers = make_argparser()
    run(argparser)


if __name__ == '__main__':
    main()
