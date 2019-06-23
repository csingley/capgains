# coding: utf-8
"""CLI front end to import transactions, book to inventory, and report results.


INSTALL
-------
q.v. package README.  The package requires Python v3.7+, SQLAlchemy, the Psycopg2
database adapter, and a running PostgreSQL server.  To import OFX data files, ofxtools
is required; to import Interactive Brokers Flex XML data files, ibflex is required.

CONFIGURE
---------
We look for the config file in ~/.config/capgains/capgains.cfg.  It's in INI format,
and needs to have at least the following sections (with values modified for your
installation):

    [db]
    dialect = postgresql
    driver = psycopg2
    username = user
    password = pass
    host = localhost
    port = 5432
    database = capgainsdb

    [books]
    functional_currency = USD

PREPARATION
-----------
To use this program to report capital gains & ending lots for a period, you will need
to have either:
    * an existing database with complete and accurate transaction data
for all prior periods; or
    * a CSV dump of the prior period ending portfolio.

IMPORT
------
Import the transaction data for the current period, e.g.

    python script.py import /path/to/transaction/files/*.ofx

REPORT
------
With a complete transaction database, reporting capital gains looks like:
    python script.py gains -b <first day of period> -e <first day of next period> /path/to/desired/dumpfile.csv

To report ending positions with a complete transaction database:
    python script.py lots -e <first day of next period> /path/to/desired/dumpfile.csv

If loading from prior period position CSV file:
    python script.py gains -l /path/to/last/lots/dumpfile.csv -b <first day of period> -s <first day of period> -e <first day of next period> /path/to/desired/dumpfile.csv
    python script.py lots -l /path/to/last/lots/dumpfile.csv -s <first day of period> -e <first day of next period> /path/to/desired/dumpfile.csv

Instead of granular reporting at the level of individual lots, the above reports can
be consolidated by security/account by passing the --consolidate/-c option to the CLI.
Probably you want a set of reports including:

    1) Capital gains by lot
    2) Ending lots (tax lot detail)
    3) Ending consolidated lots (investment summary)
"""
# stdlib imports
import argparse
from argparse import ArgumentParser, _SubParsersAction
from datetime import datetime
from typing import Tuple, Sequence, Optional

# 3rd party imports
import sqlalchemy
import tablib


# Local imports
from capgains import models, flex, ofx, CSV, CONFIG
from capgains.inventory import report
from capgains.inventory.api import Portfolio
from capgains.database import Base, sessionmanager


def create_engine():
    """
    """
    engine = sqlalchemy.create_engine(CONFIG.db_uri)
    # Create table metadata here too
    Base.metadata.create_all(bind=engine)
    return engine


def drop_all_tables(args):
    """Just what it says on the tin.  DROP all tables defined by models.

    Args:
        args: argparse.Namespace instance populated with parsed CLI arguments.
    """
    engine = sqlalchemy.create_engine(CONFIG.db_uri)
    print("Dropping all tables on {}...".format(CONFIG.db_uri), end=" ")
    Base.metadata.drop_all(bind=engine)
    print("finished.")


def import_transactions(args: argparse.Namespace) -> Sequence[models.Transaction]:
    """Import securities transactions from OFX/XML/CSV datafile; persist to DB.

    Args:
        args: argparse.Namespace instance populated with parsed CLI arguments.
    """
    engine = create_engine()

    output: list = []
    EXTMAP = {"ofx": ofx.read, "qfx": ofx.read, "xml": flex.read, "csv": CSV.read}
    with sessionmanager(bind=engine) as session:
        for path in args.file:
            # Dispatch file according to file extension
            ext = path.split(".")[-1].lower()
            readfn = EXTMAP.get(ext, None)
            if readfn is None:
                raise ValueError("")
            print(path)
            transactions = readfn(session, path)
            session.add_all(transactions)
            output.extend(transactions)
            session.commit()
    return output


def dump_lots(args: argparse.Namespace) -> None:
    """Book DB transactions matching CLI args to inventory; write ending Lots to disk.

    Args:
        args: argparse.Namespace instance populated with parsed CLI arguments.
    """
    engine = create_engine()
    dump_csv(
        engine,
        dtstart=args.dtstart,
        dtend=args.dtend,
        dtstart_gains=datetime.max,
        consolidate=args.consolidate,
        lotloadfile=args.loadcsv,
        lotdumpfile=args.file,
    )


def dump_gains(args: argparse.Namespace) -> None:
    """Book DB transactions matching CLI args to inventory; write Gains to disk.

    Args:
        args: argparse.Namespace instance populated with parsed CLI arguments.
    """
    engine = create_engine()
    dump_csv(
        engine,
        dtstart=args.dtstart,
        dtend=args.dtend,
        dtstart_gains=args.begin,
        consolidate=args.consolidate,
        lotloadfile=args.loadcsv,
        gaindumpfile=args.file,
    )


def dump_csv(
    engine: sqlalchemy.engine,
    dtstart: datetime,
    dtend: datetime,
    dtstart_gains: Optional[datetime] = None,
    consolidate: Optional[bool] = False,
    lotloadfile: Optional[str] = None,
    lotdumpfile: Optional[str] = None,
    gaindumpfile: Optional[str] = None,
) -> None:
    """
    Args:
        engine: a sqlalchemy.engine.Engine instance representing a database connection.
        dtstart: book Transactions occurring on/after this date/time
                 (if None, book from beginning of Transactions).
        dtend: book Transactions occurring before this date/time
               (if None, book through end of Transactions).
        dtstart_gains: report Gains ockcurring on or after this date/time
                       (if None, report all Gains).
        consolidate: if True, consolidate output Lots by (FiAccount, Security);
                     consolidate output Gains by (Security).
        lotloadfile: if set, path to file holding serialized begin portfolio positions.
        lotdumpfile: if set, path to write file of serialized end portfolio positions.
        gaindumpfile: if set, path to write file of seralized realized gains.
    """
    dtstart_gains = dtstart_gains or datetime.min

    with sessionmanager(bind=engine) as session:
        portfolio = load_portfolio(session, lotloadfile)

        transactions = models.Transaction.between(
            session, dtstart=dtstart or datetime.min, dtend=dtend or datetime.max
        )

        gains = [portfolio.book(tx) for tx in transactions]

        if gaindumpfile:
            # Flatten nested list; filter for gains during reporting period
            gains_ = [
                gain
                for gs in gains
                for gain in gs
                if gain.transaction.datetime >= dtstart_gains
            ]
            gains_dataset = report.flatten_gains(session, gains_, consolidate=consolidate)
            with open(gaindumpfile, "w") as csvfile:
                csvfile.write(gains_dataset.csv)

        if lotdumpfile:
            lots_dataset = report.flatten_portfolio(
                portfolio, consolidate=consolidate
            )
            with open(lotdumpfile, "w") as csvfile:
                csvfile.write(lots_dataset.csv)


def load_portfolio(
    session: sqlalchemy.orm.session.Session, path: Optional[str]
) -> Portfolio:
    """Deserialize starting portfolio positions from saved dumpfile.

    Args:
        session: a sqlalchemy.Session instance bound to a database engine.
        path: filesystem path to Lot dumpfile.
    """
    portfolio = Portfolio()
    if not path:
        return portfolio

    with open(path, "r") as csvfile:
        data = tablib.Dataset().load(csvfile.read())

    return report.unflatten_portfolio(session, data)


def make_argparser() -> Tuple[ArgumentParser, _SubParsersAction]:
    """Return subparsers along with the ArgumentParer, so the latter can be extended.
    """
    argparser = ArgumentParser(description="Lot utility")
    # argparser.add_argument('--verbose', '-v', action='count',
    # help='-vv for DEBUG')
    argparser.set_defaults(func=None)
    subparsers = argparser.add_subparsers()

    drop_parser = subparsers.add_parser(
        "drop", aliases=["erase"], help="Drop all database tables"
    )
    drop_parser.set_defaults(func=drop_all_tables)

    import_parser = subparsers.add_parser("import", help="Import OFX/Flex/CSV data")
    import_parser.add_argument("file", nargs="+", help="Broker data file(s)")
    import_parser.set_defaults(func=import_transactions)

    dump_parser = subparsers.add_parser(
        "lots", aliases=["dump"], help="Dump Lots to CSV file"
    )
    dump_parser.add_argument("file", help="CSV file")
    dump_parser.add_argument(
        "-s",
        "--dtstart",
        default=None,
        help=("Start date for Transactions processed " "for Lot report (included)"),
    )
    dump_parser.add_argument(
        "-e",
        "--dtend",
        default=None,
        help=("End date for Transactions processed " "for Lot report (excluded)"),
    )
    dump_parser.add_argument(
        "-l", "--loadcsv", default=None, help="CSV dump file of Lots to load"
    )
    dump_parser.add_argument("-c", "--consolidate", action="store_true")
    dump_parser.set_defaults(func=dump_lots, loadcsv=None)

    gain_parser = subparsers.add_parser("gains", help="Dump Gains to CSV file")
    gain_parser.add_argument("file", help="CSV file")
    gain_parser.add_argument(
        "-s",
        "--dtstart",
        default=None,
        help=("Start date for Transactions processed " "for Gain report (included)"),
    )
    gain_parser.add_argument(
        "-e",
        "--dtend",
        default=None,
        help=("End date for Transactions processed " "for Gain report (excluded)"),
    )
    gain_parser.add_argument(
        "-b",
        "--begin",
        default=None,
        help="Start date for Gain report period (included)",
    )
    gain_parser.add_argument(
        "-l", "--loadcsv", default=None, help="CSV dump file of Lots to load"
    )
    gain_parser.add_argument("-c", "--consolidate", action="store_true")
    gain_parser.set_defaults(func=dump_gains)

    return argparser, subparsers


def run(argparser: ArgumentParser) -> None:
    """Parse args and pass them to the indication function.

    Args:
        argparser: the ArgumentParser instance returned by make_argparser().
    """
    args = argparser.parse_args()

    # Parse datetime args
    if getattr(args, "dtstart", None):
        args.dtstart = datetime.strptime(args.dtstart, "%Y-%m-%d")

    if getattr(args, "dtend", None):
        args.dtend = datetime.strptime(args.dtend, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59
        )

    if getattr(args, "begin", None):
        args.begin = datetime.strptime(args.begin, "%Y-%m-%d")

    # Execute selected function
    if args.func:
        args.func(args)
    else:
        argparser.print_help()


def main() -> None:
    argparser, subparsers = make_argparser()
    run(argparser)


if __name__ == "__main__":
    main()
