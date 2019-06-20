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
from typing import Tuple, MutableMapping

# 3rd party imports
import sqlalchemy
import tablib


# Local imports
from capgains import models, utils, flex, ofx, CSV, CONFIG
from capgains.inventory import report
from capgains.inventory.types import Lot, DummyTransaction
from capgains.inventory.api import Portfolio
from capgains.database import Base, sessionmanager


def create_engine():
    engine = sqlalchemy.create_engine(CONFIG.db_uri)
    # Create table metadata here too
    Base.metadata.create_all(bind=engine)
    return engine


def drop_all_tables(args):
    engine = sqlalchemy.create_engine(CONFIG.db_uri)
    print("Dropping all tables on {}...".format(CONFIG.db_uri), end=" ")
    Base.metadata.drop_all(bind=engine)
    print("finished.")


def import_transactions(args):
    engine = create_engine()

    output = []
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


def dump_lots(args):
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


def dump_gains(args):
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
    engine,
    dtstart,
    dtend,
    dtstart_gains=None,
    consolidate=False,
    lotloadfile=None,
    lotdumpfile=None,
    gaindumpfile=None,
):
    dtstart_gains = dtstart_gains or datetime.min

    with sessionmanager(bind=engine) as session:
        portfolio = load_portfolio(session, lotloadfile)

        transactions = models.Transaction.between(
            session,
            dtstart=dtstart or datetime.min,
            dtend=dtend or datetime.max,
        )

        gains = [portfolio.book(tx) for tx in transactions]
        # Flatten nested list; filter for gains during reporting period
        gains = [
            gain
            for gs in gains
            for gain in gs
            if gain.transaction.datetime >= dtstart_gains
        ]

        if gaindumpfile:
            gains_table = report.flatten_gains(session, gains, consolidate=consolidate)
            with open(gaindumpfile, "w") as csvfile:
                csvfile.write(gains_table.csv)

        if lotdumpfile:
            portfolio_table = report.flatten_portfolio(portfolio, consolidate=consolidate)
            with open(lotdumpfile, "w") as csvfile:
                csvfile.write(portfolio_table.csv)


def load_portfolio(session, path):
    portfolio = Portfolio()
    if not path:
        return portfolio

    with open(path, "r") as csvfile:
        data = tablib.Dataset().load(csvfile.read())

    reports = (load_lot_report(row) for row in data.dict)
    for rept in reports:
        account, security, lot = load_lot(session, rept)
        portfolio[(account, security)].append(lot)

    return portfolio


def load_lot_report(row: MutableMapping) -> report.FlatLot:
    row.update(
        {
            "opendt": datetime.strptime(row["opendt"], "%Y-%m-%d %H:%M:%S"),
            "units": utils.round_decimal(row["units"]),
            "cost": utils.round_decimal(row["cost"]),
            "currency": getattr(models.Currency, row["currency"]),
        }
    )
    return report.FlatLot(**row)


def load_lot(
    session, lotreport: report.FlatLot
) -> Tuple[models.FiAccount, models.Security, Lot]:
    account = models.FiAccount.merge(
        session, brokerid=lotreport.brokerid, number=lotreport.acctid
    )
    assert lotreport.opentxid is not None
    assert lotreport.opendt is not None

    # Create mock opentransaction
    opentransaction = DummyTransaction(
        uniqueid=lotreport.opentxid,
        datetime=lotreport.opendt,
        fiaccount=None,
        security=None,
        type=models.TransactionType.TRADE,
    )

    for uniqueidtype in ("CUSIP", "ISIN", "CONID", "TICKER"):
        uniqueid = getattr(lotreport, uniqueidtype)
        if uniqueid:
            security = models.Security.merge(
                session,
                uniqueidtype=uniqueidtype,
                uniqueid=uniqueid,
                ticker=lotreport.ticker,
                name=lotreport.secname,
            )

    lot = Lot(
        units=lotreport.units,
        price=lotreport.cost / lotreport.units,
        opentransaction=opentransaction,
        createtransaction=opentransaction,
        currency=lotreport.currency,
    )

    return account, security, lot


def make_argparser():
    """
    Return subparsers as well, so that the ArgumentParser can be extended.
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


def run(argparser):
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


def main():
    argparser, subparsers = make_argparser()
    run(argparser)


if __name__ == "__main__":
    main()
