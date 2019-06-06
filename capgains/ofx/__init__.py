# 3rd party imports
import ofxtools


def read(session, source):
    # Avoid import loop by delaying import until after module initialization
    from capgains.ofx.reader import OfxStatementReader
    from capgains.ofx import ibkr, amtd, etfc, scottrade

    dispatcher = {
        ibkr.BROKERID: ibkr.OfxStatementReader,
        amtd.BROKERID: amtd.OfxStatementReader,
        etfc.BROKERID: etfc.OfxStatementReader,
        scottrade.BROKERID: scottrade.OfxStatementReader,
    }

    ofxtree = ofxtools.OFXTree()
    ofxtree.parse(source)
    ofx = ofxtree.convert()

    transactions = []
    for stmt in ofx.statements:
        # We only want INVSTMTRS
        if not isinstance(stmt, ofxtools.models.INVSTMTRS):
            continue
        acctfrom = stmt.account
        # Look up OfxReader subclass by brokerid
        Reader = dispatcher.get(acctfrom.brokerid, OfxStatementReader)
        # Initialize reader instance with INVSTMTRS, SECLIST
        reader = Reader(session, stmt, ofx.securities)
        reader.read()
        transactions.extend(reader.transactions)
    return transactions
