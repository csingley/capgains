BROKERID = '4705'


# local imports
from capgains.flex import parser
from capgains.flex.reader import FlexResponseReader


def read(session, source):
    statements = parser.parse(source)
    reader = FlexResponseReader(session, statements)
    reader.read()
    transactions = []
    for stmt in reader.statements:
        transactions.extend(stmt.transactions)
    return transactions
