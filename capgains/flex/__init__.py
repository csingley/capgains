#  BROKERID must come before importing subpackages, because those
#  packages want to import BROKERID from the flex namespace.
BROKERID = "4705"


# local imports
from . import parser
from . import reader
from . import Types
from . import regexes


def read(session, source):
    statements = parser.parse(source)
    rdr = reader.FlexResponseReader(statements)
    rdr.read(session)
    transactions = []
    for stmt in rdr.statements:
        transactions.extend(stmt.transactions)
    return transactions
