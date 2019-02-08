from . import etfc
from . import local


ReaderClasses = [local.CsvTransactionReader, etfc.CsvTransactionReader]


def read(session, filename):
    transactions = None
    for ReaderClass in ReaderClasses:
        if transactions is None:
            try:
                with open(filename) as csvfile:
                    reader = ReaderClass(session, csvfile)
                    transactions = reader.read()
            except:
                continue
    if transactions is None:
        raise ValueError("Can't read CSV file {}".format(filename))
    return transactions
