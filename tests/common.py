# coding: utf-8
""" Reusable test elements """
# stdlib imports
import inspect
import xml.etree.ElementTree as ET
import io


# 3rd party imports
#  import sqlalchemy
from sqlalchemy import create_engine
import ofxtools


# local imports
from capgains.config import CONFIG
from capgains import database, flex, ofx, models


DB_URI = CONFIG.test_db_uri
DB_STATE = {
    "engine": create_engine(DB_URI),
    "connection": None,
    "transaction": None,
    "session": None,
}


def logPoint(context):
    """" Utility function to trace control flow """
    callingFunction = inspect.stack()[1][3]
    print("in %s - %s()" % (context, callingFunction))


def setUpModule():
    """
    Called once, before anything else in this module

    http://docs.sqlalchemy.org/en/latest/orm/session_transaction.html
    http://alextechrants.blogspot.com/2013/08/unit-testing-sqlalchemy-apps.html
    """
    #  logPoint('module {}'.format(__name__))

    # Create a connection and start a transaction. This is needed so that
    # we can run the drop_all/create_all inside the same transaction as
    # the tests
    connection = DB_STATE["engine"].connect()
    transaction = connection.begin()
    session = database.Session(bind=connection)

    DB_STATE["connection"] = connection
    DB_STATE["transaction"] = transaction
    DB_STATE["session"] = session

    # Drop all to get an empty database free of old crud just in case
    database.Base.metadata.drop_all(transaction.connection)

    # Create everything
    database.Base.metadata.create_all(transaction.connection)


def tearDownModule():
    """ Called once, after everything else in this module """
    #  logPoint('module {}'.format(__name__))
    # Roll back everything
    DB_STATE["transaction"].rollback()

    # Disconnect from the database
    DB_STATE["connection"].close()


class RollbackMixin(object):
    """ Mixin to roll back database changes during test """

    @classmethod
    def setUpClass(cls):
        """ Called once, before any tests """
        #  logPoint('class {}'.format(cls.__name__))
        cls.session = DB_STATE["session"]
        # Create class savepoint
        cls.savepoint = cls.session.begin_nested()

    @classmethod
    def tearDownClass(cls):
        """ Called once, after all tests, if cls.setUpClass successful """
        #  logPoint('class {}'.format(cls.__name__))
        # Roll back to class savepoint
        cls.savepoint.rollback()

    def setUp(self):
        """ Called multiple times, before every test method """
        #  self.logPoint()
        self.savepoint_inner = self.session.begin_nested()

    def tearDown(self):
        """ Called multiple times, after every test method """
        self.savepoint_inner.rollback()

    def logPoint(self):
        """ Utility method to trace control flow """
        callingFunction = inspect.stack()[1][3]
        currentTest = self.id().split(".")[-1]
        print("in {} - {}()".format(currentTest, callingFunction))


class OfxSnippetMixin(RollbackMixin):
    readerclass = ofx.reader.OfxStatementReader
    ofx = NotImplemented

    @classmethod
    def setUpClass(cls):
        super(OfxSnippetMixin, cls).setUpClass()

        treebuilder = ofxtools.Parser.TreeBuilder()
        treebuilder.feed(cls.ofx)
        cls.parsed_txs = ofxtools.models.base.Aggregate.from_etree(treebuilder.close())

        cls.reader = cls.readerclass(cls.session)

        # Manually set up fake account; save copy as class attribute.
        cls.fi = models.Fi.merge(
            cls.session, brokerid="4705", name="Dewey Cheatham & Howe"
        )
        cls.account = models.FiAccount.merge(
            cls.session, fi=cls.fi, number="5678", name="Test"
        )

        cls.reader.account = cls.account

        cls.securities = []  # Save copies as class attribute
        for tx in cls.parsed_txs:
            uniqueidtype = tx.uniqueidtype
            uniqueid = tx.uniqueid
            sec = models.Security.merge(
                cls.session, uniqueidtype=uniqueidtype, uniqueid=uniqueid
            )
            cls.reader.securities[(uniqueidtype, uniqueid)] = sec
            if sec not in cls.securities:
                cls.securities.append(sec)

        cls.reader.currency_default = "USD"
        #  cls.reader.currency_default = models.Currency.USD


class XmlSnippetMixin:
    stmt_sections = []
    securities_info = []

    @classmethod
    def setUpClass(cls):
        cls._setUpStatement()

    @classmethod
    def _setUpStatement(cls):
        #  Mock up a FlexQueryResponse hierarchy.
        mock_response = ET.Element(
            "FlexQueryResponse",
            attrib={
                "queryName": "Test",
                "type": "FOO",
            }
        )
        stmts = ET.SubElement(
            mock_response,
            "FlexStatements",
            attrib={"count": "1"},
        )
        stmt = ET.SubElement(
            stmts,
            "FlexStatement",
            attrib={
                "accountId": "U12345",
                "fromDate": "20060101",
                "toDate": "20161231",
                "period": "Foobar",
                "whenGenerated": "20170101",
            },
        )
        ET.SubElement(
            stmt,
            "AccountInformation",
            attrib={
                "accountId": "5678",
                "currency": "USD",
            },
        )

        #  Append the transactions under test from the subclass.
        cls.securities_info = []
        for stmt_section in cls.stmt_sections:
            section = ET.fromstring(stmt_section)
            stmt.append(section)
            #  Collect securities info for transactions.
            for tx in section:
                conid = tx.get("conid")
                if conid not in [s["conid"] for s in cls.securities_info]:
                    cls.securities_info.append(
                        {"conid": conid, "symbol": tx.get("symbol")}
                    )

        #  Append securities info for transactions.
        securities_info = ET.SubElement(stmt, "SecuritiesInfo")
        for sec in cls.securities_info:
            ET.SubElement(securities_info, "SecurityInfo", attrib=sec)

        #  Parse mocked-up XML
        source = io.BytesIO(ET.tostring(mock_response))
        stmts = flex.parser.parse(source)
        assert len(stmts) == 1
        cls.statement = stmts[0]


class ReadXmlSnippetMixin(RollbackMixin, XmlSnippetMixin):
    extra_securities = []

    @property
    def persisted_txs(self):
        # Implement in subclass as sequence of inventory.Transaction instances
        # Needs to be instance property so that the class attributes created
        # by setupClass() are available
        raise NotImplementedError

    @classmethod
    def setUpClass(cls):
        super(ReadXmlSnippetMixin, cls).setUpClass()

        cls._setUpStatement()

        # Manually set up fake account; save copy as class attribute.
        cls.fi = models.Fi.merge(
            cls.session, brokerid="4705", name="Dewey Cheatham & Howe"
        )
        cls.account = models.FiAccount.merge(
            cls.session, fi=cls.fi, number="5678", name="Test"
        )

        #  Merge models.Security instances for each & store as class attribute
        #  to compare with reslts of read().
        cls.securities = []

        for sec in cls.securities_info:
            cls.securities.append(
                models.Security.merge(
                    cls.session,
                    ticker=sec["symbol"],
                    uniqueidtype="CONID",
                    uniqueid=sec["conid"],
                )
            )

        #  Merge models.Security instances for any extra securities not part
        #  of transaction data that are needed for reorgs.
        for sec in cls.extra_securities:
            cls.securities.append(
                models.Security.merge(cls.session, **sec)
            )

        cls.reader = flex.reader.FlexStatementReader(
            cls.session,
            statement=cls.statement,
        )
        cls.reader.read()

    def testEndToEnd(self):
        # First test that the transactions made it to the DB correctly.
        # Don't order_by() Transaction.type, b/c this sorts differently
        # under e.g. sqlite (alpha by string) vs. postgresql (enum order)
        txs = (
            self.session.query(models.Transaction)
            .order_by(
                models.Transaction.datetime,
                models.Transaction.memo,
                models.Transaction.cash,
                models.Transaction.units,
            )
            .all()
        )
        predicted_txs = self.persisted_txs
        self.assertEqual(len(predicted_txs), len(txs))

        for predicted, actual in zip(predicted_txs, txs):
            self._testTransaction(predicted, actual)

        #  Sort FlexStatementReader.transactions the same as models.Transaction above.
        #  FIXME there's some weird sorting going on in sqlalchemy order_by, where
        #  some kind of enum ordering trumps the memo sorting.  This is my best guess.
        def sortKey(tx):
            type_ = {
                models.TransactionType.TRADE: 3,
                models.TransactionType.RETURNCAP: 1,
                models.TransactionType.TRANSFER: 5,
                models.TransactionType.SPLIT: 2,
                models.TransactionType.SPINOFF: 6,
                models.TransactionType.EXERCISE: 4,
            }[tx.type]
            return (tx.datetime, type_, tx.memo, tx.cash if tx.cash else 0, tx.units if tx.units else 0)

            #  RETURNCAP = 1
            #  SPLIT = 2
            #  SPINOFF = 3
            #  TRANSFER = 4
            #  TRADE = 5
            #  EXERCISE = 6

        txs = sorted(
            self.reader.transactions,
            key=sortKey,
        )

        for predicted, actual in zip(predicted_txs, txs):
            self._testTransaction(predicted, actual)

    def _testTransaction(self, predicted, actual):
        fields = list(type(predicted)._fields)
        # Don't test uniqueid`
        fields.remove("uniqueid")

        for field in fields:
            pred_value = getattr(predicted, field)
            act_value = getattr(actual, field)
            if pred_value != act_value:
                self.fail(
                    (
                        #  f"\n{type(pred_value)} {type(act_value)}"
                        f"\n{predicted}\ndiffers from\n{actual}\nin field "
                        f"'{field}':\n{pred_value} != {act_value}"
                    )
                )
