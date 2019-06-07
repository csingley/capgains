# coding: utf-8
""" Reusable test elements """
# stdlib imports
import inspect
import xml.etree.ElementTree as ET


# 3rd party imports
#  import sqlalchemy
from sqlalchemy import create_engine
import ibflex
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


class XmlSnippetMixin(RollbackMixin):
    txs_entry_point = NotImplemented  # Name of main function (type str)
    xml = NotImplemented

    @property
    def persisted_txs(self):
        # Implement in subclass as sequence of inventory.Transaction instances
        # Needs to be instance property so that the class attributes created
        # by setupClass() are available
        raise NotImplementedError

    @classmethod
    def setUpClass(cls):
        super(XmlSnippetMixin, cls).setUpClass()
        # Manually set up fake account; save copy as class attribute.
        cls.fi = models.Fi.merge(
            cls.session, brokerid="4705", name="Dewey Cheatham & Howe"
        )
        cls.account = models.FiAccount.merge(
            cls.session, fi=cls.fi, number="5678", name="Test"
        )

        # Manually parse XML transactions with ibflex.
        # Save copies as class attribute.
        elem = ET.fromstring(cls.xml)
        root_tag, xml_items = ibflex.parser.parse_list(elem)
        parse_method = {
            "Trades": flex.parser.parse_trades,
            "OptionEAE": flex.parser.parse_optionEAE,
        }.get(root_tag) or flex.parser.SUBPARSERS[root_tag]
        cls.parsed_txs = parse_method(
            xml_items
        )  # sequence of flex.parser data containers

        # Manually set up FlexStatementReader with fake account/securities
        # (can't call read() b/c our XML snippet only contains transactions,
        # not account/securities etc.)
        cls.reader = flex.reader.FlexStatementReader(cls.session)
        cls.reader.account = cls.account

        cls.securities = []  # Save copies as class attribute
        for tx in xml_items:
            conid = tx["conid"]
            ticker = tx["symbol"]
            sec = models.Security.merge(
                cls.session, ticker=ticker, uniqueidtype="CONID", uniqueid=conid
            )
            cls.reader.securities[("CONID", conid)] = sec
            if sec not in cls.securities:
                cls.securities.append(sec)

        #  If XML dataset doesn't contain needed info for securities
        #  (e.g. for spinoffs), persist it manually so it can be used by
        #  e.g. flex.reader.FlexStatementReader.guess_security()
        if hasattr(cls, "extra_securities"):
            extra_securities = cls.extra_securities
            if extra_securities:
                assert type(extra_securities) in (list, tuple)
                for extra_security in extra_securities:
                    cls.securities.append(
                        models.Security.merge(cls.session, **extra_security))

    def testEndToEnd(self):
        main_fn = getattr(self.reader, self.txs_entry_point)
        main_fn(self.parsed_txs)

        # Don't order_by() Transaction.type, b/c this sort differently
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

    def _testTransaction(self, predicted, actual):
        fields = list(type(predicted)._fields)
        # Don't test `id`, `uniqueid`
        for unwanted in ["id", "uniqueid"]:
            fields.remove(unwanted)

        for field in fields:
            pred_field = getattr(predicted, field)
            act_field = getattr(actual, field)
            if pred_field != act_field:
                msg = "{} differs from {} in field '{}':\n{} != {}"
                self.fail(msg.format(predicted, actual, field, pred_field, act_field))
