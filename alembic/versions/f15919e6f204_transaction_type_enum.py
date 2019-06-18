"""Transaction type enum

Revision ID: f15919e6f204
Revises: 029f05e90aa3
Create Date: 2019-06-12 19:21:25.488847

"""
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'f15919e6f204'
down_revision = '029f05e90aa3'
branch_labels = None
depends_on = None


OLD_ENUM = ('returnofcapital', 'split', 'spinoff', 'transfer', 'trade', 'exercise')
NEW_ENUM = ('RETURNCAP', 'SPLIT', 'SPINOFF', 'TRANSFER', 'TRADE', 'EXERCISE')


ENUM_UPGRADE = """
CREATE OR REPLACE FUNCTION enum_upgrade(current_enum character varying)
RETURNS character varying AS
$BODY$
SELECT CASE
WHEN current_enum = 'returnofcapital' THEN
    'RETURNCAP'
ELSE
    UPPER(current_enum)
END;
$BODY$
LANGUAGE sql IMMUTABLE;
"""


ENUM_DOWNGRADE = """
CREATE OR REPLACE FUNCTION enum_downgrade(current_enum character varying)
RETURNS character varying AS
$BODY$
SELECT CASE
    WHEN current_enum = 'RETURNCAP' THEN
        'returnofcapital'
    ELSE
        LOWER(current_enum)
END;
$BODY$
LANGUAGE sql IMMUTABLE;
"""


def upgrade():
    # Rename current transaction.type enum
    op.execute("ALTER TYPE transaction_type RENAME TO tmp_transaction_type")
    # Create desired transaction.type enum
    desired_enum_type = postgresql.ENUM(*NEW_ENUM, name='transaction_type')
    connection = op.get_bind()
    desired_enum_type.create(connection, checkfirst=False)
    op.execute(ENUM_UPGRADE)
    # Alter transaction.type to use desired enum
    unwanted_enum_type = postgresql.ENUM(*OLD_ENUM, name='tmp_transaction_type')
    op.alter_column(
        'transaction', 'type',
        existing_type=unwanted_enum_type,
        type_=desired_enum_type,
        postgresql_using="enum_upgrade(type::varchar)::transaction_type",
        comment="One of ('RETURNCAP', 'SPLIT', 'SPINOFF', 'TRANSFER', 'TRADE', 'EXERCISE')"
    )
    # Clean up after ourselves
    unwanted_enum_type.drop(connection)
    op.execute("DROP FUNCTION enum_upgrade")

    op.alter_column('transaction', 'datetime',
                    comment='Effective date/time: ex-date for reorgs, return of capital')
    op.alter_column('transaction', 'denominator',
                    comment='For splits, spinoffs: normalized units of source security')
    op.alter_column('transaction', 'dtsettle',
                    comment='Settlement date: pay date for return of capital')
    op.alter_column('transaction', 'fiaccount_id',
                    comment='Financial institution account (for transfers, destination FI account) - FK fiaccount.id')
    op.alter_column('transaction', 'fiaccountfrom_id',
                    comment='For transfers: source FI account (FK fiaccount.id)')
    op.alter_column('transaction', 'numerator',
                    comment='For splits, spinoffs: normalized units of destination security')
    op.alter_column('transaction', 'security_id',
                    comment='FK security.id')
    op.alter_column('transaction', 'securityfrom_id',
                    comment='For transfers, spinoffs, exercise: source security (FK security.id)')
    op.alter_column('transaction', 'securityfromprice',
                    comment='For spinoffs: unit price used to fair-value source security')
    op.alter_column('transaction', 'securityprice',
                    comment='For spinoffs: unit price used to fair-value destination security')
    op.alter_column('transaction', 'sort',
                    comment='Sort algorithm for gain recognition')
    op.alter_column('transaction', 'uniqueid',
                    comment='FI transaction unique identifier')
    op.alter_column('transaction', 'units',
                    comment='Change in shares, contracts, etc. caused by Transaction (for splits, transfers, exercise: destination security change in units)')
    op.alter_column('transaction', 'unitsfrom',
                    comment='For splits, transfers, exercise: source security change in units')
    op.create_table_comment('transaction', 'Securities Transactions')

    op.alter_column('currencyrate', 'fromcurrency',
                    comment='Currency of exchange rate denominator (ISO4217)')
    op.alter_column('currencyrate', 'rate',
                    comment='Multiply this rate by fromcurrency amount to yield tocurrency amount')
    op.alter_column('currencyrate', 'tocurrency',
                    comment='Currency of exchange rate numerator (ISO417)')
    op.create_table_comment('currencyrate', 'Exchange Rates for Currency Pairs')

    op.alter_column('fi', 'brokerid',
                    comment='OFX <INVACCTFROM><BROKERID> value')
    op.create_table_comment('fi', 'Financial Institution (e.g. Brokerage)')

    op.alter_column('fiaccount', 'fi_id',
                    comment='Financial institution (FK fi.id)')
    op.alter_column('fiaccount', 'number',
                    comment='account# (OFX <INVACCTFROM><ACCTID> value')
    op.create_table_comment('fiaccount', 'Financial Institution (e.g. Brokerage) Account')

    op.alter_column('securityid', 'security_id',
                    comment='FK security.id')
    op.alter_column('securityid', 'uniqueid',
                    comment='CUSIP, ISIN, etc.')
    op.alter_column('securityid', 'uniqueidtype',
                    comment='CUSIP, ISIN, etc.')
    op.create_table_comment('securityid', 'Unique Identifiers for Securities')


def downgrade():
    # Rename current transaction.type enum
    op.execute("ALTER TYPE transaction_type RENAME TO tmp_transaction_type")
    # Create desired transaction.type enum
    desired_enum_type = postgresql.ENUM(*OLD_ENUM, name='transaction_type')
    connection = op.get_bind()
    desired_enum_type.create(connection, checkfirst=False)
    op.execute(ENUM_DOWNGRADE)
    # Alter transaction.type to use desired enum
    unwanted_enum_type = postgresql.ENUM(*NEW_ENUM, name='tmp_transaction_type')
    op.alter_column(
        'transaction', 'type',
        existing_type=unwanted_enum_type,
        type_=desired_enum_type,
        postgresql_using="enum_downgrade(type::varchar)::transaction_type",
        comment=None)
    # Clean up after ourselves
    unwanted_enum_type.drop(connection)
    op.execute("DROP FUNCTION enum_downgrade")

    op.drop_table_comment('transaction')
    op.alter_column('transaction', 'unitsfrom', comment=None)
    op.alter_column('transaction', 'units', comment=None)
    op.alter_column('transaction', 'uniqueid', comment=None)
    op.alter_column('transaction', 'type', comment=None)
    op.alter_column('transaction', 'sort', comment=None)
    op.alter_column('transaction', 'securityprice', comment=None)
    op.alter_column('transaction', 'securityfromprice', comment=None)
    op.alter_column('transaction', 'securityfrom_id', comment=None)
    op.alter_column('transaction', 'security_id', comment=None)
    op.alter_column('transaction', 'numerator', comment=None)
    op.alter_column('transaction', 'fiaccountfrom_id', comment=None)
    op.alter_column('transaction', 'fiaccount_id', comment=None)
    op.alter_column('transaction', 'dtsettle', comment=None)
    op.alter_column('transaction', 'denominator', comment=None)
    op.alter_column('transaction', 'datetime', comment=None)

    op.drop_table_comment('securityid')
    op.alter_column('securityid', 'uniqueidtype', comment=None)
    op.alter_column('securityid', 'uniqueid', comment=None)
    op.alter_column('securityid', 'security_id', comment=None)

    op.drop_table_comment('fiaccount')
    op.alter_column('fiaccount', 'number', comment=None)
    op.alter_column('fiaccount', 'fi_id', comment=None)

    op.drop_table_comment('fi')
    op.alter_column('fi', 'brokerid', comment=None)

    op.drop_table_comment('currencyrate')
    op.alter_column('currencyrate', 'tocurrency', comment=None)
    op.alter_column('currencyrate', 'rate', comment=None)
    op.alter_column('currencyrate', 'fromcurrency', comment=None)
