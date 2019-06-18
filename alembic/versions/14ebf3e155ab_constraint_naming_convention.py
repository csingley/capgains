"""Constraint naming convention

Revision ID: 14ebf3e155ab
Revises: bc10e6480037
Create Date: 2019-06-18 08:51:34.840669

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '14ebf3e155ab'
down_revision = 'bc10e6480037'
branch_labels = None
depends_on = None


TRADE_CONSTRAINT = (
    "type='TRADE' "
    "AND currency IS NOT NULL "
    "AND cash IS NOT NULL "
    "AND units IS NOT NULL "
    "AND securityprice IS NULL "
    "AND fromfiaccount_id IS NULL "
    "AND fromsecurity_id IS NULL "
    "AND fromunits IS NULL "
    "AND fromsecurityprice IS NULL "
    "AND numerator IS NULL "
    "AND denominator IS NULL"
)
ROC_CONSTRAINT = (
    "type='RETURNCAP' "
    "AND currency is NOT NULL "
    "AND cash IS NOT NULL "
    "AND units IS NULL "
    "AND securityprice IS NULL "
    "AND fromfiaccount_id IS NULL "
    "AND fromsecurity_id IS NULL "
    "AND fromunits IS NULL "
    "AND fromunits IS NULL "
    "AND fromsecurityprice IS NULL "
    "AND numerator IS NULL "
    "AND denominator IS NULL"
)
TRANSFER_CONSTRAINT = (
    "type='TRANSFER' "
    "AND units IS NOT NULL "
    "AND fromfiaccount_id IS NOT NULL "
    "AND fromsecurity_id IS NOT NULL "
    "AND fromunits IS NOT NULL "
    "AND currency IS NULL "
    "AND cash IS NULL "
    "AND securityprice IS NULL "
    "AND fromsecurityprice IS NULL "
    "AND numerator IS NULL "
    "AND denominator IS NULL"
)
SPLIT_CONSTRAINT = (
    "type='SPLIT' "
    "AND units IS NOT NULL "
    "AND numerator IS NOT NULL "
    "AND denominator IS NOT NULL "
    "AND currency IS NULL "
    "AND cash is NULL "
    "AND securityprice IS NULL "
    "AND fromsecurityprice IS NULL "
    "AND fromfiaccount_id IS NULL "
    "AND fromsecurity_id IS NULL "
    "AND fromunits IS NULL"
)
SPINOFF_CONSTRAINT = (
    "type='SPINOFF' "
    "AND units IS NOT NULL "
    "AND fromsecurity_id IS NOT NULL "
    "AND numerator IS NOT NULL "
    "AND denominator IS NOT NULL "
    "AND currency IS NULL "
    "AND cash IS NULL "
    "AND fromfiaccount_id IS NULL "
    "AND fromunits IS NULL"
)
EXERCISE_CONSTRAINT = (
    "type='EXERCISE' "
    "AND units IS NOT NULL "
    "AND security_id IS NOT NULL "
    "AND fromunits IS NOT NULL "
    "AND fromsecurity_id IS NOT NULL "
    "AND currency IS NOT NULL "
    "AND cash IS NOT NULL "
    "AND numerator IS NULL "
    "AND denominator IS NULL "
    "AND fromfiaccount_id IS NULL"
)
TRANSACTION_CONSTRAINT = (
    f"{TRADE_CONSTRAINT} OR {ROC_CONSTRAINT} OR {TRANSFER_CONSTRAINT} OR "
    f"{SPLIT_CONSTRAINT} OR {SPINOFF_CONSTRAINT} OR {EXERCISE_CONSTRAINT}"
)


def upgrade():
    op.create_unique_constraint(
        None,
        "fi",
        ["brokerid"],
    )
    op.drop_constraint(
        op.f("fi_brokerid_key"),
        "fi",
        type_="unique",
    )

    op.create_foreign_key(
        None,
        "fiaccount",
        "fi",
        ["fi_id"],
        ["id"],
    )
    op.drop_constraint(
        op.f("fiaccount_fi_id_fkey"),
        "fiaccount",
        type_="foreignkey",
    )

    op.create_unique_constraint(
        None,
        "securityid",
        ["uniqueidtype", "uniqueid"],
    )
    op.drop_constraint(
        op.f("securityid_uniqueidtype_uniqueid_key"),
        "securityid",
        type_="unique",
    )
    op.create_foreign_key(
        None,
        "securityid",
        "security",
        ["security_id"],
        ["id"],
        onupdate="CASCADE")
    op.drop_constraint(
        op.f("securityid_security_id_fkey"),
        "securityid",
        type_="foreignkey",
    )

    op.create_unique_constraint(
        None,
        "transaction",
        ["uniqueid"],
    )
    op.drop_constraint(
        op.f("transaction_uniqueid_key"),
        "transaction",
        type_="unique",
    )
    op.create_foreign_key(
        None,
        "transaction",
        "fiaccount",
        ["fiaccount_id"],
        ["id"],
        onupdate="CASCADE",
    )
    op.drop_constraint(
        op.f("transaction_fiaccount_id_fkey"),
        "transaction",
        type_="foreignkey",
    )
    op.create_foreign_key(
        None,
        "transaction",
        "security",
        ["security_id"],
        ["id"],
        onupdate="CASCADE")
    op.drop_constraint(
        op.f("transaction_security_id_fkey"),
        "transaction",
        type_="foreignkey",
    )
    op.create_check_constraint(
        "units_nonzero",
        "transaction",
        "units <> 0",
    )
    op.drop_constraint(
        op.f("units_nonzero"),
        "transaction",
        type_="check",
    )
    op.create_check_constraint(
        "securityprice_not_negative",
        "transaction",
        "securityprice >= 0",
    )
    op.drop_constraint(
        op.f("securityprice_not_negative"),
        "transaction",
        type_="check",
    )
    op.create_check_constraint(
        "fromsecurityprice_not_negative",
        "transaction",
        "fromsecurityprice >= 0",
    )
    op.drop_constraint(
        op.f("securityfromprice_not_negative"),
        "transaction",
        type_="check",
    )
    op.create_check_constraint(
        "numerator_positive",
        "transaction",
        "numerator > 0",
    )
    op.drop_constraint(
        op.f("numerator_positive"),
        "transaction",
        type_="check",
    )
    op.create_check_constraint(
        "denominator_positive",
        "transaction",
        "denominator > 0",
    )
    op.drop_constraint(
        op.f("denominator_positive"),
        "transaction",
        type_="check",
    )
    op.create_check_constraint(
        "enforce_subtype_nulls",
        "transaction",
        TRANSACTION_CONSTRAINT,
    )
    op.drop_constraint(
        op.f("transaction_constraint"),
        "transaction",
        type_="check",
    )

    op.create_unique_constraint(
        None,
        "currencyrate",
        ["date", "fromcurrency", "tocurrency"],
    )
    op.drop_constraint(
        op.f("currencyrate_date_fromcurrency_tocurrency_key"),
        "currencyrate",
        type_="unique",
    )


def downgrade():
    op.create_unique_constraint(
        op.f("fi_brokerid_key"),
        "fi",
        ["brokerid"],
    )
    op.drop_constraint(
        op.f("uq_fi_brokerid"),
        "fi",
        type_="unique",
    )

    op.create_foreign_key(
        op.f("fiaccount_fi_id_fkey"),
        "fiaccount",
        "fi",
        ["fi_id"],
        ["id"],
    )
    op.drop_constraint(
        op.f("fk_fiaccount_fi_id_fi"),
        "fiaccount",
        type_="foreignkey",
    )

    op.create_unique_constraint(
        op.f("securityid_uniqueidtype_uniqueid_key"),
        "securityid",
        ["uniqueidtype", "uniqueid"],
    )
    op.drop_constraint(
        op.f("uq_securityid_uniqueidtype"),
        "securityid",
        type_="unique",
    )

    op.create_foreign_key(
        op.f("securityid_security_id_fkey"),
        "securityid",
        "security",
        ["security_id"],
        ["id"],
        onupdate="CASCADE")
    op.drop_constraint(
        op.f("fk_securityid_security_id_security"),
        "securityid",
        type_="foreignkey",
    )

    op.create_unique_constraint(
        op.f('transaction_uniqueid_key'),
        'transaction',
        ['uniqueid'],
    )
    op.drop_constraint(
        op.f('uq_transaction_uniqueid'),
        'transaction',
        type_='unique',
    )
    op.create_foreign_key(
        op.f("transaction_fiaccount_id_fkey"),
        "transaction",
        "fiaccount",
        ["fiaccount_id"],
        ["id"],
        onupdate="CASCADE",
    )
    op.drop_constraint(
        op.f("fk_transaction_fiaccount_id_fiaccount"),
        "transaction",
        type_="foreignkey",
    )
    op.create_foreign_key(
        op.f("transaction_security_id_fkey"),
        "transaction",
        "security",
        ["security_id"],
        ["id"],
        onupdate="CASCADE",
    )
    op.drop_constraint(
        op.f("fk_transaction_security_id_security"),
        "transaction",
        type_="foreignkey",
    )
    op.create_check_constraint(
        op.f("units_nonzero"),
        "transaction",
        "units <> 0",
    )
    op.drop_constraint(
        op.f("ck_transaction_units_nonzero"),
        "transaction",
        type_="check",
    )
    op.create_check_constraint(
        op.f("securityprice_not_negative"),
        "transaction",
        "securityprice >= 0",
    )
    op.drop_constraint(
        op.f("ck_transaction_securityprice_not_negative"),
        "transaction",
        type_="check",
    )
    op.create_check_constraint(
        op.f("securityfromprice_not_negative"),
        "transaction",
        "fromsecurityprice >= 0",
    )
    op.drop_constraint(
        op.f("ck_transaction_fromsecurityprice_not_negative"),
        "transaction",
        type_="check",
    )
    op.create_check_constraint(
        op.f("numerator_positive"),
        "transaction",
        "numerator > 0",
    )
    op.drop_constraint(
        op.f("ck_transaction_numerator_positive"),
        "transaction",
        type_="check",
    )
    op.create_check_constraint(
        op.f("denominator_positive"),
        "transaction",
        "denominator > 0",
    )
    op.drop_constraint(
        op.f("ck_transaction_denominator_positive"),
        "transaction",
        type_="check",
    )
    op.create_check_constraint(
        op.f("transaction_constraint"),
        "transaction",
        TRANSACTION_CONSTRAINT,
    )
    op.drop_constraint(
        op.f("ck_transaction_enforce_subtype_nulls"),
        "transaction",
        type_="check",
    )

    op.create_unique_constraint(
        op.f("currencyrate_date_fromcurrency_tocurrency_key"),
        "currencyrate",
        ["date", "fromcurrency", "tocurrency"],
    )
    op.drop_constraint(
        op.f("uq_currencyrate_date"),
        "currencyrate",
        type_="unique",
    )
