"""Transaction constraints

Revision ID: 881f97565ab1
Revises: f15919e6f204
Create Date: 2019-06-17 06:32:23.726120

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '881f97565ab1'
down_revision = 'f15919e6f204'
branch_labels = None
depends_on = None


TRADE_CONSTRAINT = (
    "type='TRADE' "
    "AND currency IS NOT NULL "
    "AND cash IS NOT NULL "
    "AND units IS NOT NULL "
    "AND securityprice IS NULL "
    "AND fiaccountfrom_id IS NULL "
    "AND securityfrom_id IS NULL "
    "AND unitsfrom IS NULL "
    "AND securityfromprice IS NULL "
    "AND numerator IS NULL "
    "AND denominator IS NULL"
)
ROC_CONSTRAINT = (
    "type='RETURNCAP' "
    "AND currency is NOT NULL "
    "AND cash IS NOT NULL "
    "AND units IS NULL "
    "AND securityprice IS NULL "
    "AND fiaccountfrom_id IS NULL "
    "AND securityfrom_id IS NULL "
    "AND unitsfrom IS NULL "
    "AND unitsfrom IS NULL "
    "AND securityfromprice IS NULL "
    "AND numerator IS NULL "
    "AND denominator IS NULL"
)
TRANSFER_CONSTRAINT = (
    "type='TRANSFER' "
    "AND units IS NOT NULL "
    "AND fiaccountfrom_id IS NOT NULL "
    "AND securityfrom_id IS NOT NULL "
    "AND unitsfrom IS NOT NULL "
    "AND currency IS NULL "
    "AND cash IS NULL "
    "AND securityprice IS NULL "
    "AND securityfromprice IS NULL "
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
    "AND securityfromprice IS NULL "
    "AND fiaccountfrom_id IS NULL "
    "AND securityfrom_id IS NULL "
    "AND unitsfrom IS NULL"
)
SPINOFF_CONSTRAINT = (
    "type='SPINOFF' "
    "AND units IS NOT NULL "
    "AND securityfrom_id IS NOT NULL "
    "AND numerator IS NOT NULL "
    "AND denominator IS NOT NULL "
    "AND currency IS NULL "
    "AND cash IS NULL "
    "AND fiaccountfrom_id IS NULL "
    "AND unitsfrom IS NULL"
)
EXERCISE_CONSTRAINT = (
    "type='EXERCISE' "
    "AND units IS NOT NULL "
    "AND security_id IS NOT NULL "
    "AND unitsfrom IS NOT NULL "
    "AND securityfrom_id IS NOT NULL "
    "AND currency IS NOT NULL "
    "AND cash IS NOT NULL "
    "AND numerator IS NULL "
    "AND denominator IS NULL "
    "AND fiaccountfrom_id IS NULL"
)
TRANSACTION_CONSTRAINT = (
    f"{TRADE_CONSTRAINT} OR {ROC_CONSTRAINT} OR {TRANSFER_CONSTRAINT} OR "
    f"{SPLIT_CONSTRAINT} OR {SPINOFF_CONSTRAINT} OR {EXERCISE_CONSTRAINT}"
)


def upgrade():
    op.create_check_constraint(
        op.f("transaction_constraint"),
        "transaction",
        TRANSACTION_CONSTRAINT,
    )
    op.create_check_constraint(
        op.f("units_nonzero"),
        "transaction",
        "units <> 0",
    )
    op.create_check_constraint(
        op.f("securityprice_not_negative"),
        "transaction",
        "securityprice >= 0",
    )
    op.create_check_constraint(
        op.f("securityfromprice_not_negative"),
        "transaction",
        "securityfromprice >= 0",
    )
    op.create_check_constraint(
        op.f("numerator_positive"),
        "transaction",
        "numerator > 0",
    )
    op.create_check_constraint(
        op.f("denominator_positive"),
        "transaction",
        "denominator > 0",
    )


def downgrade():
    op.drop_constraint(
        op.f("transaction_constraint"),
        "transaction",
        type_="check",
    )
    op.drop_constraint(
        op.f("units_nonzero"),
        "transaction",
        type_="check",
    )
    op.drop_constraint(
        op.f("securityprice_not_negative"),
        "transaction",
        type_="check",
    )
    op.drop_constraint(
        op.f("securityfromprice_not_negative"),
        "transaction",
        type_="check",
    )
    op.drop_constraint(
        op.f("numerator_positive"),
        "transaction",
        type_="check",
    )
    op.drop_constraint(
        op.f("denominator_positive"),
        "transaction",
        type_="check",
    )
