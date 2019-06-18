"""Transaction fromXXX

Revision ID: bc10e6480037
Revises: b8823b40217c
Create Date: 2019-06-18 06:12:06.698487

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'bc10e6480037'
down_revision = 'b8823b40217c'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column("transaction", "fiaccountfrom_id", new_column_name="fromfiaccount_id")
    op.alter_column("transaction", "securityfrom_id", new_column_name="fromsecurity_id")
    op.alter_column("transaction", "securityfromprice", new_column_name="fromsecurityprice")
    op.alter_column("transaction", "unitsfrom", new_column_name="fromunits")
    op.drop_constraint('transaction_fiaccountfrom_id_fkey', 'transaction', type_='foreignkey')
    op.drop_constraint('transaction_securityfrom_id_fkey', 'transaction', type_='foreignkey')
    op.create_foreign_key(None, 'transaction', 'security', ['fromsecurity_id'], ['id'], onupdate='CASCADE')
    op.create_foreign_key(None, 'transaction', 'fiaccount', ['fromfiaccount_id'], ['id'], onupdate='CASCADE')


def downgrade():
    op.alter_column("transaction", "fromfiaccount_id", new_column_name="fiaccountfrom_id")
    op.alter_column("transaction", "fromsecurity_id", new_column_name="securityfrom_id")
    op.alter_column("transaction", "fromsecurityprice", new_column_name="securityfromprice")
    op.alter_column("transaction", "fromunits", new_column_name="unitsfrom")


    op.create_foreign_key(
        "transaction_fiaccountfrom_id_fkey",
        "transaction",
        "fiaccount",
        ["fiaccountfrom_id"],
        ["id"],
        onupdate="CASCADE",
    )
    op.drop_constraint(
        op.f("fk_transaction_fromfiaccount_id_fiaccount"),
        "transaction",
        type_="foreignkey",
    )
    op.create_foreign_key(
        #  op.f("transaction_fromsecurity_id_fkey"),
        op.f("transaction_securityfrom_id_fkey"),
        "transaction",
        "security",
        ["securityfrom_id"],
        ["id"],
        onupdate="CASCADE",
    )
    op.drop_constraint(
        op.f("fk_transaction_fromsecurity_id_security"),
        "transaction",
        type_="foreignkey",
    )
