"""lowercase columns

Revision ID: 029f05e90aa3
Revises: c79cb01f6712
Create Date: 2019-06-12 13:13:03.778612

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '029f05e90aa3'
down_revision = 'c79cb01f6712'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column("transaction", "securityPrice", new_column_name="securityprice")
    op.alter_column("transaction", "fiaccountFrom_id", new_column_name="fiaccountfrom_id")
    op.alter_column("transaction", "securityFrom_id", new_column_name="securityfrom_id")
    op.alter_column("transaction", "unitsFrom", new_column_name="unitsfrom")
    op.alter_column("transaction", "securityFromPrice", new_column_name="securityfromprice")

    op.drop_constraint('transaction_securityFrom_id_fkey', 'transaction', type_='foreignkey')
    op.drop_constraint('transaction_fiaccountFrom_id_fkey', 'transaction', type_='foreignkey')
    op.create_foreign_key('transaction_securityfrom_id_fkey', 'transaction', 'security', ['securityfrom_id'], ['id'], onupdate='CASCADE')
    op.create_foreign_key('transaction_fiaccountfrom_id_fkey', 'transaction', 'fiaccount', ['fiaccountfrom_id'], ['id'], onupdate='CASCADE')


def downgrade():
    op.alter_column("transaction", "securityprice", new_column_name="securityPrice")
    op.alter_column("transaction", "fiaccountfrom_id", new_column_name="fiaccountFrom_id")
    op.alter_column("transaction", "securityfrom_id", new_column_name="securityFrom_id")
    op.alter_column("transaction", "unitsfrom", new_column_name="unitsFrom")
    op.alter_column("transaction", "securityfromprice", new_column_name="securityFromPrice")

    op.drop_constraint('transaction_fiaccountfrom_id_fkey', 'transaction', type_='foreignkey')
    op.drop_constraint('transaction_securityfrom_id_fkey', 'transaction', type_='foreignkey')
    op.create_foreign_key('transaction_fiaccountFrom_id_fkey', 'transaction', 'fiaccount', ['fiaccountFrom_id'], ['id'], onupdate='CASCADE')
    op.create_foreign_key('transaction_securityFrom_id_fkey', 'transaction', 'security', ['securityFrom_id'], ['id'], onupdate='CASCADE')
