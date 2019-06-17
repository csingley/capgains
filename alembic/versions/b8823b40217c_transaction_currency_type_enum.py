"""Transaction currency type enum

Revision ID: b8823b40217c
Revises: 881f97565ab1
Create Date: 2019-06-17 14:27:20.132136

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "b8823b40217c"
down_revision = "881f97565ab1"
branch_labels = None
depends_on = None


CURRENCIES = (
    "AED", "AFN", "ALL", "AMD", "ANG", "AOA", "ARS", "AUD", "AWG", "AZN", "BAM", "BBD",
    "BDT", "BGN", "BHD", "BIF", "BMD", "BND", "BOB", "BOV", "BRL", "BSD", "BTN", "BWP",
    "BYR", "BZD", "CAD", "CDF", "CHE", "CHF", "CHW", "CLF", "CLP", "CNY", "COP", "COU",
    "CRC", "CUC", "CUP", "CVE", "CZK", "DJF", "DKK", "DOP", "DZD", "EEK", "EGP", "ERN",
    "ETB", "EUR", "FJD", "FKP", "GBP", "GEL", "GHS", "GIP", "GMD", "GNF", "GTQ", "GYD",
    "HKD", "HNL", "HRK", "HTG", "HUF", "IDR", "ILS", "INR", "IQD", "IRR", "ISK", "JMD",
    "JOD", "JPY", "KES", "KGS", "KHR", "KMF", "KPW", "KRW", "KWD", "KYD", "KZT", "LAK",
    "LBP", "LKR", "LRD", "LSL", "LTL", "LVL", "LYD", "MAD", "MDL", "MGA", "MKD", "MMK",
    "MNT", "MOP", "MRO", "MUR", "MVR", "MWK", "MXN", "MXV", "MYR", "MZN", "NAD", "NGN",
    "NIO", "NOK", "NPR", "NZD", "OMR", "PAB", "PEN", "PGK", "PHP", "PKR", "PLN", "PYG",
    "QAR", "RON", "RSD", "RUB", "RWF", "SAR", "SBD", "SCR", "SDG", "SEK", "SGD", "SHP",
    "SLL", "SOS", "SRD", "STD", "SVC", "SYP", "SZL", "THB", "TJS", "TMT", "TND", "TOP",
    "TRY", "TTD", "TWD", "TZS", "UAH", "UGX", "USD", "USN", "USS", "UYI", "UYU", "UZS",
    "VEF", "VND", "VUV", "WST", "XAF", "XAG", "XAU", "XBA", "XBB", "XBC", "XBD", "XCD",
    "XDR", "XOF", "XPD", "XPF", "XPT", "XTS", "XXX", "YER", "ZAR", "ZMK", "ZWL", "CNH",
)


def upgrade():
    #  Create single currency type enum for common use by transaction.currency,
    #  currencyrate.fromcurrency, currencyrate.tocurrencs
    currency_enum_type = postgresql.ENUM(*CURRENCIES, name='currency_type')
    connection = op.get_bind()
    currency_enum_type.create(connection, checkfirst=False)

    op.alter_column(
        'transaction', 'currency',
        existing_type=postgresql.ENUM(*CURRENCIES, name='transaction_currency'),
        type_=currency_enum_type,
        postgresql_using="currency::varchar::currency_type",
    )

    op.alter_column(
        'currencyrate', 'fromcurrency',
        existing_type=postgresql.ENUM(*CURRENCIES, name='fromcurrency'),
        type_=currency_enum_type,
        postgresql_using="fromcurrency::varchar::currency_type",
    )

    op.alter_column(
        'currencyrate', 'tocurrency',
        existing_type=postgresql.ENUM(*CURRENCIES, name='tocurrency'),
        type_=currency_enum_type,
        postgresql_using="tocurrency::varchar::currency_type",
    )
    #  Remove unused currency types
    op.execute("DROP TYPE transaction_currency")
    op.execute("DROP TYPE fromcurrency")
    op.execute("DROP TYPE tocurrency")


def downgrade():
    currency_enum_type = postgresql.ENUM(*CURRENCIES, name='currency_type')

    transactioncurrency_enum_type = postgresql.ENUM(*CURRENCIES, name='transaction_currency')
    fromcurrency_enum_type = postgresql.ENUM(*CURRENCIES, name='fromcurrency')
    tocurrency_enum_type = postgresql.ENUM(*CURRENCIES, name='tocurrency')
    connection = op.get_bind()
    for enum_type in (
        transactioncurrency_enum_type,
        fromcurrency_enum_type,
        tocurrency_enum_type,
    ):
        enum_type.create(connection, checkfirst=False)

    op.alter_column(
        'transaction', 'currency',
        existing_type=currency_enum_type,
        type_=transactioncurrency_enum_type,
        postgresql_using="currency::varchar::transaction_currency",
    )

    op.alter_column(
        'currencyrate', 'fromcurrency',
        type_=fromcurrency_enum_type,
        existing_type=currency_enum_type,
        postgresql_using="fromcurrency::varchar::fromcurrency",
    )

    op.alter_column(
        'currencyrate', 'tocurrency',
        type_=tocurrency_enum_type,
        existing_type=currency_enum_type,
        postgresql_using="tocurrency::varchar::tocurrency",
    )
    #  Remove unused currency type
    op.execute("DROP TYPE currency_type")
