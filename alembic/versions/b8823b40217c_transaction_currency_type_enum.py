"""Transaction currency type enum

Revision ID: b8823b40217c
Revises: 881f97565ab1
Create Date: 2019-06-17 14:27:20.132136

"""
from alembic import op
#  import sqlalchemy as sa
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


CURRENCY_ENUM_TYPE = postgresql.ENUM(*CURRENCIES, name='currency_type')


TXCURRENCY_ENUM_TYPE = postgresql.ENUM(*CURRENCIES, name='transaction_currency')
FROMCURRENCY_ENUM_TYPE = postgresql.ENUM(*CURRENCIES, name='fromcurrency')
TOCURRENCY_ENUM_TYPE = postgresql.ENUM(*CURRENCIES, name='tocurrency')


def upgrade():
    #  Create single currency type enum for common use by transaction.currency,
    #  currencyrate.fromcurrency, currencyrate.tocurrencs
    currency_enum_type = postgresql.ENUM(*CURRENCIES, name='currency_type')
    connection = op.get_bind()
    currency_enum_type.create(connection, checkfirst=False)

    op.alter_column(
        'transaction', 'currency',
        existing_type=TXCURRENCY_ENUM_TYPE,
        type_=CURRENCY_ENUM_TYPE,
        postgresql_using="currency::varchar::currency_type",
    )

    op.alter_column(
        'currencyrate', 'fromcurrency',
        existing_type=FROMCURRENCY_ENUM_TYPE,
        type_=CURRENCY_ENUM_TYPE,
        postgresql_using="fromcurrency::varchar::currency_type",
    )

    op.alter_column(
        'currencyrate', 'tocurrency',
        existing_type=TOCURRENCY_ENUM_TYPE,
        type_=CURRENCY_ENUM_TYPE,
        postgresql_using="tocurrency::varchar::currency_type",
    )
    #  Remove unused currency types
    TXCURRENCY_ENUM_TYPE.drop(connection)
    FROMCURRENCY_ENUM_TYPE.drop(connection)
    TOCURRENCY_ENUM_TYPE.drop(connection)


def downgrade():
    connection = op.get_bind()
    for enum_type in (
        TXCURRENCY_ENUM_TYPE,
        FROMCURRENCY_ENUM_TYPE,
        TOCURRENCY_ENUM_TYPE,
    ):
        enum_type.create(connection, checkfirst=False)

    op.alter_column(
        'transaction', 'currency',
        existing_type=CURRENCY_ENUM_TYPE,
        type_=TXCURRENCY_ENUM_TYPE,
        postgresql_using="currency::varchar::transaction_currency",
    )

    op.alter_column(
        'currencyrate', 'fromcurrency',
        type_=FROMCURRENCY_ENUM_TYPE,
        existing_type=CURRENCY_ENUM_TYPE,
        postgresql_using="fromcurrency::varchar::fromcurrency",
    )

    op.alter_column(
        'currencyrate', 'tocurrency',
        type_=TOCURRENCY_ENUM_TYPE,
        existing_type=CURRENCY_ENUM_TYPE,
        postgresql_using="tocurrency::varchar::tocurrency",
    )
    #  Remove unused currency type
    CURRENCY_ENUM_TYPE.drop(connection)
