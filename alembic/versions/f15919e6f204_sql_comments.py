"""SQL comments

Revision ID: f15919e6f204
Revises: 029f05e90aa3
Create Date: 2019-06-12 19:21:25.488847

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'f15919e6f204'
down_revision = '029f05e90aa3'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('currencyrate', 'fromcurrency',
               existing_type=postgresql.ENUM('AED', 'AFN', 'ALL', 'AMD', 'ANG', 'AOA', 'ARS', 'AUD', 'AWG', 'AZN', 'BAM', 'BBD', 'BDT', 'BGN', 'BHD', 'BIF', 'BMD', 'BND', 'BOB', 'BOV', 'BRL', 'BSD', 'BTN', 'BWP', 'BYR', 'BZD', 'CAD', 'CDF', 'CHE', 'CHF', 'CHW', 'CLF', 'CLP', 'CNY', 'COP', 'COU', 'CRC', 'CUC', 'CUP', 'CVE', 'CZK', 'DJF', 'DKK', 'DOP', 'DZD', 'EEK', 'EGP', 'ERN', 'ETB', 'EUR', 'FJD', 'FKP', 'GBP', 'GEL', 'GHS', 'GIP', 'GMD', 'GNF', 'GTQ', 'GYD', 'HKD', 'HNL', 'HRK', 'HTG', 'HUF', 'IDR', 'ILS', 'INR', 'IQD', 'IRR', 'ISK', 'JMD', 'JOD', 'JPY', 'KES', 'KGS', 'KHR', 'KMF', 'KPW', 'KRW', 'KWD', 'KYD', 'KZT', 'LAK', 'LBP', 'LKR', 'LRD', 'LSL', 'LTL', 'LVL', 'LYD', 'MAD', 'MDL', 'MGA', 'MKD', 'MMK', 'MNT', 'MOP', 'MRO', 'MUR', 'MVR', 'MWK', 'MXN', 'MXV', 'MYR', 'MZN', 'NAD', 'NGN', 'NIO', 'NOK', 'NPR', 'NZD', 'OMR', 'PAB', 'PEN', 'PGK', 'PHP', 'PKR', 'PLN', 'PYG', 'QAR', 'RON', 'RSD', 'RUB', 'RWF', 'SAR', 'SBD', 'SCR', 'SDG', 'SEK', 'SGD', 'SHP', 'SLL', 'SOS', 'SRD', 'STD', 'SVC', 'SYP', 'SZL', 'THB', 'TJS', 'TMT', 'TND', 'TOP', 'TRY', 'TTD', 'TWD', 'TZS', 'UAH', 'UGX', 'USD', 'USN', 'USS', 'UYI', 'UYU', 'UZS', 'VEF', 'VND', 'VUV', 'WST', 'XAF', 'XAG', 'XAU', 'XBA', 'XBB', 'XBC', 'XBD', 'XCD', 'XDR', 'XOF', 'XPD', 'XPF', 'XPT', 'XTS', 'XXX', 'YER', 'ZAR', 'ZMK', 'ZWL', 'CNH', name='fromcurrency'),
               comment='Currency of exchange rate denominator (ISO4217)',
               existing_nullable=False)
    op.alter_column('currencyrate', 'rate',
               existing_type=sa.NUMERIC(),
               comment='Multiply this rate by fromcurrency amount to yield tocurrency amount',
               existing_nullable=False)
    op.alter_column('currencyrate', 'tocurrency',
               existing_type=postgresql.ENUM('AED', 'AFN', 'ALL', 'AMD', 'ANG', 'AOA', 'ARS', 'AUD', 'AWG', 'AZN', 'BAM', 'BBD', 'BDT', 'BGN', 'BHD', 'BIF', 'BMD', 'BND', 'BOB', 'BOV', 'BRL', 'BSD', 'BTN', 'BWP', 'BYR', 'BZD', 'CAD', 'CDF', 'CHE', 'CHF', 'CHW', 'CLF', 'CLP', 'CNY', 'COP', 'COU', 'CRC', 'CUC', 'CUP', 'CVE', 'CZK', 'DJF', 'DKK', 'DOP', 'DZD', 'EEK', 'EGP', 'ERN', 'ETB', 'EUR', 'FJD', 'FKP', 'GBP', 'GEL', 'GHS', 'GIP', 'GMD', 'GNF', 'GTQ', 'GYD', 'HKD', 'HNL', 'HRK', 'HTG', 'HUF', 'IDR', 'ILS', 'INR', 'IQD', 'IRR', 'ISK', 'JMD', 'JOD', 'JPY', 'KES', 'KGS', 'KHR', 'KMF', 'KPW', 'KRW', 'KWD', 'KYD', 'KZT', 'LAK', 'LBP', 'LKR', 'LRD', 'LSL', 'LTL', 'LVL', 'LYD', 'MAD', 'MDL', 'MGA', 'MKD', 'MMK', 'MNT', 'MOP', 'MRO', 'MUR', 'MVR', 'MWK', 'MXN', 'MXV', 'MYR', 'MZN', 'NAD', 'NGN', 'NIO', 'NOK', 'NPR', 'NZD', 'OMR', 'PAB', 'PEN', 'PGK', 'PHP', 'PKR', 'PLN', 'PYG', 'QAR', 'RON', 'RSD', 'RUB', 'RWF', 'SAR', 'SBD', 'SCR', 'SDG', 'SEK', 'SGD', 'SHP', 'SLL', 'SOS', 'SRD', 'STD', 'SVC', 'SYP', 'SZL', 'THB', 'TJS', 'TMT', 'TND', 'TOP', 'TRY', 'TTD', 'TWD', 'TZS', 'UAH', 'UGX', 'USD', 'USN', 'USS', 'UYI', 'UYU', 'UZS', 'VEF', 'VND', 'VUV', 'WST', 'XAF', 'XAG', 'XAU', 'XBA', 'XBB', 'XBC', 'XBD', 'XCD', 'XDR', 'XOF', 'XPD', 'XPF', 'XPT', 'XTS', 'XXX', 'YER', 'ZAR', 'ZMK', 'ZWL', 'CNH', name='tocurrency'),
               comment='Currency of exchange rate numerator (ISO417)',
               existing_nullable=False)
    op.create_table_comment(
        'currencyrate',
        'Exchange Rates for Currency Pairs',
        existing_comment=None,
        schema=None
    )
    op.alter_column('fi', 'brokerid',
               existing_type=sa.VARCHAR(),
               comment='OFX <INVACCTFROM><BROKERID> value',
               existing_nullable=False)
    op.create_table_comment(
        'fi',
        'Financial Institution (e.g. Brokerage)',
        existing_comment=None,
        schema=None
    )
    op.alter_column('fiaccount', 'fi_id',
               existing_type=sa.INTEGER(),
               comment='Financial institution (FK fi.id)',
               existing_nullable=False)
    op.alter_column('fiaccount', 'number',
               existing_type=sa.VARCHAR(),
               comment='account# (OFX <INVACCTFROM><ACCTID> value',
               existing_nullable=False)
    op.create_table_comment(
        'fiaccount',
        'Financial Institution (e.g. Brokerage) Account',
        existing_comment=None,
        schema=None
    )
    op.alter_column('securityid', 'security_id',
               existing_type=sa.INTEGER(),
               comment='FK security.id',
               existing_nullable=False)
    op.alter_column('securityid', 'uniqueid',
               existing_type=sa.VARCHAR(),
               comment='CUSIP, ISIN, etc.',
               existing_nullable=False)
    op.alter_column('securityid', 'uniqueidtype',
               existing_type=sa.VARCHAR(),
               comment='CUSIP, ISIN, etc.',
               existing_nullable=False)
    op.create_table_comment(
        'securityid',
        'Unique Identifiers for Securities',
        existing_comment=None,
        schema=None
    )
    op.alter_column('transaction', 'datetime',
               existing_type=postgresql.TIMESTAMP(),
               comment='Effective date/time: ex-date for reorgs, return of capital',
               existing_nullable=False)
    op.alter_column('transaction', 'denominator',
               existing_type=sa.NUMERIC(),
               comment='For splits, spinoffs: normalized units of source security',
               existing_nullable=True)
    op.alter_column('transaction', 'dtsettle',
               existing_type=postgresql.TIMESTAMP(),
               comment='Settlement date: pay date for return of capital',
               existing_nullable=True)
    op.alter_column('transaction', 'fiaccount_id',
               existing_type=sa.INTEGER(),
               comment='Financial institution account (for transfers, destination FI account) - FK fiaccount.id',
               existing_nullable=False)
    op.alter_column('transaction', 'fiaccountfrom_id',
               existing_type=sa.INTEGER(),
               comment='For transfers: source FI account (FK fiaccount.id)',
               existing_nullable=True)
    op.alter_column('transaction', 'numerator',
               existing_type=sa.NUMERIC(),
               comment='For splits, spinoffs: normalized units of destination security',
               existing_nullable=True)
    op.alter_column('transaction', 'security_id',
               existing_type=sa.INTEGER(),
               comment='FK security.id',
               existing_nullable=False)
    op.alter_column('transaction', 'securityfrom_id',
               existing_type=sa.INTEGER(),
               comment='For transfers, spinoffs, exercise: source security (FK security.id)',
               existing_nullable=True)
    op.alter_column('transaction', 'securityfromprice',
               existing_type=sa.NUMERIC(),
               comment='For spinoffs: unit price used to fair-value source security',
               existing_nullable=True)
    op.alter_column('transaction', 'securityprice',
               existing_type=sa.NUMERIC(),
               comment='For spinoffs: unit price used to fair-value destination security',
               existing_nullable=True)
    op.alter_column('transaction', 'sort',
               existing_type=postgresql.ENUM('FIFO', 'LIFO', 'MAXGAIN', 'MINGAIN', name='transaction_sort'),
               comment='Sort algorithm for gain recognition',
               existing_nullable=True)
    op.alter_column('transaction', 'type',
               existing_type=postgresql.ENUM('returnofcapital', 'split', 'spinoff', 'transfer', 'trade', 'exercise', name='transaction_type'),
               comment="One of ('RETURNCAP', 'SPLIT', 'SPINOFF', 'TRANSFER', 'TRADE', 'EXERCISE')",
               existing_nullable=False)
    op.alter_column('transaction', 'uniqueid',
               existing_type=sa.VARCHAR(),
               comment='FI transaction unique identifier',
               existing_nullable=False)
    op.alter_column('transaction', 'units',
               existing_type=sa.NUMERIC(),
               comment='Change in shares, contracts, etc. caused by Transaction (for splits, transfers, exercise: destination security change in units)',
               existing_nullable=True)
    op.alter_column('transaction', 'unitsfrom',
               existing_type=sa.NUMERIC(),
               comment='For splits, transfers, exercise: source security change in units',
               existing_nullable=True)
    op.create_table_comment(
        'transaction',
        'Securities Transactions',
        existing_comment=None,
        schema=None
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table_comment(
        'transaction',
        existing_comment='Securities Transactions',
        schema=None
    )
    op.alter_column('transaction', 'unitsfrom',
               existing_type=sa.NUMERIC(),
               comment=None,
               existing_comment='For splits, transfers, exercise: source security change in units',
               existing_nullable=True)
    op.alter_column('transaction', 'units',
               existing_type=sa.NUMERIC(),
               comment=None,
               existing_comment='Change in shares, contracts, etc. caused by Transaction (for splits, transfers, exercise: destination security change in units)',
               existing_nullable=True)
    op.alter_column('transaction', 'uniqueid',
               existing_type=sa.VARCHAR(),
               comment=None,
               existing_comment='FI transaction unique identifier',
               existing_nullable=False)
    op.alter_column('transaction', 'type',
               existing_type=postgresql.ENUM('returnofcapital', 'split', 'spinoff', 'transfer', 'trade', 'exercise', name='transaction_type'),
               comment=None,
               existing_comment="One of ('RETURNCAP', 'SPLIT', 'SPINOFF', 'TRANSFER', 'TRADE', 'EXERCISE')",
               existing_nullable=False)
    op.alter_column('transaction', 'sort',
               existing_type=postgresql.ENUM('FIFO', 'LIFO', 'MAXGAIN', 'MINGAIN', name='transaction_sort'),
               comment=None,
               existing_comment='Sort algorithm for gain recognition',
               existing_nullable=True)
    op.alter_column('transaction', 'securityprice',
               existing_type=sa.NUMERIC(),
               comment=None,
               existing_comment='For spinoffs: unit price used to fair-value destination security',
               existing_nullable=True)
    op.alter_column('transaction', 'securityfromprice',
               existing_type=sa.NUMERIC(),
               comment=None,
               existing_comment='For spinoffs: unit price used to fair-value source security',
               existing_nullable=True)
    op.alter_column('transaction', 'securityfrom_id',
               existing_type=sa.INTEGER(),
               comment=None,
               existing_comment='For transfers, spinoffs, exercise: source security (FK security.id)',
               existing_nullable=True)
    op.alter_column('transaction', 'security_id',
               existing_type=sa.INTEGER(),
               comment=None,
               existing_comment='FK security.id',
               existing_nullable=False)
    op.alter_column('transaction', 'numerator',
               existing_type=sa.NUMERIC(),
               comment=None,
               existing_comment='For splits, spinoffs: normalized units of destination security',
               existing_nullable=True)
    op.alter_column('transaction', 'fiaccountfrom_id',
               existing_type=sa.INTEGER(),
               comment=None,
               existing_comment='For transfers: source FI account (FK fiaccount.id)',
               existing_nullable=True)
    op.alter_column('transaction', 'fiaccount_id',
               existing_type=sa.INTEGER(),
               comment=None,
               existing_comment='Financial institution account (for transfers, destination FI account) - FK fiaccount.id',
               existing_nullable=False)
    op.alter_column('transaction', 'dtsettle',
               existing_type=postgresql.TIMESTAMP(),
               comment=None,
               existing_comment='Settlement date: pay date for return of capital',
               existing_nullable=True)
    op.alter_column('transaction', 'denominator',
               existing_type=sa.NUMERIC(),
               comment=None,
               existing_comment='For splits, spinoffs: normalized units of source security',
               existing_nullable=True)
    op.alter_column('transaction', 'datetime',
               existing_type=postgresql.TIMESTAMP(),
               comment=None,
               existing_comment='Effective date/time: ex-date for reorgs, return of capital',
               existing_nullable=False)
    op.drop_table_comment(
        'securityid',
        existing_comment='Unique Identifiers for Securities',
        schema=None
    )
    op.alter_column('securityid', 'uniqueidtype',
               existing_type=sa.VARCHAR(),
               comment=None,
               existing_comment='CUSIP, ISIN, etc.',
               existing_nullable=False)
    op.alter_column('securityid', 'uniqueid',
               existing_type=sa.VARCHAR(),
               comment=None,
               existing_comment='CUSIP, ISIN, etc.',
               existing_nullable=False)
    op.alter_column('securityid', 'security_id',
               existing_type=sa.INTEGER(),
               comment=None,
               existing_comment='FK security.id',
               existing_nullable=False)
    op.drop_table_comment(
        'fiaccount',
        existing_comment='Financial Institution (e.g. Brokerage) Account',
        schema=None
    )
    op.alter_column('fiaccount', 'number',
               existing_type=sa.VARCHAR(),
               comment=None,
               existing_comment='account# (OFX <INVACCTFROM><ACCTID> value',
               existing_nullable=False)
    op.alter_column('fiaccount', 'fi_id',
               existing_type=sa.INTEGER(),
               comment=None,
               existing_comment='Financial institution (FK fi.id)',
               existing_nullable=False)
    op.drop_table_comment(
        'fi',
        existing_comment='Financial Institution (e.g. Brokerage)',
        schema=None
    )
    op.alter_column('fi', 'brokerid',
               existing_type=sa.VARCHAR(),
               comment=None,
               existing_comment='OFX <INVACCTFROM><BROKERID> value',
               existing_nullable=False)
    op.drop_table_comment(
        'currencyrate',
        existing_comment='Exchange Rates for Currency Pairs',
        schema=None
    )
    op.alter_column('currencyrate', 'tocurrency',
               existing_type=postgresql.ENUM('AED', 'AFN', 'ALL', 'AMD', 'ANG', 'AOA', 'ARS', 'AUD', 'AWG', 'AZN', 'BAM', 'BBD', 'BDT', 'BGN', 'BHD', 'BIF', 'BMD', 'BND', 'BOB', 'BOV', 'BRL', 'BSD', 'BTN', 'BWP', 'BYR', 'BZD', 'CAD', 'CDF', 'CHE', 'CHF', 'CHW', 'CLF', 'CLP', 'CNY', 'COP', 'COU', 'CRC', 'CUC', 'CUP', 'CVE', 'CZK', 'DJF', 'DKK', 'DOP', 'DZD', 'EEK', 'EGP', 'ERN', 'ETB', 'EUR', 'FJD', 'FKP', 'GBP', 'GEL', 'GHS', 'GIP', 'GMD', 'GNF', 'GTQ', 'GYD', 'HKD', 'HNL', 'HRK', 'HTG', 'HUF', 'IDR', 'ILS', 'INR', 'IQD', 'IRR', 'ISK', 'JMD', 'JOD', 'JPY', 'KES', 'KGS', 'KHR', 'KMF', 'KPW', 'KRW', 'KWD', 'KYD', 'KZT', 'LAK', 'LBP', 'LKR', 'LRD', 'LSL', 'LTL', 'LVL', 'LYD', 'MAD', 'MDL', 'MGA', 'MKD', 'MMK', 'MNT', 'MOP', 'MRO', 'MUR', 'MVR', 'MWK', 'MXN', 'MXV', 'MYR', 'MZN', 'NAD', 'NGN', 'NIO', 'NOK', 'NPR', 'NZD', 'OMR', 'PAB', 'PEN', 'PGK', 'PHP', 'PKR', 'PLN', 'PYG', 'QAR', 'RON', 'RSD', 'RUB', 'RWF', 'SAR', 'SBD', 'SCR', 'SDG', 'SEK', 'SGD', 'SHP', 'SLL', 'SOS', 'SRD', 'STD', 'SVC', 'SYP', 'SZL', 'THB', 'TJS', 'TMT', 'TND', 'TOP', 'TRY', 'TTD', 'TWD', 'TZS', 'UAH', 'UGX', 'USD', 'USN', 'USS', 'UYI', 'UYU', 'UZS', 'VEF', 'VND', 'VUV', 'WST', 'XAF', 'XAG', 'XAU', 'XBA', 'XBB', 'XBC', 'XBD', 'XCD', 'XDR', 'XOF', 'XPD', 'XPF', 'XPT', 'XTS', 'XXX', 'YER', 'ZAR', 'ZMK', 'ZWL', 'CNH', name='tocurrency'),
               comment=None,
               existing_comment='Currency of exchange rate numerator (ISO417)',
               existing_nullable=False)
    op.alter_column('currencyrate', 'rate',
               existing_type=sa.NUMERIC(),
               comment=None,
               existing_comment='Multiply this rate by fromcurrency amount to yield tocurrency amount',
               existing_nullable=False)
    op.alter_column('currencyrate', 'fromcurrency',
               existing_type=postgresql.ENUM('AED', 'AFN', 'ALL', 'AMD', 'ANG', 'AOA', 'ARS', 'AUD', 'AWG', 'AZN', 'BAM', 'BBD', 'BDT', 'BGN', 'BHD', 'BIF', 'BMD', 'BND', 'BOB', 'BOV', 'BRL', 'BSD', 'BTN', 'BWP', 'BYR', 'BZD', 'CAD', 'CDF', 'CHE', 'CHF', 'CHW', 'CLF', 'CLP', 'CNY', 'COP', 'COU', 'CRC', 'CUC', 'CUP', 'CVE', 'CZK', 'DJF', 'DKK', 'DOP', 'DZD', 'EEK', 'EGP', 'ERN', 'ETB', 'EUR', 'FJD', 'FKP', 'GBP', 'GEL', 'GHS', 'GIP', 'GMD', 'GNF', 'GTQ', 'GYD', 'HKD', 'HNL', 'HRK', 'HTG', 'HUF', 'IDR', 'ILS', 'INR', 'IQD', 'IRR', 'ISK', 'JMD', 'JOD', 'JPY', 'KES', 'KGS', 'KHR', 'KMF', 'KPW', 'KRW', 'KWD', 'KYD', 'KZT', 'LAK', 'LBP', 'LKR', 'LRD', 'LSL', 'LTL', 'LVL', 'LYD', 'MAD', 'MDL', 'MGA', 'MKD', 'MMK', 'MNT', 'MOP', 'MRO', 'MUR', 'MVR', 'MWK', 'MXN', 'MXV', 'MYR', 'MZN', 'NAD', 'NGN', 'NIO', 'NOK', 'NPR', 'NZD', 'OMR', 'PAB', 'PEN', 'PGK', 'PHP', 'PKR', 'PLN', 'PYG', 'QAR', 'RON', 'RSD', 'RUB', 'RWF', 'SAR', 'SBD', 'SCR', 'SDG', 'SEK', 'SGD', 'SHP', 'SLL', 'SOS', 'SRD', 'STD', 'SVC', 'SYP', 'SZL', 'THB', 'TJS', 'TMT', 'TND', 'TOP', 'TRY', 'TTD', 'TWD', 'TZS', 'UAH', 'UGX', 'USD', 'USN', 'USS', 'UYI', 'UYU', 'UZS', 'VEF', 'VND', 'VUV', 'WST', 'XAF', 'XAG', 'XAU', 'XBA', 'XBB', 'XBC', 'XBD', 'XCD', 'XDR', 'XOF', 'XPD', 'XPF', 'XPT', 'XTS', 'XXX', 'YER', 'ZAR', 'ZMK', 'ZWL', 'CNH', name='fromcurrency'),
               comment=None,
               existing_comment='Currency of exchange rate denominator (ISO4217)',
               existing_nullable=False)
    # ### end Alembic commands ###