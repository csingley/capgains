# coding: utf-8
"""
"""
# stdlib imports
import re


# 3rd party imports
import ofxtools


# Local imports


transferMemoRE = re.compile(
    r"""
    (?P<memo>.+)
    \s+
    \( (?P<ticker>.+), \s+ (?P<secname>.+), \s+ (?P<uniqueid>[\w]+) \)
    """,
    re.VERBOSE | re.IGNORECASE,
)

retofcapMemoRE = re.compile(
    r"""
    (?P<memo>.+)
    \s+
    \(Return\ of\ Capital\)
    """,
    re.VERBOSE | re.IGNORECASE,
)


handlers = {
    ofxtools.models.INVBUY: "trade",
    ofxtools.models.INVSELL: "trade",
    ofxtools.models.INVBANKTRAN: "invbanktran",
    ofxtools.models.INCOME: "income",
    ofxtools.models.INVEXPENSE: "invexpense",
    ofxtools.models.TRANSFER: "transfer",
}


def applyIbkrQuirks(transactions, securities):
    # Find INCOME; match return of capital in memo; apply reversal; change to RETCAP
    # Find TRANSFER, group by memo, parse memo to infer type;
    # dispatch by type to handler:
    # Split
    # Spinoff
    # Delist
    # CUSIP/ISIN change
    # Over subscribe
    # Subscribable rights issue
    # Stock dividend
    # Rights subscription
    # Merged
    # Tender
    return transactions
