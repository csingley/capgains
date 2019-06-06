# coding: utf-8
"""
Regular expressions used by importer to process corporate actions.
"""
import re


###############################################################################
# BUILDING BLOCKS
###############################################################################
whitespace = r"\s+"
optionalWhitespace = r"\s*"
whatever = r".*"
# IBKR sometimes randomly prepends time/date stamp to ticker ?!
tickerTo0 = r"(\d{14,14})?(?P<tickerTo0>[\w\.-]+)"
tickerTo1 = r"(\d{14,14})?(?P<tickerTo1>[\w\.-]+)"
price = r"(?P<price>[\d\.]+)"
currency = r"(?P<currency>[A-Z]{3,3})"


# IBKR sometimes randomly prepends time/date stamp to ticker ?!
tickerIsinFrom = r"""
(\d{14,14})?
(?P<tickerFrom>[^(]+)
\(
(?P<isinFrom>[^)]+)
\)\s*
"""


numeratordenominator0 = r"(?P<numerator0>\d+)\ FOR\ (?P<denominator0>\d+)"
numeratordenominator1 = r"(?P<numerator1>\d+)\ FOR\ (?P<denominator1>\d+)"

reason = (
    r"""
\(
(?P<reason>[\w ]+)
\)
"""
    + whitespace
)


def optional(regex):
    return r"({})?".format(regex)


def OR(*args):
    return r"({})".format(r"|".join(args))


whitespace = r"\s+"


###############################################################################
# REGEXES
###############################################################################
secSymbolRE = re.compile(r"(\d{14,14})?(?P<ticker>.+)")


# IBKR sometimes randomly prepends time/date stamp to ticker ?!
corpActRE = re.compile(
    r"(?P<memo>.+)"
    + whitespace
    + r"""
    \( (\d{14,14})?
    (?P<ticker>[^,]+), \s+
    (?P<secname>.+), \s+
    (?P<cusip>[\w]+) \)
    """,
    re.VERBOSE | re.IGNORECASE,
)


changeSecurityRE = re.compile(
    tickerIsinFrom + r"CUSIP/ISIN\ CHANGE\ TO\ \( (?P<isinTo0>[\w \.]+) \)" + whatever,
    re.VERBOSE | re.IGNORECASE,
)


oversubscribeRE = re.compile(
    r"OVER\ SUBSCRIBE\ (?P<ticker>[\w \.-]+)"
    + optionalWhitespace
    + r"\( (?P<isin>\w+) \)"
    + optionalWhitespace
    + "AT"
    + whitespace
    + price
    + whitespace
    + currency
    + whatever,
    re.VERBOSE | re.IGNORECASE,
)


rightsIssueRE = re.compile(
    tickerIsinFrom
    + r"SUBSCRIBABLE\ RIGHTS\ ISSUE"
    + whitespace
    + numeratordenominator0
    + whatever,
    re.VERBOSE | re.IGNORECASE,
)


splitRE = re.compile(
    tickerIsinFrom + r"SPLIT" + whitespace + numeratordenominator0 + whatever,
    re.VERBOSE | re.IGNORECASE,
)


stockDividendRE = re.compile(
    tickerIsinFrom + r"STOCK\ DIVIDEND" + whitespace + numeratordenominator0 + whatever,
    re.VERBOSE | re.IGNORECASE,
)


spinoffRE = re.compile(
    tickerIsinFrom
    + whitespace
    + r"SPINOFF"
    + whitespace
    + optional(tickerTo0)
    + whitespace
    + numeratordenominator0
    + optional(
        r","
        + optionalWhitespace
        + optional(tickerTo1)
        + optionalWhitespace
        + numeratordenominator1
    )
    + whatever,
    re.VERBOSE | re.IGNORECASE,
)


subscribeRE = re.compile(
    tickerIsinFrom
    + r"SUBSCRIBES\ TO"
    + whitespace
    + r"\("
    + optionalWhitespace
    + r"\)"
    + whatever,
    re.VERBOSE | re.IGNORECASE,
)


cashMergerRE = re.compile(
    tickerIsinFrom
    + OR(r"MERGED" + reason, r"ACQUIRED" + whitespace)
    + r"FOR"
    + whitespace
    + currency
    + whitespace
    + price
    + whitespace
    + r"PER\ SHARE"
    + optionalWhitespace,
    re.VERBOSE | re.IGNORECASE,
)


kindMergerRE = re.compile(
    tickerIsinFrom
    + r"MERGED"
    + optional(reason)
    + optionalWhitespace
    + r"WITH"
    + whitespace
    + tickerTo0
    + whitespace
    + numeratordenominator0
    + optional(
        r","
        + optionalWhitespace
        + tickerTo1
        + optional(whitespace + numeratordenominator1)
    )
    + whatever,
    re.VERBOSE | re.IGNORECASE,
)


cashAndKindMergerRE = re.compile(
    tickerIsinFrom
    + r"CASH\ and\ STOCK\ MERGER"
    + optionalWhitespace
    + optional(reason)
    + tickerTo0
    + whitespace
    + numeratordenominator0
    + optional(whitespace + r"AND")
    + whatever,
    re.VERBOSE | re.IGNORECASE,
)


tenderRE = re.compile(
    tickerIsinFrom
    + r"TENDERED\ TO"
    + whitespace
    + optional(r"\(")
    + tickerTo0
    + optional(r"\)")
    + optionalWhitespace
    + optional(numeratordenominator0 + optionalWhitespace),
    re.VERBOSE | re.IGNORECASE,
)
