# stdlib imports
import unittest
import itertools
import os


# local imports
from capgains.flex import regexes


def setUpModule():
    """ Load corporate action description data """
    global corpActDescriptions
    path = os.path.join(os.path.dirname(__file__), "data", "corpact_descriptions.txt")
    with open(path) as f:
        corpActDescriptions = f.readlines()


class CorpActReTestCase(unittest.TestCase):
    regex = regexes.corpActRE

    @property
    def descriptions(self):
        # Wrap in property because corpActDescriptions hasn't been defined yet
        # at class creation time
        return corpActDescriptions

    def runRegex(self):
        return [self.regex.match(desc) for desc in self.descriptions]

    def testRegex(self):
        matches = self.runRegex()
        self.assertEqual(len(matches), len(self.descriptions))
        for i, desc in enumerate(self.descriptions):
            split = desc.split("(")
            tail = split[-1].rstrip(")")
            extracted = [t for t in tail.split(",")]
            cusip = extracted.pop().strip().rstrip(")")
            ticker = extracted.pop(0).strip()
            if ticker.startswith("20"):
                ticker = ticker[14:]
            name = ",".join(extracted).strip()
            memo = "(".join(split[:-1]).strip()
            match = matches[i]
            if match is None:
                raise ValueError(desc)
            self.assertEqual(ticker, match.group("ticker"))
            self.assertEqual(name, match.group("secname"))
            self.assertEqual(cusip, match.group("cusip"))
            self.assertEqual(memo, match.group("memo"))


class ChangeSecurityReTestCase(CorpActReTestCase):
    memoSignature = "CUSIP/ISIN CHANGE"
    regex = regexes.changeSecurityRE

    @property
    def descriptions(self):
        return [
            regexes.corpActRE.match(desc).group("memo").strip()
            for desc in corpActDescriptions
            if self.memoSignature in desc
        ]

    def testRegex(self):
        matches = self.runRegex()
        self.assertEqual(len(matches), len(self.descriptions))
        for i, desc in enumerate(self.descriptions):
            split = list(
                itertools.chain.from_iterable([m.split(")") for m in desc.split("(")])
            )[:-1]
            self.assertEqual(len(split), 4)
            match = matches[i]
            tickerFrom, isinFrom, boilerplate, isinTo0 = split
            self.assertEqual(tickerFrom, match.group("tickerFrom"))
            self.assertEqual(isinFrom, match.group("isinFrom"))
            self.assertEqual(isinTo0, match.group("isinTo0"))


class OversubscribeReTestCase(ChangeSecurityReTestCase):
    memoSignature = "OVER SUBSCRIBE "
    regex = regexes.oversubscribeRE

    def testRegex(self):
        matches = self.runRegex()
        self.assertEqual(len(matches), len(self.descriptions))
        for i, desc in enumerate(self.descriptions):
            split = list(
                itertools.chain.from_iterable([m.split(")") for m in desc.split("(")])
            )
            self.assertEqual(len(split), 3)
            ticker, isin, priceCurrency = split
            ticker = ticker[
                ticker.index(self.memoSignature) + len(self.memoSignature) :
            ]
            priceCurrency = priceCurrency.strip().split()
            self.assertEqual(len(priceCurrency), 3)
            price, currency = priceCurrency[1:]

            match = matches[i]
            self.assertEqual(ticker, match.group("ticker"))
            self.assertEqual(isin, match.group("isin"))
            self.assertEqual(price, match.group("price"))
            self.assertEqual(currency, match.group("currency"))


class RightsIssueReTestCase(ChangeSecurityReTestCase):
    memoSignature = "SUBSCRIBABLE RIGHTS ISSUE"
    regex = regexes.rightsIssueRE

    def testRegex(self):
        matches = self.runRegex()
        self.assertEqual(len(matches), len(self.descriptions))
        for i, desc in enumerate(self.descriptions):
            split = list(
                itertools.chain.from_iterable([m.split(")") for m in desc.split("(")])
            )
            self.assertEqual(len(split), 3)
            tickerFrom, isinFrom, ratio = split
            ratio = ratio.strip().split()
            self.assertEqual(len(ratio), 6)
            self.assertEqual(ratio[4], "FOR")
            numerator = ratio[3]
            denominator = ratio[5]
            match = matches[i]
            self.assertEqual(tickerFrom, match.group("tickerFrom"))
            self.assertEqual(isinFrom, match.group("isinFrom"))
            self.assertEqual(numerator, match.group("numerator0"))
            self.assertEqual(denominator, match.group("denominator0"))


class SplitReTestCase(ChangeSecurityReTestCase):
    memoSignature = "SPLIT"
    regex = regexes.splitRE

    def testRegex(self):
        matches = self.runRegex()
        self.assertEqual(len(matches), len(self.descriptions))
        for i, desc in enumerate(self.descriptions):
            split = list(
                itertools.chain.from_iterable([m.split(")") for m in desc.split("(")])
            )
            self.assertEqual(len(split), 3)
            tickerFrom, isinFrom, ratio = split
            ratio = ratio.strip().split()
            self.assertEqual(len(ratio), 4)
            self.assertEqual(ratio[2], "FOR")
            numerator0 = ratio[1]
            denominator0 = ratio[3]
            match = matches[i]
            self.assertEqual(tickerFrom, match.group("tickerFrom"))
            self.assertEqual(isinFrom, match.group("isinFrom"))
            self.assertEqual(numerator0, match.group("numerator0"))
            self.assertEqual(denominator0, match.group("denominator0"))


class StockDividendReTestCase(ChangeSecurityReTestCase):
    memoSignature = "STOCK DIVIDEND"
    regex = regexes.stockDividendRE

    def testRegex(self):
        matches = self.runRegex()
        self.assertEqual(len(matches), len(self.descriptions))
        for i, desc in enumerate(self.descriptions):
            split = list(
                itertools.chain.from_iterable([m.split(")") for m in desc.split("(")])
            )
            self.assertEqual(len(split), 3)
            tickerFrom, isinFrom, ratio = split
            ratio = ratio.strip().split()
            self.assertEqual(len(ratio), 5)
            self.assertEqual(ratio[3], "FOR")
            numerator0 = ratio[2]
            denominator0 = ratio[4]
            match = matches[i]
            self.assertEqual(tickerFrom, match.group("tickerFrom"))
            self.assertEqual(isinFrom, match.group("isinFrom"))
            self.assertEqual(numerator0, match.group("numerator0"))
            self.assertEqual(denominator0, match.group("denominator0"))


class SpinoffReTestCase(ChangeSecurityReTestCase):
    memoSignature = "SPINOFF "
    regex = regexes.spinoffRE

    def testRegex(self):
        matches = self.runRegex()
        self.assertEqual(len(matches), len(self.descriptions))
        for i, desc in enumerate(self.descriptions):
            split = list(
                itertools.chain.from_iterable([m.split(")") for m in desc.split("(")])
            )
            self.assertEqual(len(split), 3)
            tickerFrom, isinFrom, ratios = split
            ratios = [r.split()[-3:] for r in ratios.split(",")]
            match = matches[i]

            ratio = ratios[0]
            numerator0, FOR, denominator0 = ratio
            self.assertEqual(FOR, "FOR")
            self.assertEqual(tickerFrom, match.group("tickerFrom"))
            self.assertEqual(isinFrom, match.group("isinFrom"))
            self.assertEqual(numerator0, match.group("numerator0"))
            self.assertEqual(denominator0, match.group("denominator0"))

            if len(ratios) > 1:
                self.assertEqual(len(ratios), 2)
                ratio = ratios[1]
                numerator1, FOR, denominator1 = ratio
                self.assertEqual(FOR, "FOR")
                self.assertEqual(numerator1, match.group("numerator1"))
                self.assertEqual(denominator1, match.group("denominator1"))


class SubscribeReTestCase(ChangeSecurityReTestCase):
    memoSignature = "SUBSCRIBES TO"
    regex = regexes.subscribeRE

    def testRegex(self):
        matches = self.runRegex()
        self.assertEqual(len(matches), len(self.descriptions))
        for i, desc in enumerate(self.descriptions):
            split = list(
                itertools.chain.from_iterable([m.split(")") for m in desc.split("(")])
            )
            match = matches[i]
            self.assertEqual(split[0], match.group("tickerFrom"))
            self.assertEqual(split[1], match.group("isinFrom"))


class CashMergerReTestCase(ChangeSecurityReTestCase):
    memoSignature = "PER SHARE"
    regex = regexes.cashMergerRE

    def testRegex(self):
        matches = self.runRegex()
        self.assertEqual(len(matches), len(self.descriptions))
        for i, desc in enumerate(self.descriptions):
            split = list(
                itertools.chain.from_iterable([m.split(")") for m in desc.split("(")])
            )
            tickerFrom, isinFrom, *terms = split
            terms = terms[-1].split("FOR ")[-1].split()
            self.assertEqual(len(terms), 4)
            self.assertEqual(terms[2:], ["PER", "SHARE"])
            currency, price = terms[:2]

            match = matches[i]
            self.assertEqual(tickerFrom, match.group("tickerFrom"))
            self.assertEqual(isinFrom, match.group("isinFrom"))
            self.assertEqual(currency, match.group("currency"))
            self.assertEqual(price, match.group("price"))


class KindMergerReTestCase(ChangeSecurityReTestCase):
    memoSignature = "WITH"
    regex = regexes.kindMergerRE

    def testRegex(self):
        matches = self.runRegex()
        self.assertEqual(len(matches), len(self.descriptions))
        for i, desc in enumerate(self.descriptions):
            match = matches[i]

            split = list(
                itertools.chain.from_iterable([m.split(")") for m in desc.split("(")])
            )
            tickerFrom = split[0]
            isinFrom = split[1]

            self.assertEqual(tickerFrom, match.group("tickerFrom"))
            self.assertEqual(isinFrom, match.group("isinFrom"))

            terms = split[-1].replace("MERGED", "").replace("WITH", "").strip()
            terms = [t.split() for t in terms.split(",")]
            term = terms[0]
            self.assertEqual(len(term), 4)
            tickerTo0, numerator0, FOR, denominator0 = term

            self.assertEqual(tickerTo0, match.group("tickerTo0"))
            self.assertEqual(numerator0, match.group("numerator0"))
            self.assertEqual(FOR, "FOR")
            self.assertEqual(denominator0, match.group("denominator0"))

            if len(terms) > 1:
                self.assertEqual(len(terms), 2)
                term = terms[1]

                if len(term) > 1:
                    self.assertEqual(len(term), 4)
                    tickerTo1, numerator1, FOR, denominator1 = term
                    self.assertEqual(term[0], match.group("tickerTo1"))
                    self.assertEqual(numerator1, match.group("numerator1"))
                    self.assertEqual(FOR, "FOR")
                    self.assertEqual(denominator1, match.group("denominator1"))


class CashAndKindMergerReTestCase(ChangeSecurityReTestCase):
    memoSignature = "CASH and STOCK MERGER"
    regex = regexes.cashAndKindMergerRE

    def testRegex(self):
        matches = self.runRegex()
        self.assertEqual(len(matches), len(self.descriptions))
        for i, desc in enumerate(self.descriptions):
            match = matches[i]

            split = list(
                itertools.chain.from_iterable([m.split(")") for m in desc.split("(")])
            )
            self.assertEqual(len(split), 5)
            tickerFrom = split[0]
            isinFrom = split[1]
            self.assertEqual(tickerFrom, match.group("tickerFrom"))
            self.assertEqual(isinFrom, match.group("isinFrom"))

            terms = split[-1].strip().split(" AND ")[0].split()
            self.assertEqual(len(terms), 4)
            tickerTo0, numerator0, FOR, denominator0 = terms

            self.assertEqual(tickerTo0, match.group("tickerTo0"))
            self.assertEqual(numerator0, match.group("numerator0"))
            self.assertEqual(FOR, "FOR")
            self.assertEqual(denominator0, match.group("denominator0"))


class TenderReTestCase(ChangeSecurityReTestCase):
    memoSignature = "TENDERED TO"
    regex = regexes.tenderRE

    def testRegex(self):
        matches = self.runRegex()
        self.assertEqual(len(matches), len(self.descriptions))
        for i, desc in enumerate(self.descriptions):
            match = matches[i]

            split = list(
                itertools.chain.from_iterable([m.split(")") for m in desc.split("(")])
            )
            self.assertEqual(split[0], match.group("tickerFrom"))
            self.assertEqual(split[1], match.group("isinFrom"))
            if split[-1]:
                terms = split[-1].replace("TENDERED TO", "")
            else:
                terms = split[-2]
            terms = terms.strip().split()
            self.assertEqual(terms[0], match.group("tickerTo0"))
            if len(terms) > 1:
                tickerTo0, numerator0, FOR, denominator0 = terms
                self.assertEqual(numerator0, match.group("numerator0"))
                self.assertEqual(FOR, "FOR")
                self.assertEqual(denominator0, match.group("denominator0"))
