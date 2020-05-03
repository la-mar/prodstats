import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

import dateutil.parser

import config as conf
from parsing.rules import ParserRule
from util.deco import safe_convert

logger = logging.getLogger(__name__)


class Parser:
    """ Parses text values according to a set of arbitrary rules """

    def __init__(
        self, rules: List[ParserRule], name: str = None, parse_dtypes: bool = True
    ):
        self.name = name or ""
        self.rules = rules
        self.parse_dtypes = parse_dtypes

    def __repr__(self):
        return f"Parser - {self.name}: {len(self.rules)} rules"

    @classmethod
    def init(cls, ruleset: Dict[str, List], name: str = None) -> "Parser":
        """ Initialize from a configuration dict """
        rules: List[ParserRule] = []
        for ruledef in ruleset:
            rules.append(ParserRule.from_list(**ruledef))  # type: ignore
        return cls(rules, name=name)

    @staticmethod
    @safe_convert
    def try_int(s: str) -> int:
        return int(s)

    @staticmethod
    @safe_convert
    def try_float(s: str) -> float:
        return float(s)

    @staticmethod
    @safe_convert
    def try_date(s: str) -> Optional[datetime]:
        if s is not None:
            return dateutil.parser.parse(s)

    @staticmethod
    @safe_convert
    def try_bool(s: str) -> Union[bool, str]:
        value = str(s)
        if value.lower() == "true":
            return True
        elif value.lower() == "false":
            return False
        else:
            return s

    @staticmethod
    @safe_convert
    def try_empty_str_to_none(s: str) -> Union[None, str]:
        return None if s == "" else s

    def add_rule(self, rule: ParserRule):
        self.rules.append(rule)

    def run_checks(
        self, value: Any, return_partials: bool = False
    ) -> Union[bool, List[bool]]:
        """ Check if all parsing rules are satisfied """
        checks = []
        for Rule in self.rules:
            result = Rule(value)
            checks.append(result)
            if not result:
                logger.debug("Parser check failed: %s", (Rule,))
            else:
                logger.debug("Parser check passed: %s", (Rule,))

        return all(checks) if not return_partials else checks  # type: ignore

    def parse_dtype(self, value: str) -> Union[int, float, str, datetime]:
        funcs = [
            "try_int",
            "try_float",
            "try_date",
            "try_bool",
        ]

        for fname in funcs:
            func = getattr(self, fname)
            newvalue = func(value)
            logger.debug(
                "Parsed dtype: %s -> %s (%s)",
                value or "None",
                newvalue or "None",
                type(newvalue).__name__,
            )
            if not isinstance(newvalue, str) and newvalue is not None:
                value = newvalue
                break

        value = self.try_empty_str_to_none(value)
        return value

    def parse(self, value: Any) -> Any:
        """ Attempt to parse a value if all checks are satisfied """
        if not self.run_checks(value):
            return value
        else:
            return self.parse_dtype(value) if self.parse_dtypes else value

    def parse_many(self, values: List[Any]) -> List[Any]:
        return [self.parse(v) for v in values]


if __name__ == "__main__":

    parser = Parser.init(
        conf.PARSER_CONFIG["parsers"]["default"]["rules"], name="default"
    )

    test_values = [
        "1.1034",
        "0.1034",
        "11.1034",
        "00.1034",
        "01.1034",
        "1234567890.1034",
        "31.24141",
        "101.98853",
    ]

    logger.setLevel(20)
    results = []
    for value in test_values:
        new_value = parser.parse(value)
        new_value = new_value if new_value is not None else "-"
        value = value if value is not None else "-"
        results.append(new_value)
        print(f"{value:<20} -> {new_value} ({type(new_value).__name__})")
