from typing import List

import pytest  # noqa

from parsing import ParserRule
from parsing.criteria import RegexCriterion


@pytest.fixture
def regex_criteria():
    fc = RegexCriterion(r"^[-+]?\d*\.\d+$")
    ic = RegexCriterion(r"^[-+]?[0-9]+$")
    yield [fc, ic]


@pytest.fixture
def rule(regex_criteria):
    yield ParserRule(criteria=regex_criteria, allow_partial=True)


class TestRules:
    def test_rule_repr(self, rule):
        repr(rule)

    def test_rule_return_partials(self, rule):
        assert rule("123.321", return_partials=True) == [True, False]

    def test_get_match_mode(self, rule):
        assert rule.match_mode == "PARTIAL"

    def test_toggle_match_mode(self, rule):
        rule.allow_partial = False
        assert rule.match_mode == "FULL"

    def test_partial_parse(self, rule):
        assert rule("123") is True
        assert rule("132.32") is True
        assert rule("test553.23") is False
        assert rule("55test") is False
        assert rule("55.123test") is False
        assert rule("test55.123") is False

    def test_full_parse(self, regex_criteria: List[RegexCriterion]):
        fc = regex_criteria[0]
        fc2 = regex_criteria[0]
        rule = ParserRule(criteria=[fc, fc2], allow_partial=False)
        assert rule("132.32") is True
        assert rule("test553.23") is False
        assert rule("55test") is False
        assert rule("55.123test") is False
        assert rule("test55.123") is False
