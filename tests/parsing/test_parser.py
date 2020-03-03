from datetime import datetime

import pytest

from parsing import Parser, ParserRule
from parsing.criteria import RegexCriterion


@pytest.fixture
def rule_defs():
    yield [
        {
            "name": "match_int_float_isodate",
            "allow_partial": True,
            "criteria": [
                {
                    "name": "match_float",
                    "type": "RegexCriterion",
                    "value": "^[-+]?\\d*\\.\\d+$",
                },
                {
                    "name": "match_int",
                    "type": "RegexCriterion",
                    "value": "^[-+]?[0-9]+$",
                },
                {
                    "name": "match_isodate",
                    "type": "RegexCriterion",
                    "value": "^\\d\\d\\d\\d-\\d\\d-\\d\\d$",
                },
                {
                    "name": "match_any_date",
                    "type": "RegexCriterion",
                    "value": "\\d{0,4}[\\/-]\\d{0,4}[\\/-]\\d{0,4}\\s?\\d{0,2}:?\\d{0,2}:?\\d{0,2}\\s?[AMPM]{0,2}",  # noqa
                },
                {
                    "name": "match_bool",
                    "type": "RegexCriterion",
                    "value": "true|false|True|False",
                },
                {"name": "match_empty_string", "type": "RegexCriterion", "value": "^$"},
            ],
        }
    ]


@pytest.fixture
def parser(rule_defs) -> Parser:
    yield Parser.init(rule_defs, name="default")


class TestParser:
    @pytest.mark.parametrize(
        "data,expected",
        [
            ("+00001", 1),
            ("+01", 1),
            ("+1", 1),
            ("+11", 11),
            ("-00001", -1),
            ("-01", -1),
            ("-1", -1),
            ("-11", -11),
            ("+0", 0),
            ("-0", 0),
            ("+00", 0),
            ("-00", 0),
            ("+00000", 0),
            ("-00000", 0),
        ],
    )
    def test_parse_signed_int(self, parser, data, expected):
        assert parser.parse(data) == expected

    @pytest.mark.parametrize(
        "data,expected", [("1", 1), ("10", 10)],
    )
    def test_parse_unsigned_int(self, parser, data, expected):
        assert parser.parse(data) == expected

    @pytest.mark.parametrize(
        "data,expected",
        [
            ("+1.1034", 1.1034),
            ("-1.1034", -1.1034),
            ("+0.1034", 0.1034),
            ("-0.1034", -0.1034),
            ("+11.1034", 11.1034),
            ("+00.1034", 0.1034),
            ("+01.1034", 1.1034),
            ("-11.1034", -11.1034),
            ("-00.1034", -0.1034),
            ("-01.1034", -1.1034),
            ("+1234567890.1034", 1234567890.1034),
            ("-1234567890.1034", -1234567890.1034),
            ("+31.24141", 31.24141),
            ("-101.98853", -101.98853),
        ],
    )
    def test_parse_signed_float(self, parser, data, expected):
        assert parser.parse(data) == expected

    @pytest.mark.parametrize(
        "data,expected",
        [
            ("1.1034", 1.1034),
            ("0.1034", 0.1034),
            ("11.1034", 11.1034),
            ("00.1034", 0.1034),
            ("01.1034", 1.1034),
            ("1234567890.1034", 1234567890.1034),
            ("31.24141", 31.24141),
            ("101.98853", 101.98853),
        ],
    )
    def test_parse_unsigned_float(self, parser, data, expected):
        assert parser.parse(data) == expected

    @pytest.mark.parametrize(
        "data,expected",
        [
            ("2019-01-01", datetime(year=2019, month=1, day=1)),
            ("2019/01/01", datetime(year=2019, month=1, day=1)),
            ("19-01-01", datetime(year=2001, month=1, day=19)),
            ("9/11/2014 12:00:00 AM", datetime(year=2014, month=9, day=11, hour=0)),
            ("9/25/2014 5:00:00 AM", datetime(year=2014, month=9, day=25, hour=5)),
        ],
    )
    def test_parse_datetime(self, parser, data, expected):
        assert parser.parse(data) == expected

    @pytest.mark.parametrize(
        "expected", ["qwe2019-01-01", "2019-01-01rte", "3242019-01-01"],
    )
    def test_ignore_clouded_datetime(self, parser, expected):
        assert parser.parse(expected) == expected

    @pytest.mark.parametrize(
        "expected", ["2019-01"],
    )
    def test_ignore_incomplete_datetime(self, parser, expected):
        assert parser.parse(expected) == expected

    @pytest.mark.parametrize(
        "data,expected",
        [("true", True), ("True", True), ("false", False), ("False", False)],
    )
    def test_parse_bool(self, parser, data, expected):
        assert parser.parse(data) == expected

    def test_converter_silence_error(self, parser):
        value = "a"
        assert parser.try_int(value) is None
        assert parser.try_float(value) is None
        assert parser.try_date(value) is None
        assert parser.try_int(value) is None

    def test_parser_repr(self, parser):
        repr(parser)

    def test_try_date(self, parser):
        assert parser.try_date("2019-01-01") == datetime(2019, 1, 1)

    def test_try_date_handle_none(self, parser):
        assert parser.try_date(None) is None

    def test_parse_many(self, parser):
        expected = ["a", 1, 2]
        actual = parser.parse_many(["a", 1, "2"])
        assert expected == actual

    def test_add_rule(self):
        fc = RegexCriterion(r"^[-+]?\d*\.\d+$")
        fc2 = RegexCriterion(r"^[-+]?\d*\.\d+$")
        rule = ParserRule(criteria=[fc, fc2], allow_partial=False)
        parser = Parser(rules=[rule, rule])
        parser.add_rule(rule)
        assert len(parser.rules) == 3
