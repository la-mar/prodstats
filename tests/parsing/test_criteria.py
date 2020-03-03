import pytest  # noqa

from parsing.criteria import Criterion, RegexCriterion, TypeCriterion, ValueCriterion


class TestCriteria:
    def test_criterion_repr_works(self):
        repr(Criterion(lambda x: 1, name="test"))

    def test_regexcriterion_repr_works(self):
        repr(RegexCriterion(r"\w", name="test"))

    def test_criterion_callable(self):
        c = Criterion(lambda x: 1, name="test")
        assert c(1) == 1

    def test_regex_criterion(self):
        rc = RegexCriterion(r"\w")  # noqa
        assert rc("test value") is True

    def test_type_criterion_int(self):
        tc = TypeCriterion(int)
        assert tc(1) is True

    def test_type_criterion_string_is_not_int(self):
        tc = TypeCriterion(str)
        assert tc(1) is False

    def test_value_criterion_parse_value(self):
        vc = ValueCriterion(123)
        assert vc(123) is True
