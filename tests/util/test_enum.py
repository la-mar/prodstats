import pytest

from util.enums import Enum


@pytest.fixture(scope="session")
def enum():
    class StringEnumerator(str, Enum):
        TOPO = "topo chico"
        WATERLOO = "waterloo"
        LACROIX = "lacroix"

    yield StringEnumerator


class TestEnum:
    def test_has_value_existing(self, enum):
        assert enum.has_value("topo chico") is True

    def test_has_value_missing(self, enum):
        assert enum.has_value("dr pepper") is False

    def test_has_member_existing_lower(self, enum):
        assert enum.has_member("topo") is True

    def test_has_member_existing_upper(self, enum):
        assert enum.has_member("TOPO") is True

    def test_has_member_missing(self, enum):
        assert enum.has_member("not_topo") is False

    def test_value_map(self, enum):
        assert enum.value_map() == {
            "topo chico": enum.TOPO,
            "waterloo": enum.WATERLOO,
            "lacroix": enum.LACROIX,
        }

    def test_keys(self, enum):
        assert enum.keys() == ["TOPO", "WATERLOO", "LACROIX"]

    def test_values(self, enum):
        assert enum.values() == ["topo chico", "waterloo", "lacroix"]

    def test_members(self, enum):
        assert enum.members() == [enum.TOPO, enum.WATERLOO, enum.LACROIX]

    def test_iter_class(self, enum):
        assert list(enum) == [enum.TOPO, enum.WATERLOO, enum.LACROIX]
