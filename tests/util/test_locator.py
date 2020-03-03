import pytest  # noqa

from db.models import ProdStat  # noqa
from db.models.bases import Base as Model
from util import locate


class TestLocator:
    def test_locate_with_dotted_path(self):
        assert issubclass(locate("db.models.ProdStat"), Model)

    def test_locate_with_context_dict(self):
        assert issubclass(locate("ProdStat", globals()), Model)

    def test_locate_with_context_str(self):
        assert issubclass(locate("ProdStat", "db.models"), Model)

    def test_locate_local_name_without_context(self):
        with pytest.raises(ValueError):
            locate("ProdStat")

    def test_locate_local_name_with_empty_context(self):
        with pytest.raises(ValueError):
            locate("ProdStat", {})
