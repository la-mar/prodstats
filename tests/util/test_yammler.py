import logging
from datetime import datetime

import pytest
import yaml

from util.yammler import Yammler

logger = logging.getLogger(__name__)


@pytest.fixture
def tmpyaml(tmpdir):
    path = tmpdir.mkdir("test").join("yaml.yaml")
    path.write(
        """container:
            example:
                enabled: true
                model: db.models.TestModel
                ignore_unkown: true
            """
    )
    yield path


class TestYammler:
    def test_load_yaml(self, tmpyaml):

        expected = {
            "example": {
                "enabled": True,
                "model": "db.models.TestModel",
                "ignore_unkown": True,
            }
        }

        yml = Yammler(str(tmpyaml))
        assert yml.fspath == str(tmpyaml)
        assert yml["container"] == expected

    def test_dump_to_file(self, tmpdir):
        path = tmpdir.mkdir("test").join("yaml.yaml")
        yml = Yammler(str(path), {"key": "value"})
        yml.dump()

    def test_stamp(self, tmpdir):
        path = tmpdir.mkdir("test").join("yaml.yaml")
        yml = Yammler(str(path), {})
        assert isinstance(yml.stamp(), datetime)

    def test_generic_context_manager(self, tmpyaml):
        with Yammler.context(tmpyaml) as f:
            f["tempvar"] = "tempvalue"

        # open another context to check the result was persisted
        with Yammler.context(tmpyaml) as f:
            assert f["tempvar"] == "tempvalue"

    def test_dump_revert_on_error(self, tmpyaml):
        with Yammler.context(tmpyaml) as f:
            f["tempvar"] = "tempvalue"

        try:
            with Yammler.durable(tmpyaml, "w") as f:
                yaml.safe_dump({"test": "value"}, f, default_flow_style=False)
                raise Exception()
        except Exception:
            with Yammler.context(tmpyaml) as f:
                assert isinstance(f["container"], dict)
