import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from util.jsontools import DateTimeEncoder, ObjectEncoder, load_json, to_json

logger = logging.getLogger(__name__)


@pytest.fixture
def kv():
    yield {"key": datetime.utcfromtimestamp(0)}


@pytest.fixture
def datetime_encoder():
    yield DateTimeEncoder()


class TestDatetimeEncoder:
    def test_encode_datetime(self, datetime_encoder):
        data = {"key": datetime.utcfromtimestamp(0)}
        expected = '{"key": "1970-01-01T00:00:00"}'
        result = datetime_encoder.encode(data)
        assert result == expected

    def test_encode_non_datetime(self, datetime_encoder):
        data = {"key": "test123", "key2": "test234"}
        expected = '{"key": "test123", "key2": "test234"}'
        result = datetime_encoder.encode(data)
        assert result == expected

    def test_dump_datetime(self):
        data = {"key": datetime.utcfromtimestamp(0)}
        expected = '{"key": "1970-01-01T00:00:00"}'
        result = json.dumps(data, cls=DateTimeEncoder)
        assert result == expected

    def test_super_class_raise_type_error(self, datetime_encoder):
        with pytest.raises(TypeError):
            datetime_encoder.default(0)

    def test_encode_timedelta(self):
        data = {"test_obj": timedelta(hours=1)}
        expected = '{"test_obj": 3600}'
        result = json.dumps(data, cls=DateTimeEncoder)
        assert result == expected


class TestObjectEncoder:
    def test_encode_with_to_dict_attribute(self):
        class ObjectForEncoding:
            key = "value"

            def to_dict(self):
                return {"key": self.key}

        data = {"test_obj": ObjectForEncoding()}
        expected = '{"test_obj": {"key": "value"}}'
        assert json.dumps(data, cls=ObjectEncoder) == expected

    def test_encode_with_dict_attribute(self):
        class ObjectForEncoding:
            key = "value"

            def dict(self):
                return {"key": self.key}

        data = {"test_obj": ObjectForEncoding()}
        expected = '{"test_obj": {"key": "value"}}'
        assert json.dumps(data, cls=ObjectEncoder) == expected

    def test_encode_with_pathlib_path(self):
        path = Path(".").resolve()
        data = {"path": path}
        expected = json.dumps({"path": str(path)}, cls=ObjectEncoder)
        assert json.dumps(data, cls=ObjectEncoder) == expected

    def test_encode_list(self):
        data = [1, 2, 2, "house", "of", "voodoo"]
        expected = '[1, 2, 2, "house", "of", "voodoo"]'
        assert json.dumps(data, cls=ObjectEncoder) == expected

    def test_encode_set(self):
        data = {1, 2, 3, "you", "dont", "know", "about", "me"}
        expected = json.dumps(list(data), cls=ObjectEncoder)

        assert json.dumps(data, cls=ObjectEncoder) == expected

    def test_encode_scalar(self):
        data = 1
        expected = "1"
        assert json.dumps(data, cls=ObjectEncoder) == expected

    # @pytest.mark.parametrize(
    #     "geom",
    #     [
    #         {"type": "Point", "coordinates": [-102.15990376316262, 31.882545563762434]},
    #         {
    #             "type": "LineString",
    #             "coordinates": [
    #                 [-102.1658373327804, 31.90677101457917],
    #                 [-102.1658377151659, 31.906770789271725],
    #                 [-102.16583857765673, 31.906770392266168],
    #                 [-102.1658401362329, 31.906769325679146],
    #                 [-102.16584303140229, 31.906765711084283],
    #             ],
    #         },
    #     ],
    # )
    # @pytest.importorskip("shapely")
    # def test_encode_geometry(self, geom):
    #     import shapely

    #     g = shapely.geometry.asShape(geom)
    #     actual = json.loads(json.dumps(g, cls=ObjectEncoder))
    #     assert actual == geom


class TestIO:
    def test_json_file(self, tmpdir):
        path = tmpdir.mkdir("test").join("test.json")
        data = {"key": "value"}
        to_json(data, path)
        loaded = load_json(path)
        assert data == loaded
