import logging

import httpx
import pytest

from collector import IHSClient
from tests.utils import MockAsyncDispatch

logger = logging.getLogger(__name__)


pytestmark = pytest.mark.asyncio


base_url = httpx.URL("http://127.0.0.1")


@pytest.fixture
def well_dispatcher():
    yield MockAsyncDispatch(
        {
            "data": [
                {"a": 1, "b": 2, "c": 3},
                {"a": 3, "b": 4, "c": 5},
                {"a": 5, "b": 6, "c": 9},
            ]
        }
    )


@pytest.fixture
def id_dispatcher():
    yield MockAsyncDispatch(
        {
            "data": [
                {"name": "a", "ids": ["a", "b", "c"]},
                {"name": "b", "ids": ["d", "e", "f"]},
                {"name": "c", "ids": ["g", "h", "i"]},
            ]
        }
    )


class TestGetProduction:
    @pytest.mark.parametrize("idname", ["api10s", "entities", "entity12s"])
    async def test_get_production(self, idname, well_dispatcher):
        ids = ["a", "b", "c"]

        kwargs = {
            "path": IHSClient.paths.prod_h,
            "params": {"related": False},
            "dispatch": well_dispatcher,
            idname: ids,
        }

        result = await IHSClient.get_production(**kwargs)
        logger.debug(result)
        x = sum([sum(x.values()) for x in result])
        assert x == 38 * len(ids)
        assert isinstance(result, list)
        assert isinstance(result[0], dict)

    async def test_no_id_opt(self):
        with pytest.raises(ValueError):
            await IHSClient.get_production(path=IHSClient.paths.prod_h)

    async def test_too_many_id_opts(self):
        with pytest.raises(ValueError):
            await IHSClient.get_production(
                path=IHSClient.paths.prod_h, api10s=["a"], entity12s=["b"]
            )


class TestGetWells:
    @pytest.mark.parametrize("idname", ["api10s", "api14s"])
    async def test_get_wells(self, idname, well_dispatcher):
        ids = ["a", "b", "c"]

        kwargs = {
            "path": IHSClient.paths.prod_h,
            "params": {"related": False},
            "dispatch": well_dispatcher,
            idname: ids,
        }

        result = await IHSClient.get_wells(**kwargs)
        logger.debug(result)
        x = sum([sum(x.values()) for x in result])
        assert x == 38 * len(ids)
        assert isinstance(result, list)
        assert isinstance(result[0], dict)

    async def test_no_id_opt(self):
        with pytest.raises(ValueError):
            await IHSClient.get_wells(path=IHSClient.paths.prod_h)

    async def test_too_many_id_opts(self):
        with pytest.raises(ValueError):
            await IHSClient.get_wells(
                path=IHSClient.paths.prod_h, api10s=["a"], api14s=["b"]
            )


class TestGetOther:
    async def test_get_ids_by_area(self, id_dispatcher):

        kwargs = {
            "path": IHSClient.paths.prod_h,
            "dispatch": id_dispatcher,
            "area": "placeholder",
        }
        result = await IHSClient.get_ids_by_area(**kwargs)
        logger.debug(result)
        assert result == ["a", "b", "c"]

    async def test_get_ids_catch_str_path(self):
        with pytest.raises(AttributeError):
            await IHSClient.get_ids(area="placeholder", path="not_an_enum")

    async def test_get_areas_name_only(self, id_dispatcher):

        result = await IHSClient.get_areas(
            path=IHSClient.paths.prod_h, dispatch=id_dispatcher, name_only=True
        )
        logger.debug(result)
        assert result == ["a", "b", "c"]

    async def test_get_areas(self, id_dispatcher):

        result = await IHSClient.get_areas(
            path=IHSClient.paths.prod_h, dispatch=id_dispatcher, name_only=False
        )
        logger.debug(result)
        expected = id_dispatcher._original_body["data"]
        assert result == expected
