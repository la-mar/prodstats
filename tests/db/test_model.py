import logging

import pytest
from sqlalchemy import String
from tests.utils import rand_str

from db.models import ProdHeader as SinglePKModel
from db.models import ProdStat as Model
from db.models.bases import Base

logger = logging.getLogger(__name__)

# pytestmark = pytest.mark.asyncio  # mark all tests as async


class TestModel:
    @pytest.mark.asyncio
    async def test_create_instance(self, bind):
        x = rand_str(length=10)
        result = await Model.create(api10=x, name=rand_str(length=20))
        assert result.to_dict()["api10"] == x

    def test_model_repr(self):
        repr(Model())

    def test_columns_property_alias(self):
        assert Model.c == Model.columns

    def test_base_repr(self):
        repr(Base)


class TestPrimaryKeyProxy:
    def test_access_pk_names(self, bind):
        assert Model.pk.names == ["api10", "name"]

    def test_pk_repr(self, bind):
        assert repr(Model.pk)

    @pytest.mark.asyncio
    async def test_pk_values(self, bind):
        ids = [(rand_str(length=10), rand_str(length=20)) for i in range(1, 5)]
        for i, v in ids:
            await Model.create(api10=i, name=v)
        assert sorted(await Model.pk.values) == sorted(ids)

    @pytest.mark.asyncio
    async def test_pk_values_single_pk(self, bind):
        ids = [(rand_str(length=10)) for i in range(1, 5)]
        for i in ids:
            await SinglePKModel.create(api10=i, entity12=i)
        assert {*(await SinglePKModel.pk.values)} == {*ids}


class TestAggregateProxy:
    @pytest.mark.asyncio
    async def test_agg_count(self, bind):
        ids = [(rand_str(length=10), rand_str(length=20)) for i in range(1, 5)]
        for i, v in ids:
            await Model.create(api10=i, name=v)
        result = await Model.agg.count()
        assert result == len(ids)

    def test_agg_repr(self):
        repr(Model.agg)


class TestColumnProxy:
    def test_get_column_pytypes(self):
        assert Model.c.pytypes["api10"] == str

    def test_get_column_dtype(self):
        assert isinstance(Model.c.dtypes["api10"], String)
