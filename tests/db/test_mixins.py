import logging

import pytest
from asyncpg.exceptions import UniqueViolationError
from sqlalchemy.exc import IntegrityError
from tests.utils import rand_str

from db.models import ProdStat as Model

logger = logging.getLogger(__name__)


pytestmark = pytest.mark.asyncio  # mark all tests as async


class TestMixins:
    async def test_bulk_upsert_update_on_conflict(self, bind):
        ids = [rand_str(length=10) for i in range(1, 5)]
        records = [{"api10": i} for i in ids]
        await Model.bulk_upsert(records, update_on_conflict=True)

        records2 = [{"api10": i} for i in ids]
        await Model.bulk_upsert(records2)

        results = await Model.query.gino.load((Model.api10)).all()

        expected = [(d["api10"]) for d in records2]
        assert results == expected
        # assert sorted(ids) == sorted(await Model.pk.values)

    async def test_bulk_upsert_ignore_on_conflict(self, bind):
        ids = [rand_str(length=10) for i in range(1, 5)]
        records = [{"api10": i} for i in ids]
        await Model.bulk_upsert(
            records, update_on_conflict=False, ignore_on_conflict=True
        )

        records2 = [{"api10": i} for i in ids]
        await Model.bulk_upsert(records2)

        results = await Model.query.gino.load((Model.api10)).all()

        expected = [(d["api10"]) for d in records]
        assert results == expected

    async def test_bulk_insert(self, bind):
        ids = [rand_str(length=10) for i in range(1, 5)]
        records = [{"api10": i} for i in ids]
        await Model.bulk_insert(records)

        results = await Model.query.gino.load((Model.api10)).all()

        expected = [(d["api10"]) for d in records]
        assert results == expected

    async def test_bulk_insert_raise_integrity_error(self, bind):
        ids = [rand_str(length=10) for i in range(1, 5)]
        records = [{"api10": i} for i in ids]
        await Model.bulk_insert(records)

        with pytest.raises((IntegrityError, UniqueViolationError)):
            await Model.bulk_insert(records)

    # async def test_bulk_upsert_handle_data_error(self, bind):
    #     ids = [rand_str(length=10) for i in range(1, 5)]
    #     records = [{"api10": i} for i in ids]
    #     records.append({"api10": 99999999999999999999})
    #     with pytest.raises(DataError):
    #         await Model.bulk_upsert(records)
    #         assert await Model.pk.values == []

    async def test_bulk_upsert_handle_generic_error(self, bind):

        with pytest.raises(Exception):
            records = [{"api10": rand_str()}, {}]
            await Model.bulk_upsert(records)

            assert await Model.pk.values == []
