import logging

import pandas as pd
import pytest
from asyncpg.exceptions import DataError, UniqueViolationError
from sqlalchemy.exc import IntegrityError

from db.models import ProdStat as Model
from tests.utils import rand_str

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
class TestMixins:
    async def test_bulk_upsert_update_on_conflict(self, bind):
        ids = [(rand_str(length=10), rand_str(length=20)) for i in range(1, 5)]
        records = [{"api10": i, "name": v} for i, v in ids]
        await Model.bulk_upsert(records, update_on_conflict=True)

        records2 = [{"api10": i, "name": v} for i, v in ids]
        await Model.bulk_upsert(records2)

        results = await Model.query.gino.load((Model.api10, Model.name)).all()

        expected = [(d["api10"], d["name"]) for d in records2]
        assert results == expected
        # assert sorted(ids) == sorted(await Model.pk.values)

    async def test_bulk_upsert_ignore_on_conflict(self, bind):
        ids = [(rand_str(length=10), rand_str(length=20)) for i in range(1, 5)]
        records = [{"api10": i, "name": v} for i, v in ids]
        await Model.bulk_upsert(
            records, update_on_conflict=False, ignore_on_conflict=True
        )

        records2 = [{"api10": i, "name": v} for i, v in ids]
        await Model.bulk_upsert(records2)

        results = await Model.query.gino.load((Model.api10, Model.name)).all()

        expected = [(d["api10"], d["name"]) for d in records]
        assert results == expected

    async def test_bulk_insert(self, bind):
        ids = [(rand_str(length=10), rand_str(length=20)) for i in range(1, 5)]
        records = [{"api10": i, "name": v} for i, v in ids]
        await Model.bulk_insert(records)

        results = await Model.query.gino.load((Model.api10, Model.name)).all()

        expected = [(d["api10"], d["name"]) for d in records]
        assert results == expected

    async def test_bulk_insert_raise_integrity_error(self, bind):
        ids = [(rand_str(length=10), rand_str(length=20)) for i in range(1, 50)]
        records = [{"api10": i, "name": v} for i, v in ids]
        await Model.bulk_insert(records)

        with pytest.raises((IntegrityError, UniqueViolationError)):
            await Model.bulk_insert(records)

    async def test_bulk_upsert_fracture_on_data_error(self, bind):
        ids = [(rand_str(length=10), rand_str(length=20)) for i in range(1, 11)]
        good = [{"api10": i, "name": v} for i, v in ids]
        mixed = good + [
            {"api10": 99999999999999999999, "name": "lets hope this record fails"}
        ]

        expected = [tuple(x.values()) for x in good]
        await Model.bulk_upsert(mixed, errors="fractionalize")
        assert len(await Model.pk.values) == len(expected)

    async def test_bulk_upsert_fail_batch_on_data_error(self, bind):
        ids = [(rand_str(length=10), rand_str(length=20)) for i in range(1, 11)]
        good = [{"api10": i, "name": v} for i, v in ids]
        mixed = good + [
            {"api10": 99999999999999999999, "name": "lets hope this record fails"}
        ]

        with pytest.raises(DataError):
            await Model.bulk_upsert(mixed, errors="raise")
            assert len(await Model.pk.values) == 0

    async def test_bulk_upsert_handle_generic_error(self, bind):

        with pytest.raises(Exception):
            records = [
                {"api10": rand_str(), "name": rand_str()},
                {"api10": 9999999999999999, "name": rand_str()},
            ]
            await Model.bulk_upsert(records)

            assert await Model.pk.values == []


class TestDataFrameMixin:
    @pytest.fixture
    def records(self):
        yield [
            {"api10": "22222", "name": "test1", "value": 1, "other_value": "v"},
            {"api10": "11111", "name": "test2", "value": 2, "other_value": "v"},
        ]

    def test_prepare_df_keep_index(self, records):
        df = pd.DataFrame(records)

        assert records == Model._prepare(df, reset_index=False)

    def test_prepare_df_reset_index(self, records):
        df = pd.DataFrame(records).set_index("name")

        assert records == Model._prepare(df, reset_index=True)

    @pytest.mark.asyncio
    async def test_bulk_upsert(self, bind):
        ids = [(rand_str(length=10), rand_str(length=20)) for i in range(1, 5)]
        records = [{"api10": i, "name": v} for i, v in ids]
        await Model.bulk_upsert(
            pd.DataFrame(records), reset_index=False, update_on_conflict=True
        )

        records2 = [{"api10": i, "name": v} for i, v in ids]
        await Model.bulk_upsert(pd.DataFrame(records2), reset_index=False)

        results = await Model.query.gino.load((Model.api10, Model.name)).all()

        expected = [(d["api10"], d["name"]) for d in records2]
        assert results == expected

    @pytest.mark.asyncio
    async def test_bulk_insert(self, bind):
        ids = [(rand_str(length=10), rand_str(length=20)) for i in range(1, 5)]
        records = [{"api10": i, "name": v} for i, v in ids]
        await Model.bulk_insert(pd.DataFrame(records), reset_index=False)

        results = await Model.query.gino.load((Model.api10, Model.name)).all()

        expected = [(d["api10"], d["name"]) for d in records]
        assert results == expected
