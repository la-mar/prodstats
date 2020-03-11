# type: ignore

from __future__ import annotations

import asyncio
import logging
from enum import Enum
from timeit import default_timer as timer
from typing import Coroutine, Dict, List, Union

import numpy as np
import pandas as pd
from asyncpg.exceptions import DataError, UniqueViolationError
from sqlalchemy.dialects.postgresql.dml import Insert
from sqlalchemy.exc import IntegrityError

import util


class Operation(Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    UPSERT = "upsert"


logger = logging.getLogger(__name__)


class BulkIOMixin(object):
    @classmethod
    async def execute_statement(
        cls,
        stmt,
        records: List[Dict] = None,
        op_name: str = None,
        update_on_conflict: bool = True,
        ignore_on_conflict: bool = False,
    ) -> int:
        try:

            if records:
                n = len(records)
            else:
                n = -1

            ts = timer()
            await stmt.gino.load(cls).all()
            exc_time = round(timer() - ts, 2)
            cls.log_operation(op_name, n, exc_time)

        except (IntegrityError, UniqueViolationError, DataError) as ie:
            logger.debug(ie.args[0])

            # fragment and reprocess
            if n > 1:
                first_half = records[: n // 2]
                second_half = records[n // 2 :]
                await cls.bulk_upsert(
                    records=first_half,
                    batch_size=len(first_half) // 4,
                    update_on_conflict=update_on_conflict,
                    ignore_on_conflict=ignore_on_conflict,
                )
                await cls.bulk_upsert(
                    records=second_half,
                    batch_size=len(second_half) // 4,
                    update_on_conflict=update_on_conflict,
                    ignore_on_conflict=ignore_on_conflict,
                )
        except Exception as e:
            logger.error(f"{e.__class__}: {e} -- {e.args}")
            raise e

        return n

    @classmethod
    async def bulk_upsert(
        cls,
        records: List[Dict],
        batch_size: int = 100,
        exclude_cols: list = None,
        update_on_conflict: bool = True,
        ignore_on_conflict: bool = False,
        concurrency: int = 50
        # fetch_result: bool = False,
    ) -> int:
        batch_size = batch_size or len(records)
        exclude_cols = exclude_cols or []
        coros: List[Coroutine] = []

        for idx, chunk in enumerate(util.chunks(records, batch_size)):
            op_name = "bulk_upsert"
            chunk = list(chunk)
            stmt = Insert(cls).values(chunk)

            # update these columns when a conflict is encountered
            if ignore_on_conflict:
                stmt = stmt.on_conflict_do_nothing(constraint=cls.__table__.primary_key)
                op_name = op_name + " (ignore_on_conflict)"
            elif update_on_conflict:
                on_conflict_update_cols = [
                    c.name
                    for c in cls.columns
                    if c not in cls.pk and c.name not in exclude_cols
                ]
                stmt = stmt.on_conflict_do_update(
                    constraint=cls.__table__.primary_key,
                    set_={
                        k: getattr(stmt.excluded, k) for k in on_conflict_update_cols
                    },
                )
                op_name = op_name + " (update_on_conflict)"

            coros.append(
                cls.execute_statement(
                    stmt,
                    records=chunk,
                    op_name=op_name,
                    update_on_conflict=update_on_conflict,
                    ignore_on_conflict=ignore_on_conflict,
                )
            )

        result: int = 0
        for idx, chunk in enumerate(util.chunks(coros, concurrency)):
            result += sum(await asyncio.gather(*chunk))

        return result

    @classmethod
    async def bulk_insert(cls, records: List[Dict], batch_size: int = 100) -> int:

        affected: int = 0
        batch_size = batch_size or len(records)

        for chunk in util.chunks(records, batch_size):
            ts = timer()
            stmt = Insert(cls).values(chunk)
            exc_time = round(timer() - ts, 2)
            n = len(chunk)
            await stmt.gino.load(cls).all()
            cls.log_operation("insert", n, exc_time)
            affected += n
        return affected

    @classmethod
    def log_operation(cls, method: str, n: int, exc_time: float):
        op_name = method.lower()
        measurements = {
            "tablename": cls.__table__.name,
            "method": method,
            f"{op_name}_time": exc_time,
        }

        if n > 0:
            measurements[f"{op_name}s"] = n

            if exc_time > 0:
                measurements[f"{op_name}s_per_second"] = n / exc_time or 1

        logger.info(
            f"{cls.__table__.name}.{method}: {op_name} {n} records ({exc_time}s)",
            extra=measurements,
        )


class DataFrameMixin(BulkIOMixin):
    @staticmethod
    def _prepare(df: pd.DataFrame, reset_index: bool) -> List[Dict]:

        df = df.replace({np.nan: None})  # nan to None

        if reset_index:
            df = df.reset_index()

        return df.to_dict(orient="records")

    @classmethod
    async def bulk_upsert(
        cls, df: Union[pd.DataFrame, List[Dict]], reset_index: bool = True, **kwargs
    ) -> int:
        records: List[Dict] = []
        if isinstance(df, pd.DataFrame):
            records = cls._prepare(df=df, reset_index=reset_index)
        else:
            records = df
        return await super().bulk_upsert(records, **kwargs)

    @classmethod
    async def bulk_insert(
        cls, df: Union[pd.DataFrame, List[Dict]], reset_index: bool = True, **kwargs
    ) -> int:
        records: List[Dict] = []
        if isinstance(df, pd.DataFrame):
            records = cls._prepare(df=df, reset_index=reset_index)
        else:
            records = df
        return await super().bulk_insert(records, **kwargs)


if __name__ == "__main__":

    async def async_wrapper():

        from db import db, DATABASE_CONFIG
        from db.models import User, Model  # noqa

        await db.set_bind(DATABASE_CONFIG.url)
        await User.agg.count()
        await User.pk.values
        await User.get(3)
        User
        # x = await User.query.gino.all()
        # users = await User.query.gino.load(User).all()
        [x.to_dict() for x in await User.query.limit(3).offset(2).gino.load(User).all()]
        # d = await User.create(**dict(api10="yolo", last_name="swag"))
        # Query specific columns
        # await User.query.gino.load((User.id, User.api10)).all()

        # await User.query.gino.load(User).all()
        await db.func.count(User.columns.names).gino.scalar()

        from sqlalchemy.sql.functions import count
        from sqlalchemy import select

        count_col = count().label("count")
        user_count = (
            select([User.api10, User.last_name, count_col])
            .group_by(User.api10, User.last_name)
            .alias()
        )
        query = user_count.select()
        await query.gino.all()

        count_col = count(User.pk).label("count")
        user_count = select([count_col]).alias()
        query = user_count.select()

        # await User.query.with_only_columns().gino.all()
