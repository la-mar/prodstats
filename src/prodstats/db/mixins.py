# type: ignore

from __future__ import annotations

import logging
from enum import Enum
from timeit import default_timer as timer
from typing import Dict, List

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
    async def bulk_upsert(
        cls,
        records: List[Dict],
        batch_size: int = None,
        exclude_cols: list = None,
        update_on_conflict: bool = True,
        ignore_on_conflict: bool = False,
        # fetch_result: bool = False,
    ) -> int:
        op_name = "bulk_upsert"
        affected: int = 0
        batch_size = batch_size or len(records)
        exclude_cols = exclude_cols or []
        for chunk in util.chunks(records, batch_size):
            ts = timer()
            chunk = list(chunk)
            stmt = Insert(cls).values(chunk)

            # update these columns when a conflict is encountered
            if ignore_on_conflict:
                stmt = stmt.on_conflict_do_nothing(constraint=cls.__table__.primary_key)
                op_name = op_name + "_ignore_on_conflict"
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
                op_name = op_name + "_update_on_conflict"

            try:
                # if fetch_result:
                #     stmt = stmt.returning(cls.__table__)
                # result = await stmt.gino.load(cls).all()
                n = len(chunk)
                await stmt.gino.load(cls).all()
                exc_time = round(timer() - ts, 2)
                cls.log_operation("upsert", op_name, n, exc_time)
                affected += n

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

        return affected

    @classmethod
    async def bulk_insert(cls, records: List[Dict], batch_size: int = None) -> int:

        affected: int = 0
        batch_size = batch_size or len(records)

        for chunk in util.chunks(records, batch_size):
            ts = timer()
            stmt = Insert(cls).values(chunk)
            exc_time = round(timer() - ts, 2)
            n = len(chunk)
            await stmt.gino.load(cls).all()
            cls.log_operation("insert", "insert", n, exc_time)
            affected += n
        return affected

    @classmethod
    def log_operation(cls, method_type: str, method: str, n: int, exc_time: float):
        op_name = method_type.lower()
        measurements = {
            "tablename": cls.__table__.name,
            "method": method,
            f"{op_name}s": n,
            f"{op_name}_time": exc_time,
        }

        if exc_time > 0:
            measurements[f"{op_name}s_per_second"] = n / exc_time or 1

        logger.info(
            f"{cls.__table__.name}.{method}: {op_name}ed {n} records ({exc_time}s)",
            extra=measurements,
        )


if __name__ == "__main__":

    async def placeholder():

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
