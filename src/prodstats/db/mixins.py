# type: ignore

from __future__ import annotations

import asyncio
import functools
import logging
from enum import Enum
from timeit import default_timer as timer
from typing import Callable, Coroutine, Dict, List, Optional, Union

import numpy as np
import pandas as pd
from asyncpg.exceptions import DataError, UniqueViolationError
from sqlalchemy.dialects.postgresql.dml import Insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.schema import Constraint

import config as conf
import util


class Operation(Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    UPSERT = "upsert"


logger = logging.getLogger(__name__)


class BulkIOMixin(object):
    @classmethod
    def log_prefix(cls, exc: Exception) -> str:
        return f"({cls.__name__}) {exc.__class__.__name__}"

    @classmethod
    async def execute_statement(
        cls,
        stmt,
        records: List[Dict],
        op_name: str,
        retry_func: Optional[Callable] = None,
    ) -> int:

        try:
            n = len(records)

            ts = timer()
            await stmt.gino.load(cls).all()
            exc_time = round(timer() - ts, 2)
            cls.log_operation(op_name, n, exc_time)

        except (IntegrityError, UniqueViolationError, DataError) as ie:

            if retry_func:
                if n > 1:
                    first_half = records[: n // 2]
                    second_half = records[n // 2 :]
                    first_n = len(first_half)
                    second_n = len(second_half)

                    logger.info(
                        f"{cls.log_prefix}: retrying with fractured records (first_half={first_n} second_half={second_n}) -- {ie}"  # noqa
                    )
                    await retry_func(first_half, batch_size=first_n // 4)
                    await retry_func(second_half, batch_size=second_n // 4)
                else:
                    record = util.reduce(records)
                    record = {k: v for k, v in record.items() if k in cls.pk.names}

                    # include primary key names/values in log message
                    # values can be scrubbed later, if needed
                    logger.error(
                        f"{cls.log_prefix}: {ie} -- primary_key={record}",
                        extra={"primary_key": record},
                    )

            else:  # fail whole batch
                log_records: str = ""

                # print records to log if in debug mode
                if conf.DEBUG:
                    log_records = f"\n{util.jsontools.to_string(records)}\n"

                logger.error(f"{cls.log_prefix}:  {ie} -- {log_records}",)
                raise ie

        except Exception as e:

            log_records: str = ""
            # print records to log if in debug mode
            if conf.DEBUG:
                log_records = f"\n{util.jsontools.to_string(records)}\n"

            logger.exception(f"{cls.log_prefix}: {e} -- {e.args} {log_records}")
            raise e

        return n

    @classmethod
    async def bulk_upsert(
        cls,
        records: List[Dict],
        batch_size: int = 500,
        exclude_cols: list = None,
        update_on_conflict: bool = True,
        ignore_on_conflict: bool = False,
        conflict_constraint: Union[str, Constraint] = None,
        concurrency: int = 50,
        errors: str = "fractionalize",
    ) -> int:
        batch_size = batch_size or len(records)
        exclude_cols = exclude_cols or []
        conflict_constraint = conflict_constraint or cls.__table__.primary_key
        coros: List[Coroutine] = []

        for idx, chunk in enumerate(util.chunks(records, batch_size)):
            op_name = "bulk_upsert"
            chunk = list(chunk)
            stmt = Insert(cls).values(chunk)

            # update these columns when a conflict is encountered
            if ignore_on_conflict:
                stmt = stmt.on_conflict_do_nothing(constraint=conflict_constraint)

            elif update_on_conflict:
                on_conflict_update_cols = [
                    c.name
                    for c in cls.columns
                    if c not in cls.pk and c.name not in exclude_cols
                ]
                stmt = stmt.on_conflict_do_update(
                    constraint=conflict_constraint,
                    set_={
                        k: getattr(stmt.excluded, k) for k in on_conflict_update_cols
                    },
                )

            if errors == "fractionalize":
                partial = functools.partial(
                    cls.bulk_upsert,
                    exclude_cols=exclude_cols,
                    update_on_conflict=update_on_conflict,
                    ignore_on_conflict=ignore_on_conflict,
                    concurrency=concurrency,
                )
            elif errors == "raise":  # placeholder for later
                partial = None
                # TODO: Log failed table_name and primary keys and capture for later reprocessing (no mechanism for this exists yet) # noqa
            else:
                raise ValueError(
                    "Invalid value for 'errors': must be one of [fractionalize, raise]"
                )

            coros.append(
                cls.execute_statement(
                    stmt, records=chunk, op_name=op_name, retry_func=partial
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
        method = method.lower()

        measurements = {
            "tablename": cls.__table__.name,
            "method": method,
            f"{method}_time": exc_time,
        }

        if n > 0:
            measurements[f"{method}s"] = n

            if exc_time > 0:
                measurements[f"{method}s_per_second"] = n / exc_time or 1

        logger.debug(
            f"({cls.__name__}) {method} {n} records ({exc_time}s)", extra=measurements,
        )


class DataFrameMixin(BulkIOMixin):
    @classmethod
    async def df(cls, create_index: bool = True) -> pd.DataFrame:
        records = await cls.query.gino.all()
        df = pd.DataFrame([x.to_dict() for x in records], columns=cls.c.names)
        if create_index:
            df = df.set_index(cls.pk.names)

        return df

    @classmethod
    def _prepare(cls, df: pd.DataFrame, reset_index: bool) -> List[Dict]:

        if reset_index:
            df = df.reset_index()

        df = df.replace([-np.inf, np.inf], np.nan)
        df = df.replace({np.nan: None})  # catch NaT

        # handles NaNs in pandas 1.0+; does NOT catch NaT
        df = df.where(df.notnull(), None)

        # drop rows with any missing primary keys
        ct_before = df.shape[0]
        for col in cls.pk.names:
            if col in df.columns:
                df = df.dropna(subset=[col], how="all")
        ct_after = df.shape[0]
        diff = ct_before - ct_after
        # TODO: capture rows when in debug mode, similar to execute_statement()
        if diff > 0:
            logger.info(
                f"({cls.__name__}) Dropped {diff} records with missing primary keys"
            )

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


# if __name__ == "__main__":

#     async def async_wrapper():

#         from db import db, DATABASE_CONFIG
#         from db.models import User, Model  # noqa

#         await db.set_bind(DATABASE_CONFIG.url)
#         await User.agg.count()
#         await User.pk.values
#         await User.get(3)
#         User
#         # x = await User.query.gino.all()
#         # users = await User.query.gino.load(User).all()
#         [x.to_dict() for x in await User.query.limit(3).offset(2).gino.load(User).all()]
#         # d = await User.create(**dict(api10="yolo", last_name="swag"))
#         # Query specific columns
#         # await User.query.gino.load((User.id, User.api10)).all()

#         # await User.query.gino.load(User).all()
#         await db.func.count(User.columns.names).gino.scalar()

#         from sqlalchemy.sql.functions import count
#         from sqlalchemy import select

#         count_col = count().label("count")
#         user_count = (
#             select([User.api10, User.last_name, count_col])
#             .group_by(User.api10, User.last_name)
#             .alias()
#         )
#         query = user_count.select()
#         await query.gino.all()

#         count_col = count(User.pk).label("count")
#         user_count = select([count_col]).alias()
#         query = user_count.select()

#         # await User.query.with_only_columns().gino.all()
