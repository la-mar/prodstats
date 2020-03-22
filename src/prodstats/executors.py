import asyncio
import logging
import uuid
from timeit import default_timer as timer
from typing import Coroutine, Dict, List, Optional, Tuple, Union

import pandas as pd

import db.models as models
import exc
import util
from calc.sets import ProdSet
from const import IHSPath

logger = logging.getLogger(__name__)


class BaseExecutor:
    def __init__(self):
        self.metrics: pd.DataFrame = pd.DataFrame(
            columns=["operation", "name", "time", "count"]
        )

    def add_metric(self, operation: str, name: str, time: float, count: int):
        idx_max = self.metrics.index.max()
        idx_max = idx_max + 1 if not pd.isna(idx_max) else 0
        self.metrics = self.metrics.append(
            pd.Series(
                {"operation": operation, "name": name, "time": time, "count": count},
                name=idx_max,
            )
        )


class ProdExecutor(BaseExecutor):
    def __init__(
        self,
        header_kwargs: Dict = None,
        monthly_kwargs: Dict = None,
        stats_kwargs: Dict = None,
        download_kwargs: Dict = None,
        process_kwargs: Dict = None,
    ):
        super().__init__()
        self.model_kwargs = {
            "header": {**(header_kwargs or {})},
            "monthly": {**(monthly_kwargs or {})},
            "stats": {"batch_size": 1000, **(stats_kwargs or {})},
        }
        self.download_kwargs = download_kwargs or {}
        self.process_kwargs = process_kwargs or {}

    async def download(
        self,
        entities: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        entity12s: Union[str, List[str]] = None,
        create_index: bool = True,
        **kwargs,
    ) -> pd.DataFrame:
        kwargs = {**self.download_kwargs, **kwargs}
        try:
            ts = timer()
            df = await pd.DataFrame.prodstats.from_ihs(
                entities=entities,
                api10s=api10s,
                entity12s=entity12s,
                path=IHSPath.prod_h,
                **kwargs,
            )
            exc_time = round(timer() - ts, 2)
            logger.info(f"downloaded {df.shape[0]} records ({exc_time}s)")
            self.add_metric(
                operation="download", name="*", time=exc_time, count=df.shape[0],
            )
            return df
        except Exception as e:
            logger.error(
                f"Error during download: {entities=} {api10s=} {entity12s=} --{e}"
            )
            raise e

    async def process(self, df: pd.DataFrame, **kwargs) -> ProdSet:
        kwargs = {**self.process_kwargs, **kwargs}
        try:
            ts = timer()

            ps = df.prodstats.calc(**kwargs)
            exc_time = round(timer() - ts, 2)

            total_count = sum([x.shape[0] for x in list(ps) if x is not None])
            for name, model, df in ps.items():
                t = round(df.shape[0] * (exc_time / (total_count or 1)), 2)
                logger.info(f"processed {df.shape[0]} {name} records ({t}s)")
                self.add_metric(
                    operation="process", name=name, time=t, count=df.shape[0],
                )
            return ps
        except Exception as e:
            api10s = df.prodstats.column_as_set("api10")
            logger.error(f"Error during persistance: {api10s} -- {e}")
            raise e

    async def _persist(
        self, name: str, model: models.Model, df: pd.DataFrame, **kwargs
    ) -> int:
        ts = timer()

        count = await model.bulk_upsert(df, **kwargs)
        exc_time = round(timer() - ts, 2)
        logger.info(f"persisted {df.shape[0]} {name} records ({exc_time}s)")
        self.add_metric(
            operation="persist", name=name, time=exc_time, count=df.shape[0],
        )
        return count

    async def persist(
        self,
        ps: ProdSet,
        header_kwargs: Dict = None,
        monthly_kwargs: Dict = None,
        stats_kwargs: Dict = None,
    ) -> int:

        try:
            coros = []
            for name, model, df in ps.items():
                kwargs = {}
                if name == "header" and header_kwargs:
                    kwargs = header_kwargs
                elif name == "monthly" and monthly_kwargs:
                    kwargs = monthly_kwargs
                elif name == "stats" and stats_kwargs:
                    kwargs = stats_kwargs

                coros.append(
                    self._persist(
                        name, model, df, **{**self.model_kwargs[name], **kwargs}
                    )
                )
            return sum(await asyncio.gather(*coros))

        except Exception as e:
            api10s = ps.header.prodstats.column_as_set("api10")
            logger.error(f"Error during persistance: {api10s} -- {e}")
            raise e

    async def _run(
        self,
        entities: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        entity12s: Union[str, List[str]] = None,
        return_data: bool = False,
    ) -> Tuple[int, Optional[ProdSet]]:

        exec_id = uuid.uuid4().hex
        logger.warning(f"starting task: {exec_id}")
        try:
            df = await self.download(
                entities=entities, api10s=api10s, entity12s=entity12s
            )
            ps = await self.process(df)
            ct = await self.persist(ps)
            logger.warning(f"completed task: {exec_id}")
            return ct, ps if return_data else None
        except Exception as e:
            logger.error(
                f"{exec_id} run failed with {entities=} {api10s=} {entity12s=} -- {e}"
            )
            raise e

    async def run(
        self,
        entities: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        entity12s: Union[str, List[str]] = None,
        batch_size: int = None,  # max number of ids to process together
        return_data: bool = False,
    ) -> Tuple[List[Tuple[int, Optional[ProdSet]]], List[BaseException]]:

        param_count = sum(
            [entities is not None, api10s is not None, entity12s is not None]
        )

        if param_count > 1:
            raise ValueError(
                f"Only one of [area_name, api10s, entity12s, entities] can be specified"
            )

        elif param_count < 1:
            raise ValueError(
                f"One of [area_name, api10s, entity12s, entities] must be specified"
            )

        batch_size = batch_size or 100

        if entities:
            ids = entities
            id_type = "entities"
        elif api10s:
            ids = api10s
            id_type = "api10s"
        elif entity12s:
            ids = entity12s
            id_type = "entity12s"
        else:
            raise ValueError(f"Could not determine value of id_type")

        # generate coros for all chunks
        coros: List[Coroutine] = []
        for chunk in util.chunks(ids, n=batch_size):
            coros.append(self._run(**{id_type: chunk, "return_data": return_data}))

        logger.info(f"created {len(coros)} coroutines from {len(ids)} {id_type}s")

        return exc.split_errors(await asyncio.gather(*coros, return_exceptions=True))

    def run_sync(
        self, **kwargs
    ) -> Tuple[List[Tuple[int, Optional[ProdSet]]], List[BaseException]]:
        loop = asyncio.get_event_loop()
        coro = self.run(**kwargs)
        return loop.run_until_complete(coro)


if __name__ == "__main__":
    import loggers
    import calc.prod  # noqa
    from db import db  # noqa

    loggers.config(level=20)

    ids = ["14207C0155111H", "14207C0155258418H"]
    ids = ["14207C0202511H", "14207C0205231H"]

    # if not db.is_bound():
    # await db.startup()
    pexec = ProdExecutor()
    self = pexec
    results, errors = pexec.run_sync(entities=ids, batch_size=20, return_data=True)

    affected, prodset = results[0]
