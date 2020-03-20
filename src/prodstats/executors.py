import asyncio
import logging
import uuid
from timeit import default_timer as timer
from typing import Coroutine, Dict, List, Optional, Tuple, Union

import pandas as pd

import db.models as models
import util
from calc.sets import ProdSet
from collector import IHSPath

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
    ):
        super().__init__()
        self.model_kwargs = {
            "header": {**(header_kwargs or {})},
            "monthly": {**(monthly_kwargs or {})},
            "stats": {"batch_size": 1000, **(stats_kwargs or {})},
        }

    async def download(
        self,
        entities: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        entity12s: Union[str, List[str]] = None,
        create_index: bool = True,
        **kwargs,
    ) -> pd.DataFrame:
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

    async def process(self, df: pd.DataFrame, **kwargs) -> ProdSet:
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

    async def _persist(
        self, name: str, model: models.Model, df: pd.DataFrame, **kwargs
    ) -> int:
        ts = timer()

        # if not db.is_bound():
        #     await db.startup()

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
                self._persist(name, model, df, **{**self.model_kwargs[name], **kwargs})
            )
        return sum(await asyncio.gather(*coros))

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
            return 0, None

    async def run(
        self,
        entities: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        entity12s: Union[str, List[str]] = None,
        batch_size: int = None,  # max number of ids to process together
        return_data: bool = False,
    ) -> List[Tuple[int, Optional[ProdSet]]]:

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

        # schedule coros in batches according to concurrency limits
        # results: List[Tuple[int, Optional[ProdSet]]] = []
        # for chunk in util.chunks(coros, n=concurrency):
        #     logger.info(f"scheduling {len(chunk)} tasks")
        #     results.append(await asyncio.gather(*chunk))

        return await asyncio.gather(*coros)

    def run_sync(self, **kwargs) -> List[Tuple[int, Optional[ProdSet]]]:
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(self.run(**kwargs))


if __name__ == "__main__":
    import loggers
    import calc.prod  # noqa

    loggers.config(level=20)

    ids = ["14207C0155111H", "14207C0155258418H"]
    ids = ["14207C0202511H", "14207C0205231H"]
    ids = [
        "24207C241961",
        "24207C242223",
        "24207C242352",
        "24207C243689",
        "24207C243750",
        "24207C243751",
        "24207C243821",
        "24207C244422",
        "24207C245633",
        "24207C246017",
    ]

    # if not db.is_bound():
    #     await db.startup()
    pexec = ProdExecutor()
    self = pexec
    pexec.run_sync(entities=ids, batch_size=1)

    # TODO: fractionalize failed batches

# pexec.run_sync(area_name="tx-upton", batch_size=25)
# print(pexec.metrics)

# import pandas as pd
# s = pd.Series([str(uuid.uuid4().hex) for x in range(0, 100000)])
# s[s.duplicated()]
# s

# area_name = None
# entities = None
# api10s = None
# entity12s = None
# batch_size = None
# concurrency = None
# return_data = False

# entities = ids

# df = await pd.DataFrame.prodstats.from_ihs(path=IHSPath.prod_h, entities=ids)
# df
