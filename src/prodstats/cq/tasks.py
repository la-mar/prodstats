import asyncio
import itertools
from datetime import timedelta
from typing import Coroutine, Dict, List, Union

import numpy as np
import pandas as pd
from celery.utils.log import get_task_logger
from celery.utils.time import humanize_seconds

import calc.prod  # noqa
import config as conf
import cq.signals  # noqa
import db.models as models
import ext.metrics as metrics
import util
from collector import IHSClient
from const import HoleDirection, IHSPath, Provider
from cq.worker import celery_app
from executors import BaseExecutor, GeomExecutor, ProdExecutor, WellExecutor  # noqa

logger = get_task_logger(__name__)


RETRY_BASE_DELAY = 15

# TODO: add retries
# TODO: tenacity?
# TODO: asynchronously fracture failed batches
# TODO: circuit breakers?

# TODO: task metadata


def log_transform(
    x: float, vs: float = 1, hs: float = 1, ht: float = 0, vt: float = 0, mod: float = 1
) -> float:
    """ Default parameters yield the untransformed natural log curve.

        f(x) = (vs * ln(hs * (x - ht)) + vt) + (x/mod)

        vs = vertical stretch or compress
        hs = horizontal stretch or compress
        ht = horizontal translation
        vt = vertical translation
        mod = modulate growth of curve W.R.T x

     """

    # import pandas as pd
    # import numpy as np
    # import math

    # df = pd.DataFrame(data={"ct": range(0, 200)})
    # df["log"] = df.ct.add(1).apply(np.log10)
    # df["a"] = df.ct.apply(lambda x: 1 * np.log(1 * (x + 0)) + 1)
    # df["b"] = df.ct.apply(lambda x: 50 * np.log(0.25 * (x + 4)) + 0)
    # df["c"] = df.ct.apply(lambda x: 25 * np.log(0.5 * (x + 2)) + 5)
    # df["d"] = df.ct.apply(lambda x: 25 * np.log(1 * (x + 1)) + 5)
    # df = df.replace([-np.inf, np.inf], np.nan)
    # df = df.fillna(0)
    # sample = df.sample(n=25).fillna(0).astype(int).sort_index()
    # # ax = sns.lineplot(data=df)
    # # ax.set_yscale('log')
    # ax.set_xlim(0, 150)
    # ax.set_ylim(0, 150)

    # multiplier = multiplier or conf.TASK_SPREAD_MULTIPLIER

    return np.round((vs * np.log(hs * (x + ht)) + vt) + (x / mod), 2)


def spread_countdown(x: float, vs: float = None, hs: float = None) -> float:
    return log_transform(x=x, vs=vs or 25, hs=hs or 0.25, ht=4, vt=0, mod=4)


def run_executors(
    hole_dir: HoleDirection,
    api14s: List[str] = None,
    api10s: List[str] = None,
    executors: List[BaseExecutor] = None,
    batch_size: int = None,
    log_vs: float = None,
    log_hs: float = None,
):
    executors = executors or [WellExecutor, GeomExecutor, ProdExecutor]
    batch_size = batch_size or conf.TASK_BATCH_SIZE

    if api14s is not None:
        id_name = "api14s"
        ids = api14s
    elif api10s is not None:
        id_name = "api10s"
        ids = api10s
    else:
        raise ValueError(f"One of [api14s, api10s] must be specified")

    # TODO: move chunking to run_executor?
    for idx, chunk in enumerate(util.chunks(ids, n=batch_size)):
        for executor in executors:
            kwargs = {
                "hole_dir": hole_dir,
                "executor_name": executor.__name__,
                id_name: chunk,
            }
            countdown = spread_countdown(idx, vs=log_vs, hs=log_hs)
            logger.info(
                f"submitting task: hole_dir={hole_dir.value} executor={executor.__name__} {id_name}={len(ids)} countdown={countdown}"  # noqa
            )

            run_executor.apply_async(
                args=[], kwargs=kwargs, countdown=countdown,
            )


@celery_app.task
def post_heartbeat():
    """ Send heartbeat to metrics backend"""
    return metrics.post_heartbeat()


@celery_app.task
def run_executor(hole_dir: HoleDirection, executor_name: str, **kwargs):
    # logger.warning(f"running {executor_name=} {hole_dir=} {kwargs=}")
    executor = globals()[executor_name]
    count, dataset = executor(hole_dir).run(**kwargs)


@celery_app.task
def run_next_available(hole_dir: Union[HoleDirection, str], force: bool = False):
    """ Run next available area """

    # TODO: set task meta
    hole_dir = HoleDirection(hole_dir)

    async def coro():
        # await db.startup()
        # hole_dir = HoleDirection.H

        # TODO: move to Router
        if hole_dir == HoleDirection.H:
            ids_path = IHSPath.well_h_ids
        else:
            ids_path = IHSPath.well_v_ids

        area_obj, attr, is_ready, cooldown_hours = await models.Area.next_available(
            hole_dir
        )
        utcnow = util.utcnow()
        prev_run = getattr(area_obj, attr)

        if is_ready or force:
            api14s: List[str] = await IHSClient.get_ids_by_area(
                path=ids_path, area=area_obj.area
            )  # pull from IDMaster once implmented
            api14s = api14s[:10]
            run_executors(hole_dir=hole_dir, api14s=api14s)

            await area_obj.update(**{attr: utcnow}).apply()

            prev_run = (
                prev_run.strftime(util.dt.formats.no_seconds) if prev_run else None
            )
            utcnow = utcnow.strftime(util.dt.formats.no_seconds)
            logger.warning(
                f"({models.Area.__name__}[{hole_dir}]) updated {area_obj.area}.{attr}: {prev_run} -> {utcnow}"  # noqa
            )
        else:
            next_run_in_seconds = (
                (prev_run + timedelta(hours=cooldown_hours)) - utcnow
            ).total_seconds()
            logger.warning(
                f"({models.Area.__name__}[{hole_dir}]) Skipping {area_obj.area} next available for run in {humanize_seconds(next_run_in_seconds)}"  # noqa
            )  # noqa

    return util.aio.async_to_sync(coro())


@celery_app.task
def sync_area_manifest():  # FIXME: change to use Counties endpoint (and add Counties endpoint to IHS service :/) # noqa
    """ Ensure the local list of areas is up to date """

    loop = asyncio.get_event_loop()

    async def wrapper(path: IHSPath, hole_dir: HoleDirection) -> List[Dict]:

        records: List[Dict] = []
        areas = await IHSClient.get_areas(path=path, name_only=False)
        records = [
            {"area": area["name"], "providers": [Provider.IHS]} for area in areas
        ]
        return records

    coros: List[Coroutine] = []
    for args in [
        (IHSPath.well_h_ids, HoleDirection.H),
        (IHSPath.well_v_ids, HoleDirection.V),
        (IHSPath.prod_h_ids, HoleDirection.H),
        (IHSPath.prod_v_ids, HoleDirection.V),
    ]:
        coros.append(wrapper(*args))

    results = loop.run_until_complete(asyncio.gather(*coros))
    inbound_df = pd.DataFrame(list(itertools.chain(*results))).set_index("area")
    inbound_areas = inbound_df.groupby(level=0).first().sort_index()

    existing_areas = util.aio.async_to_sync(models.Area.df()).sort_index()

    # get unique area names that dont already exist
    for_insert = inbound_areas[~inbound_areas.isin(existing_areas)].dropna()

    # for_insert["h_last_run_at"] = util.utcnow()

    if for_insert.shape[0] > 0:
        coro = models.Area.bulk_upsert(
            for_insert,
            update_on_conflict=True,
            reset_index=True,
            conflict_constraint=models.Area.constraints["uq_areas_area"],
        )

        affected = loop.run_until_complete(coro)

        logger.info(
            f"({models.Area.__name__}) synchronized manifest: added {affected} areas"
        )
    else:
        logger.info(f"({models.Area.__name__}) synchronized manifest: no updates")


@celery_app.task
def log():
    """Print some log messages"""
    logger.warning("heartbeat")


@celery_app.task
def smoke_test():
    """ Verify an arbitrary Celery task can run """
    return "verified"


@celery_app.task
def run_driftwood():

    executors = [WellExecutor, GeomExecutor, ProdExecutor]

    deo_api14h = [
        "42461409160000",
        "42383406370000",
        "42461412100000",
        "42461412090000",
        "42461411750000",
        "42461411740000",
        "42461411730000",
        "42461411720000",
        "42461411600000",
        "42461411280000",
        "42461411270000",
        "42461411260000",
        "42383406650000",
        "42383406640000",
        "42383406400000",
        "42383406390000",
        "42383406380000",
        "42461412110000",
        "42383402790000",
    ]

    run_executors(HoleDirection.H, deo_api14h, executors=executors)

    deo_api14v = [
        "42461326620001",
        "42461326620000",
        "42461328130000",
        "42461343960001",
        "42461352410000",
        "42383362120000",
        "42383362080000",
        "42383362090000",
        "42383374140000",
        "42383374130000",
        "42383362060000",
    ]

    run_executors(HoleDirection.V, deo_api14v, executors=executors)


if __name__ == "__main__":
    from db import db
    from db.models import ProdMonthly, ProdStat
    import loggers
    import cq.tasks

    loggers.config()
    hole_dir = HoleDirection.H
    util.aio.async_to_sync(db.startup())
    # cq.tasks.sync_area_manifest.apply_async()

    from util.jsontools import load_json

    api10s = load_json("/Users/friedrichb/Downloads/apilist.json")
    api10s = list(set(api10s))

    api10s_db = util.aio.async_to_sync(
        ProdStat.query.distinct(ProdStat.api10).gino.load(ProdStat.api10).all()
    )
    api10s_db = list(set(api10s_db))
    missing = list({x for x in api10s if x not in api10s_db})
    print(f"input={len(api10s)} missing={len(missing)} in_db={len(api10s_db)}")

    api10s_db = util.aio.async_to_sync(
        ProdMonthly.query.distinct(ProdMonthly.api10)
        .where(ProdMonthly.oil_norm_3k is None)
        .gino.load(ProdMonthly.api10)
        .all()
    )

    len(api10s_db)

    cq.tasks.run_executors(
        hole_dir=hole_dir,
        api10s=api10s,
        executors=[ProdExecutor],
        batch_size=10,
        log_vs=200,
        # log_hs=0.5,
    )

    cq.tasks.run_executors(
        hole_dir=hole_dir, api10s=api10s, executors=[WellExecutor], batch_size=10,
    )

    # import pandas as pd
    # import numpy as np
    # import math

    # df = pd.DataFrame(data={"ct": range(0, 200)})
    # df["log"] = df.ct.add(1).apply(np.log10)
    # df["a"] = df.ct.apply(lambda x: 1 * np.log(1 * (x + 0)) + 1)
    # df["b"] = df.ct.apply(lambda x: 50 * np.log(0.25 * (x + 4)) + 0)
    # df["c"] = df.ct.apply(lambda x: 25 * np.log(0.5 * (x + 2)) + 5)
    # df["d"] = df.ct.apply(lambda x: 25 * np.log(1 * (x + 1)) + 5)
    # df = df.replace([-np.inf, np.inf], np.nan)
    # df = df.fillna(0)
    # sample = df.sample(n=25).fillna(0).astype(int).sort_index()
    # # ax = sns.lineplot(data=df)
    # # ax.set_yscale('log')
    # ax.set_xlim(0, 150)
    # ax.set_ylim(0, 150)

    # f(x) = a * ln(b * (x - c)) + d
    #
    # a = vertical stretch or compress
    # b = horizontal stretch or compress
    # c = translate horizontally
    # d = translate vertically
