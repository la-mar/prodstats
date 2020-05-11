import asyncio
import itertools
from datetime import timedelta
from typing import Coroutine, Dict, List, Union

import pandas as pd
from celery.utils.log import get_task_logger
from celery.utils.time import humanize_seconds

import calc.prod  # noqa
import config as conf
import cq.signals  # noqa
import cq.util
import db.models
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

# TODO: add task meta


@celery_app.task
def log():
    """Print some log messages"""
    logger.warning("task-check")


@celery_app.task
def smoke_test():
    """ Verify an arbitrary Celery task can run """
    return "verified"


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
        raise ValueError("One of [api14s, api10s] must be specified")

    # TODO: move chunking to run_executor?
    for idx, chunk in enumerate(util.chunks(ids, n=batch_size)):
        for executor in executors:
            kwargs = {
                "hole_dir": hole_dir,
                "executor_name": executor.__name__,
                id_name: chunk,
            }
            countdown = cq.util.spread_countdown(idx, vs=log_vs, hs=log_hs)
            logger.info(
                f"({executor.__name__}[{hole_dir.value}]) submitting task: {id_name}={len(chunk)} countdown={countdown}"  # noqa
            )

            run_executor.apply_async(
                args=[],
                kwargs=kwargs,
                countdown=countdown,
                ignore_result=False,
                routing_key=hole_dir,
            )


@celery_app.task(is_eager=True)
def post_heartbeat():
    """ Send heartbeat to metrics backend"""
    return metrics.post_heartbeat()


@celery_app.task
def run_executor(hole_dir: HoleDirection, executor_name: str, **kwargs):
    # logger.warning(f"running {executor_name=} {hole_dir=} {kwargs=}")
    executor = globals()[executor_name]
    count, dataset = executor(hole_dir).run(**kwargs)


@celery_app.task
def run_next_available(
    hole_dir: Union[HoleDirection, str], force: bool = False, **kwargs
):
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

        area_obj, attr, is_ready, cooldown_hours = await db.models.Area.next_available(
            hole_dir
        )
        utcnow = util.utcnow()
        prev_run = getattr(area_obj, attr)

        if is_ready or force:
            api14s: List[str] = await IHSClient.get_ids_by_area(
                path=ids_path, area=area_obj.area
            )  # pull from IDMaster once implmented
            # api14s = api14s[:10]
            run_executors(hole_dir=hole_dir, api14s=api14s, **kwargs)

            await area_obj.update(**{attr: utcnow}).apply()

            prev_run = (
                prev_run.strftime(util.dt.formats.no_seconds) if prev_run else None
            )
            utcnow = utcnow.strftime(util.dt.formats.no_seconds)
            logger.warning(
                f"({db.models.Area.__name__}[{hole_dir}]) updated {area_obj.area}.{attr}: {prev_run} -> {utcnow}"  # noqa
            )
        else:
            next_run_in_seconds = (
                (prev_run + timedelta(hours=cooldown_hours)) - utcnow
            ).total_seconds()
            logger.warning(
                f"({db.models.Area.__name__}[{hole_dir}]) Skipping {area_obj.area} next available for run in {humanize_seconds(next_run_in_seconds)}"  # noqa
            )  # noqa

    return util.aio.async_to_sync(coro())


@celery_app.task()
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

    existing_areas = util.aio.async_to_sync(db.models.Area.df()).sort_index()

    # get unique area names that dont already exist
    for_insert = inbound_areas[~inbound_areas.isin(existing_areas)].dropna()

    # for_insert["h_last_run_at"] = util.utcnow()

    if for_insert.shape[0] > 0:
        coro = db.models.Area.bulk_upsert(
            for_insert,
            update_on_conflict=True,
            reset_index=True,
            conflict_constraint=db.models.Area.constraints["uq_areas_area"],
        )

        affected = loop.run_until_complete(coro)

        logger.info(
            f"({db.models.Area.__name__}) synchronized manifest: added {affected} areas"
        )
    else:
        logger.info(f"({db.models.Area.__name__}) synchronized manifest: no updates")


@celery_app.task
def sync_known_entities(hole_dir: HoleDirection):

    hole_dir = HoleDirection(hole_dir)

    if hole_dir == HoleDirection.H:
        path = IHSPath.well_h_ids
    else:
        path = IHSPath.well_v_ids

    areas: List[Dict] = util.aio.async_to_sync(IHSClient.get_areas(path=path))

    for idx, area in enumerate(areas):
        sync_known_entities_for_area.apply_async(
            args=(hole_dir, area), kwargs={}, countdown=idx + 30
        )


@celery_app.task
def sync_known_entities_for_area(hole_dir: HoleDirection, area: str):
    async def wrapper(hole_dir: HoleDirection, area: str):
        hole_dir = HoleDirection(hole_dir)
        index_cols = ["entity_id", "entity_type"]

        if hole_dir == HoleDirection.H:
            path = IHSPath.well_h_ids
        else:
            path = IHSPath.well_v_ids

        # fetch ids from remote service
        ids = await IHSClient.get_ids_by_area(path, area=area)
        df = pd.Series(ids, name="entity_id").to_frame()
        df["ihs_last_seen_at"] = util.utcnow()
        df["entity_type"] = "api14"
        df = df.set_index(index_cols)

        # query matching records existing in the known_entities model
        objs: List[db.models.KnownEntity] = await db.models.KnownEntity.query.where(
            db.models.KnownEntity.entity_id.in_(ids)
        ).gino.all()

        obj_df = pd.DataFrame([x.to_dict() for x in objs]).set_index(index_cols)

        fresh = pd.DataFrame(index=obj_df.index, columns=obj_df.columns)

        # merge the records, prioritizing new values from the remote service
        combined = fresh.combine_first(df).combine_first(obj_df)
        combined = combined.drop(columns=["created_at", "updated_at"])

        # persist the new records
        await db.models.KnownEntity.bulk_upsert(combined, batch_size=1000)

    util.aio.async_to_sync(wrapper(hole_dir, area))


@celery_app.task
def run_for_apilist(
    hole_dir: HoleDirection,
    api14s: List[str] = None,
    api10s: List[str] = None,
    **kwargs,
):

    run_executors(HoleDirection.H, api14s=api14s, api10s=api10s, **kwargs)


@celery_app.task
def run_driftwood(hole_dir: HoleDirection, **kwargs):

    hole_dir = HoleDirection(hole_dir)

    executors = [WellExecutor, GeomExecutor, ProdExecutor]

    if hole_dir == HoleDirection.H:
        api14s = [
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

    elif hole_dir == HoleDirection.V:
        api14s = [
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

    else:
        raise ValueError(f"Invalid hole direction: {hole_dir=}")

    run_executors(hole_dir, api14s=api14s, executors=executors, **kwargs)


if __name__ == "__main__":
    from db import db

    util.aio.async_to_sync(db.startup())
    import db.models
    import loggers
    import cq.tasks
    from const import HoleDirection

    loggers.config()
    hole_dir = HoleDirection.H
    # cq.tasks.sync_area_manifest.apply_async()
    # cq.tasks.run_next_available(HoleDirection.H, log_vs=10, log_hs=None)

    api14s = [
        "42475014800000",
        "42475014810000",
        "42475014810001",
        "42475014820000",
        "42475014820001",
        "42475014830000",
        "42475014840000",
        "42475014850000",
        "42475014860000",
        "42475014860001",
        "42475014860002",
        "42475014870000",
        "42475014870001",
        "42475014880000",
        "42475014880001",
        "42475014890000",
        "42475014890001",
        "42475014900000",
        "42475014900001",
        "42475014900002",
        "42475014910000",
        "42475014910001",
        "42475014920000",
        "42475014920001",
        "42475014920002",
    ]

    holedir = HoleDirection.V

    async def run_wells(holedir: HoleDirection, api14s: List[str]):
        wexec = WellExecutor(holedir)
        wellset = await wexec.download(api14s=api14s)
        wellset = await wexec.process(wellset)
        await wexec.persist(wellset)

    async def run_geoms(holedir: HoleDirection, api14s: List[str]):
        gexec = GeomExecutor(holedir)
        geomset = await gexec.download(api14s=api14s)
        geomset = await gexec.process(geomset)
        await gexec.persist(geomset)

    loggers.config(formatter="funcname")

    async def run_production(holedir: HoleDirection, api14s: List[str]):
        pexec = ProdExecutor(holedir)
        prodset = await pexec.download(api14s=api14s)
        prodset = await pexec.process(prodset)
        await pexec.persist(prodset)

    async def async_wrapper():
        hole_dir = HoleDirection.H
        IHSPath.well_h_ids

        sync_known_entities_for_area(hole_dir, "tx-upton")
