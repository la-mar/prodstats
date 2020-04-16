import asyncio
import itertools
import math
from typing import Coroutine, Dict, List

import pandas as pd
from celery.utils.log import get_task_logger

import calc.prod  # noqa
import cq.signals  # noqa
import db.models as models
import ext.metrics as metrics
import util
from collector import IHSClient
from const import HoleDirection, IHSPath
from cq.worker import celery_app
from executors import BaseExecutor, GeomExecutor, ProdExecutor, WellExecutor  # noqa

logger = get_task_logger(__name__)


RETRY_BASE_DELAY = 15

BATCH_SIZE: int = 25
TASK_SPREAD_MULTIPLIER: int = 30

# TODO: tenacity?
# TODO: asynchronously fracture failed batches
# TODO: circuit breakers?

# TODO: task metadata


def spread_countdown(n: int, multiplier: int = None) -> float:
    multiplier = multiplier or TASK_SPREAD_MULTIPLIER
    return math.log(n + 1) * multiplier + n


@celery_app.task
def log():
    """Print some log messages"""
    logger.warning("heartbeat")


@celery_app.task
def log_qsize():
    """Print some log messages"""
    from db import db

    logger.warning(f"total db connections: {db.qsize()}")


@celery_app.task
def smoke_test():
    """ Verify an arbitrary Celery task can run """
    return "verified"


@celery_app.task
def post_heartbeat():
    """ Send heartbeat to metrics backend"""
    return metrics.post_heartbeat()


def run(
    hole_dir: HoleDirection,
    executors: List[BaseExecutor],
    api14s: List[str],
    spread_multiplier: int = None,
):
    for idx, chunk in enumerate(util.chunks(api14s, n=BATCH_SIZE)):
        for executor in executors:
            kwargs = {
                "hole_dir": hole_dir,
                "executor_name": executor.__name__,
                "api14s": chunk,
            }
            logger.warning(f"submitting option_set: {kwargs=}")
            run_executor.apply_async(
                args=[],
                kwargs=kwargs,
                countdown=spread_countdown(n=idx, multiplier=spread_multiplier),
            )


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

    run(HoleDirection.H, executors, deo_api14h)

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

    run(HoleDirection.V, executors, deo_api14v)

    # import json
    # from util.jsontools import UniversalEncoder
    # json.loads(json.dumps(HoleDirection.H, cls=UniversalEncoder))


@celery_app.task
def run_executor(hole_dir: HoleDirection, executor_name: str, **kwargs):

    executor = globals()[executor_name]
    count, dataset = executor(hole_dir).run(**kwargs)


@celery_app.task
def run_executors(hole_dir: HoleDirection, id_var: str, ids: List[str]):
    pass
    # logger.info(f"task id count: {len(entity12s)}")
    # pexec = ProdExecutor()
    # results, errors = pexec.run_sync(entity12s=entity12s)
    # if len(errors) > 0:
    #     logger.info(f"task returned {len(errors)} errors: {errors}")
    # return len(entity12s)


@celery_app.task
def run_next_available():
    """ Run next available area """


# @celery_app.task
# def calc_prodstats_for_entity12s(entity12s: List[str]):

#     logger.info(f"task id count: {len(entity12s)}")
#     pexec = ProdExecutor()
#     results, errors = pexec.run_sync(entity12s=entity12s)
#     if len(errors) > 0:
#         logger.info(f"task returned {len(errors)} errors: {errors}")
#     return len(entity12s)


# @celery_app.task
# def calc_prodstats_for_area(
#     path: Union[IHSPath, str], area: str, hole_dir: Union[HoleDirection, str]
# ):
#     max_ids_per_task = conf.CALC_MAX_IDS_PER_TASK
#     loop = asyncio.get_event_loop()

#     if not isinstance(path, IHSPath):
#         path = IHSPath(path)

#     if not isinstance(hole_dir, HoleDirection):
#         hole_dir = HoleDirection(str(hole_dir).upper())

#     ids = loop.run_until_complete(IHSClient.get_ids_by_area(path=path, area=area))
#     ids = ["14207C0202511H", "14207C0205231H"]

#     original_id_count = len(ids)
#     logger.debug(f"original entity count: {original_id_count}")
#     # coerce entities to max length of 12
#     ids = {x[:12] for x in ids}
#     id_count = len(ids)
#     if original_id_count - id_count < 0:
#         logger.debug(
#             f"merged {original_id_count - id_count} duplicate entities during truncation to entity12s"  # noqa
#         )

#     logger.info(f"creating subtasks from {len(ids)} ids -- {ids}")

#     # chunk ids into blocks of 10
#     ids = [[x] for x in list(util.chunks(ids, n=max_ids_per_task))]
#     task_count = len(ids)

#     # create a task for each block of ids
#     calc_prodstats_for_entity12s.chunks(ids, 1).apply_async()

#     async def update_sync_manifest(path: IHSPath, hole_dir: HoleDirection):
#         data_type, _ = path.name.replace("_ids", "").split("_")
#         data_type = EntityType(data_type)
#         obj = await models.Area.get([area, data_type, hole_dir])
#         dt = util.dt.utcnow()
#         await obj.update(last_sync=dt).apply()
#         logger.info(f"Updated area manifest sync time: {data_type} -> {dt}")

#     loop.run_until_complete(update_sync_manifest(path, hole_dir))
#     logger.info(f"calculated prodstats for {id_count} wells in {task_count} tasks")


# @celery_app.task
# def calc_prodstats_for_hole_direction(hole_dir: Union[str, HoleDirection]):
#     loop = asyncio.get_event_loop()

#     async def get_areas(hole_dir: HoleDirection) -> List[Tuple]:

#         areas = await models.Area.query.where(
#             models.Area.hole_direction == hole_dir.value
#         ).gino.all()

#         params: List[Tuple] = []
#         for area in areas:
#             params.append((area.path, area.area, hole_dir))
#         return params

#     if not isinstance(hole_dir, HoleDirection):
#         hole_dir = HoleDirection(str(hole_dir).upper())

#     params = loop.run_until_complete(get_areas(hole_dir))
#     calc_prodstats_for_area.chunks(params, 3).apply_async()


@celery_app.task
def sync_area_manifest():  # FIXME: change to use Counties endpoint (and add Counties endpoint to IHS service :/) # noqa
    """ Ensure the local list of areas is up to date """

    loop = asyncio.get_event_loop()

    async def wrapper(path: IHSPath, hole_dir: HoleDirection) -> List[Dict]:

        records: List[Dict] = []
        areas = await IHSClient.get_areas(path=path, name_only=False)
        records = [{"area": area["name"], "hole_direction": hole_dir} for area in areas]
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

    area_records = pd.DataFrame(itertools.chain(*results)).drop_duplicates()

    coro = models.Area.bulk_upsert(
        area_records, update_on_conflict=True, reset_index=False
    )
    affected = loop.run_until_complete(coro)

    # TODO: delete records from Area if not in new area_records

    logger.info(f"synchronized manifest: refreshed {affected} areas")


if __name__ == "__main__":

    async def wrapper():
        from db import db

        await db.startup()
