import asyncio
import itertools
from typing import Coroutine, Dict, List

from celery.utils.log import get_task_logger

import calc.prod  # noqa
import cq.signals  # noqa
import db.models as models
import ext.metrics as metrics
from collector import IHSClient
from const import EntityType, HoleDirection, IHSPath
from cq.worker import celery_app
from executors import ProdExecutor

logger = get_task_logger(__name__)


RETRY_BASE_DELAY = 15


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


@celery_app.task
def run_executors(entity12s: List[str]):

    logger.info(f"task id count: {len(entity12s)}")
    pexec = ProdExecutor()
    results, errors = pexec.run_sync(entity12s=entity12s)
    if len(errors) > 0:
        logger.info(f"task returned {len(errors)} errors: {errors}")
    return len(entity12s)


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
def sync_area_manifest():  # TODO: change to use Counties endpoint (and add Counties endpoint to IHS service :/) # noqa
    """ Ensure the local list of areas is up to date """

    loop = asyncio.get_event_loop()

    async def wrapper(path: IHSPath) -> List[Dict]:
        records: List[Dict] = []
        if path.name.endswith("ids"):
            areas = await IHSClient.get_areas(path=path)
            etype, hole_dir = path.name.replace("_ids", "").split("_")
            records = [
                {
                    "path": path,
                    "area": a,
                    "type": EntityType(etype),
                    "hole_direction": HoleDirection(hole_dir.upper()),
                }
                for a in areas
            ]
        return records

    coros: List[Coroutine] = []
    for path in IHSPath.members():
        if path.name.endswith("ids"):
            coros.append(wrapper(path))

    results = loop.run_until_complete(asyncio.gather(*coros))

    area_records = list(itertools.chain(*results))

    coro = models.Area.bulk_upsert(area_records, ignore_on_conflict=True)
    affected = loop.run_until_complete(coro)

    logger.info(f"synchronized manifest: refreshed {affected} areas")


if __name__ == "__main__":

    async def wrapper():
        from db import db

        await db.startup()
