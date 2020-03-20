import asyncio
import itertools
from typing import Coroutine, Dict, List, Union

from celery.utils.log import get_task_logger

import calc.prod  # noqa
import config as conf
import cq.signals  # noqa
import db.models as models
import ext.metrics as metrics
import util
from collector import IHSClient, IHSPath
from cq.worker import celery_app
from executors import ProdExecutor

logger = get_task_logger(__name__)


RETRY_BASE_DELAY = 15


@celery_app.task
def log():
    """Print some log messages"""
    logger.warning("heartbeat")


@celery_app.task
def smoke_test():
    """ Verify an arbitrary Celery task can run """
    return "verified"


@celery_app.task
def post_heartbeat():
    """ Send heartbeat to metrics backend"""
    return metrics.post_heartbeat()


@celery_app.task
def calc_prodstats(ids: List[str]):

    logger.info(f"task id count: {len(ids)}")
    pexec = ProdExecutor()
    results, errors = pexec.run_sync(entities=ids)
    if len(errors) > 0:
        logger.info(f"task returned {len(errors)} errors: {errors}")
    return len(ids)


@celery_app.task
def calc_prodstats_for_area(path: Union[IHSPath, str], area: str):
    max_ids_per_task = conf.CALC_MAX_IDS_PER_TASK
    loop = asyncio.get_event_loop()

    if not isinstance(path, IHSPath):
        path = IHSPath(path)

    ids = loop.run_until_complete(IHSClient.get_ids_by_area(area=area, path=path))
    # ids = ["14207C0202511H", "14207C0205231H"]

    logger.info(f"creating subtasks from {len(ids)} ids")
    id_count = len(ids)

    # chunk ids into blocks of 10
    ids = [[x] for x in list(util.chunks(ids, n=max_ids_per_task))]
    task_count = len(ids)

    # create a task for each block of ids
    calc_prodstats.chunks(ids, 1).apply_async()

    async def update_sync_manifest(path: IHSPath):
        data_type = path.name.replace("_ids", "")
        obj = await models.Area.get([data_type, area])
        dt = util.dt.utcnow()
        await obj.update(last_sync=dt).apply()
        logger.info(f"Updated area manifest sync time: {data_type} -> {dt}")

    loop.run_until_complete(update_sync_manifest(path))
    logger.info(f"calculated prodstats for {id_count} wells in {task_count} tasks")


@celery_app.task
def calc_all_prodstats():
    pass


@celery_app.task
def sync_area_manifest():
    """ Ensure the local list of areas is up to date """

    loop = asyncio.get_event_loop()

    async def wrapper(path: IHSPath) -> List[Dict]:
        records: List[Dict] = []
        if path.name.endswith("ids"):
            areas = await IHSClient.get_areas(path=path)
            records = [
                {"path": path.name.replace("_ids", ""), "area": a} for a in areas
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
