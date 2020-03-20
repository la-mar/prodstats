import asyncio
from typing import List, Union

from celery.utils.log import get_task_logger

import calc.prod  # noqa
import config as conf
import ext.metrics as metrics
import util
import util.jsontools
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

    logger.warning(f"task id count: {len(ids)}")
    pexec = ProdExecutor()
    pexec.run_sync(entities=ids)
    return len(ids)


@celery_app.task
def calc_all_prodstats(
    path: Union[IHSPath, str], areas: List[str] = None
):  # (ids: List[str]):

    """
        get areas
        for area in areas:

            get ids

            chunk ids into separate tasks

            run_sync in each task

    """
    logger.warning(f"path: {path}")
    if not isinstance(path, IHSPath):
        path = IHSPath(path)
    loop = asyncio.get_event_loop()
    ids = loop.run_until_complete(IHSClient.get_all_ids(path=path, areas=areas))
    # ids = ["14207C0202511H", "14207C0205231H"]

    max_ids_per_task = conf.CALC_MAX_IDS_PER_TASK
    logger.warning(f"creating subtasks from {len(ids)} ids")
    id_count = len(ids)

    # chunk ids into blocks of 10
    ids = [[x] for x in list(util.chunks(ids, n=max_ids_per_task))]
    # ids = [x for x in list(util.chunks(ids, n=max_ids_per_task))]
    task_count = len(ids)

    # create a task for each block of ids
    calc_prodstats.chunks(ids, 1).apply_async()
    # for id in ids:
    #     calc_prodstats.s(ids=ids).apply_async()
    # logger.warning(f"results: {util.jsontools.to_string(ids)}")
    logger.warning(f"calculated prodstats for {id_count} wells in {task_count} tasks")
