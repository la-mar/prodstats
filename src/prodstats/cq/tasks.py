import asyncio
import itertools
from typing import Coroutine, List

from celery.utils.log import get_task_logger

import calc.prod  # noqa
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
def calc_prodstats_for_ids(ids: List[str]):
    # print(f"ids {util.jsontools.to_string(ids)}" + "\n\n")
    logger.info(f"processing {len(ids)} ids")
    pexec = ProdExecutor()
    pexec.run_sync(entities=ids)
    return len(ids)


@celery_app.task
def calc_all_prodstats():  # (ids: List[str]):

    """
        get areas
        for area in areas:

            get ids

            chunk ids into separate tasks

            run_sync in each task

    """

    async def get_ids():
        areas = await IHSClient.get_areas(IHSPath.prod_h_ids)
        areas = ["tx-upton", "tx-reagan"]

        coros: List[Coroutine] = []
        for area in areas:
            coros.append(IHSClient.get_ids(area, path=IHSPath.prod_h_ids))

        return await asyncio.gather(*coros)

    loop = asyncio.get_event_loop()
    ids = loop.run_until_complete(get_ids())
    ids = list(itertools.chain(*ids))

    # ids = ["14207C0202511H", "14207C0205231H"]
    ids = [[x] for x in list(util.chunks(ids, n=10))]

    result = calc_prodstats_for_ids.chunks(ids, 10).apply_async()

    logger.warning(f"calculated prodstats for {len(result)} wells ")
