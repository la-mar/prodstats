import asyncio

from celery.utils.log import get_task_logger

import ext.metrics as metrics
import util.jsontools
from collector import IHSClient, IHSPath
from cq.worker import celery_app

logger = get_task_logger(__name__)


RETRY_BASE_DELAY = 15

# async_to_sync(db.set_bind(conf.DATABASE_CONFIG.url))
# FIXME: Use syncronous ORM within celery task if cant find a way to initialize
# async bind once per worker process


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
def calc_prodstats():  # (ids: List[str]):

    """
        get areas
        for area in areas:

            get ids

            chunk ids into separate tasks

            run_sync in each task

    """

    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(IHSClient.get_areas(IHSPath.prod_h_ids))
    logger.warning(f"prodstats result: {util.jsontools.to_string(result)}")
