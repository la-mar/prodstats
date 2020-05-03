import logging

from celery.signals import (
    after_setup_logger,
    after_setup_task_logger,
    beat_init,
    worker_process_init,
    worker_process_shutdown,
)

import config as conf
import loggers
import util
from db import db

logger = logging.getLogger(__name__)


@after_setup_logger.connect
def setup_logger(logger, *args, **kwargs):
    """ Configure loggers on worker/beat startup """
    loggers.config(
        logger=logger, level=conf.CELERY_LOG_LEVEL, formatter=conf.CELERY_LOG_FORMAT
    )


@after_setup_task_logger.connect
def setup_task_logger(logger, *args, **kwargs):
    """ Configure loggers on worker/beat startup """
    loggers.config(
        logger=logger, level=conf.CELERY_LOG_LEVEL, formatter=conf.CELERY_LOG_FORMAT
    )


@worker_process_init.connect
def init_worker(**kwargs):
    """ Configures each Celery worker process on process startup"""
    # logger.warning(f"init_worker")
    coro = db.startup(  # creates connection pool per worker process
        pool_min_size=conf.CeleryConfig.db_pool_min_size,
        pool_max_size=conf.CeleryConfig.db_pool_max_size,
    )
    util.aio.async_to_sync(coro)


@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    """ Cleans up behind each Celery worker process on process shutdown"""
    # logger.warning(f"shutdown_worker")

    util.aio.async_to_sync(db.shutdown())


@beat_init.connect
def init_beat(**kwargs):
    """ Celery Beat process configuration """
