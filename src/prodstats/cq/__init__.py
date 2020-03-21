# flake8: noqa
import logging

import config as conf
import cq.signals
import cq.tasks as tasks
import db
import loggers
from const import HoleDirection, IHSPath
from cq.worker import celery_app

logger = logging.getLogger(__name__)


def _setup_collection_tasks(sender, **kwargs):
    """ Schedules a periodic task for each configured endpoint task """
    logger.info(f"setting up collection tasks")
    # for endpoint_name, endpoint in endpoints.items():
    #     for task_name, task in endpoint.tasks.items():
    #         if task.enabled:
    #             name = f"{endpoint_name}:{task_name}"
    #             logger.debug("Registering periodic task: %s", name)
    #             sender.add_periodic_task(
    #                 task.schedule,
    #                 tasks.sync_endpoint.s(endpoint_name, task_name),
    #                 name=name,
    #             )
    #         else:
    #             logger.warning("Task %s is DISABLED -- skipping", name)


def _setup_heartbeat(sender, **kwargs):
    """ Setup the application monitor heartbeat """
    logger.debug("Registering periodic task: %s", "heartbeat")
    # sender.add_periodic_task(
    #     30, tasks.post_heartbeat, name="heartbeat",
    # )
    sender.add_periodic_task(
        30, tasks.log, name="heartbeat",
    )


def _calc_prodstats_for_area(sender, **kwargs):
    sender.add_periodic_task(
        900,
        tasks.calc_prodstats_for_area.s(IHSPath.prod_h_ids, "tx-upton"),
        name="calc_prodstats_for_upton",
    )
    sender.add_periodic_task(
        900,
        tasks.calc_prodstats_for_area.s(IHSPath.prod_h_ids, "tx-reagan"),
        name="calc_prodstats_for_reagan",
    )


def _calc_prodstats_for_hole_direction(sender, **kwargs):
    sender.add_periodic_task(
        60,
        tasks.calc_prodstats_for_hole_direction.s(HoleDirection.h),
        name="calc_prodstats_for_upton",
    )


def _sync_area_manifest(sender, **kwargs):
    sender.add_periodic_task(
        15, tasks.sync_area_manifest, name="sync_area_manifest",
    )


@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    _setup_collection_tasks(sender, **kwargs)
    _setup_heartbeat(sender, **kwargs)
    _calc_prodstats_for_hole_direction(sender, **kwargs)
    _sync_area_manifest(sender, **kwargs)
    # _calc_prodstats_for_area(sender, **kwargs)
