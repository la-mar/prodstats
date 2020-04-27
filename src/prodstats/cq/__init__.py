# flake8: noqa
import functools
import logging

import config as conf
import cq.signals
import cq.tasks as tasks
import db
import loggers
from const import HoleDirection, IHSPath
from cq.worker import celery_app

logger = logging.getLogger(__name__)


@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    add_task = functools.partial(sender.add_periodic_task)

    add_task(10, tasks.post_heartbeat.s(), name="heartbeat")
    # add_task(30, tasks.log.s(), name="heartbeat")
    add_task(30, tasks.sync_area_manifest.s(), name="sync_area_manifest")
    add_task(120, tasks.run_driftwood.s(HoleDirection.H), name="run_driftwood_h")
    add_task(240, tasks.run_driftwood.s(HoleDirection.V), name="run_driftwood_v")
    add_task(600, tasks.run_next_available.s(HoleDirection.H), name="run_next_h")
    add_task(1800, tasks.run_next_available.s(HoleDirection.V), name="run_next_v")
    # add_task(60, tasks.run_next_available.s(HoleDirection.H), name="run_next_available")
