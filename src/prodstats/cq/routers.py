""" Custom routing functions for Celery """

import logging

import config as conf
from const import HoleDirection

logger = logging.getLogger(__name__)


def hole_direction_router(name, args, kwargs, options, task=None, **kw):

    key = options["routing_key"]

    queue = None
    if isinstance(key, HoleDirection):
        queue = f"{conf.project}-{key}".lower()

    logger.debug(f"{key} -> routed to -> {queue}")

    # logger.warning(f""" router parameters:
    #     {name=}
    #     {args=}
    #     {kwargs=}
    #     {options=}
    #     {task=}
    #     {kw=}

    #     -> routed to -> {queue}
    # """)
    return {"queue": queue}
