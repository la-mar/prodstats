from typing import Dict, Optional

from celery import Task
from httpx import HTTPError

import config as conf

# from httpx import (
#     ConnectTimeout,
#     ConnectionClosed,
#     NetworkError,
#     ProtocolError,
#     HTTPError,
# )


class CustomBaseTask(Task):
    autoretry_for = (HTTPError,)
    retry_kwargs = {"max_retries": conf.CELERY_TASK_MAX_RETRIES}
    retry_backoff = True
    retry_backoff_max = 3600
    retry_jitter = True
    exponential_backoff = conf.CELERY_TASK_EXP_BACKOFF

    meta: Optional[Dict] = None
