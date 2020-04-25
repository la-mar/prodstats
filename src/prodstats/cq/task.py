from typing import Dict, Optional

from celery import Task
from httpx import HTTPError

# from httpx import (
#     ConnectTimeout,
#     ConnectionClosed,
#     NetworkError,
#     ProtocolError,
#     HTTPError,
# )


class CustomBaseTask(Task):
    autoretry_for = (HTTPError,)
    retry_kwargs = {"max_retries": 3}
    retry_backoff = True
    retry_backoff_max = 700
    retry_jitter = True
    exponential_backoff = 2

    meta: Optional[Dict] = None
