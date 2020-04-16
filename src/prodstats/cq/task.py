from typing import Dict, Optional

from celery import Task


class CustomBaseTask(Task):
    meta: Optional[Dict] = None
