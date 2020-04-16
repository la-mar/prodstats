""" Initialize Celery worker """

from celery import Celery

from config import CeleryConfig
from cq.task import CustomBaseTask

celery_app: Celery = Celery("worker", task_cls=CustomBaseTask)
celery_app.config_from_object(CeleryConfig)
