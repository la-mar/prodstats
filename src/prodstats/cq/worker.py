""" Initialize Celery worker """

from celery import Celery

from config import CeleryConfig

celery_app: Celery = Celery("worker")
celery_app.config_from_object(CeleryConfig)
