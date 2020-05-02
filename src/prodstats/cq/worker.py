""" Initialize Celery worker """

import inspect
from typing import Any, Dict, List

from celery import Celery as Celery_
from celery import Task

from config import CeleryConfig
from cq.task import CustomBaseTask


class Celery(Celery_):
    @property
    def custom_tasks(self) -> Dict[str, Task]:
        """ Returns all tasks registered in the given app, excluding the celery builtins """
        return {k: v for k, v in self.tasks.items() if not k.startswith("celery.")}

    def describe_tasks(self) -> List[Dict[str, Any]]:

        tasks: List[Dict] = []
        for task_name, task in self.custom_tasks.items():
            task_sig = inspect.signature(task)
            params = {k: str(v.annotation) for k, v in task_sig.parameters.items()}
            short_name = task_name.split(".")[-1]
            task = {"name": short_name, "qualname": task_name, "parameters": params}
            tasks.append(task)

        return tasks


celery_app: Celery = Celery("worker", task_cls=CustomBaseTask)
celery_app.config_from_object(CeleryConfig)
