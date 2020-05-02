from typing import Dict, Optional

from schemas.bases import CustomBaseModel, ORMBase

__all__ = [
    "ORMTask",
    "TaskIn",
    "TaskOut",
]


class TaskBase(CustomBaseModel):
    class Config:
        allow_population_by_field_name = True
        extra = "allow"

    name: Optional[str] = None
    qualname: Optional[str] = None
    parameters: Dict = {}


class ORMTask(ORMBase, TaskBase):
    pass


class TaskIn(TaskBase):
    pass


class TaskOut(TaskBase):
    pass


# class TaskCreateIn(TaskBase):
#     pass


# class TaskCreateOut(TaskBase):
#     pass


# class TaskUpdateIn(TaskBase):
#     pass


# class TaskUpdateOut(TaskBase):
#     pass
