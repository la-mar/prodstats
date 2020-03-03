from datetime import datetime

import orjson
from pydantic import BaseModel

from ext.orjson import orjson_dumps


class CustomBaseModel(BaseModel):
    class Config:
        # use orjson serializers for all models
        json_loads = orjson.loads
        json_dumps = orjson_dumps


class ORMBase(CustomBaseModel):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True
