from datetime import datetime
from typing import Any

import orjson
import pytz
from pydantic import BaseModel, parse_obj_as

from ext.orjson import orjson_dumps


class CustomBaseModel(BaseModel):
    class Config:
        # use orjson serializers for all models
        json_loads = orjson.loads
        json_dumps = orjson_dumps

    def localize(v: Any) -> Any:
        try:
            v = parse_obj_as(datetime, v)
            if not v.tzinfo:
                return pytz.utc.localize(v)
            else:
                return v
        except Exception as e:
            raise e


class ORMBase(CustomBaseModel):
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True
