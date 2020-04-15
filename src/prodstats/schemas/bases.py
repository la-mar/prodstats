from datetime import datetime
from typing import Any, Dict, List, Union

import orjson
import pandas as pd
import pytz
from pydantic import BaseModel, parse_obj_as
from pydantic.fields import ModelField

from ext.orjson import orjson_dumps
from util.deco import classproperty


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


class CustomBaseSetModel(CustomBaseModel):
    @classproperty
    def __first_field__(cls) -> ModelField:
        return list(cls.__fields__.values())[0]

    @classproperty
    def __dataframe_columns__(cls) -> List[str]:
        return list(cls.__first_field__.type_.__fields__.keys())

    def records(self, using: str = "dict") -> List[Dict[str, Any]]:
        """ using options: [dict, records] """
        records: List[Dict] = []
        field_values = getattr(self, self.__first_field__.name)
        if field_values:
            if using == "dict":
                for x in field_values:
                    records.append(x.dict())
            elif using == "records":
                for x in field_values:
                    records += x.records()
        return records

    def df(
        self, create_index: bool = True, index_columns: Union[str, List[str]] = None
    ) -> pd.DataFrame:
        df = pd.DataFrame(data=self.records(), columns=self.__dataframe_columns__)
        if create_index and index_columns:
            df = df.set_index(index_columns)
        return df


class ORMBase(CustomBaseModel):
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True
