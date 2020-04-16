from __future__ import annotations

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

    @classproperty
    def __sub_models__(cls) -> Dict[str, CustomBaseModel]:
        """ Returns a list of column names generated from the fields in the pydanic model.
            The column names of child models are recursively expanded into the returned
            list of column names.

            Examples
            --------

            >>> class ModelA(BaseModel):
                    field1: int
                    field2: ModelB
                    field3: ModelC

            >>> class ModelB(BaseModel):
                    field4: ModelD

            >>> class ModelC(BaseModel):
                    field5: datetime

            >>> class ModelD(BaseModel):
                    field6: int

            >>> ModelA.__sub_models__
            {"field2": ModelB, "field3": ModelC}

            Notice only ModelA's direct child models are returned.

            >>> ModelB.__sub_models__
            {"field6":"ModelD"}

            >>> ModelD.__sub_models__
            {}

            An empty collection is returned when no child models are found.


        """
        return {
            field_name: field.type_
            for field_name, field in cls.__fields__.items()
            if issubclass(field.type_, BaseModel)
        }

    @classproperty
    def __dataframe_columns__(cls) -> List[str]:
        """ Returns a list of column names generated from the fields in the pydanic model.
            The column names of child models are recursively expanded into the returned
            list of column names.

            This mechanism is useful for ensuring DataFrames yielded from a model will have
            a consistent shape whether the DataFrame is empty or not.

            Examples
            --------

            >>> class ModelA(BaseModel):
                    field1: int
                    field2: ModelB
                    field3: str

            >>> class ModelB(BaseModel):
                    field4: int

            >>> ModelA.__dataframe_columns__
            ["field1", "field3", "field4"]

            Notice field2 is substituted with its own field names in the returned list.

            >>> ModelB.__dataframe_columns__
            ["field4"]

        """
        removals: List[str] = []
        replacements: List[str] = []
        for field_name, field in cls.__sub_models__.items():
            removals.append(field_name)
            replacements += field.__dataframe_columns__

        return [
            x for x in list(cls.__fields__.keys()) if x not in removals
        ] + replacements

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
        return cls.__first_field__.type_.__dataframe_columns__

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
