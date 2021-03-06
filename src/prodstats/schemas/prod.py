from datetime import date, datetime
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from pydantic import Field, validator

from schemas.bases import CustomBaseModel, CustomBaseSetModel

__all__ = ["ProductionRecord", "ProductionWell", "ProductionWellSet"]


class ProdBase(CustomBaseModel):
    class Config:
        allow_population_by_field_name = True


class ProdSetBase(ProdBase, CustomBaseSetModel):
    def df(
        self,
        create_index: bool = True,
        index_columns: Union[str, List[str]] = ["api10", "prod_date"],
    ) -> pd.DataFrame:
        return super().df(create_index=create_index, index_columns=index_columns)


class ProductionRecord(ProdBase):
    """ Schema for production calculation input """

    prod_date: date = Field(..., alias="first_date")
    days_in_month: Optional[int] = Field(..., alias="last_day")
    oil: Optional[int] = Field(..., alias="liquid")
    oil_uom: Optional[str] = Field(None, alias="liquid_uom")
    gas: Optional[int]
    gas_uom: Optional[str]
    water: Optional[int]
    water_uom: Optional[str]
    gor: Optional[int]
    gor_uom: Optional[str]
    water_cut: Optional[float]


class ProductionWell(ProdBase):
    api10: str = Field(..., min_length=10, max_length=10)
    api14: str = Field(..., min_length=14, max_length=14)
    entity: str
    entity12: str = Field(..., min_length=12, max_length=12)
    status: str
    provider: str
    provider_last_update_at: datetime = Field(..., alias="last_update_at")
    perf_upper: Optional[int] = Field(..., alias="perf_upper_min")
    perf_lower: Optional[int] = Field(..., alias="perf_lower_max")
    perfll: Optional[int]
    products: Optional[str]
    production: List[ProductionRecord]

    @validator("provider_last_update_at")
    def localize(cls, v):
        return super().localize(v)

    def records(self) -> List[Dict[str, Any]]:
        """ Return monthly production records with all other properties of the model
        as additional attributes on each record """
        # TODO: add params to toggle which features are included/excluded
        d = self.dict()
        prod = d.pop("production")
        return [{**d, **row} for row in prod]

    # def df(self, create_index: bool = True) -> pd.DataFrame:
    #     df = pd.DataFrame(self.records())
    #     if create_index:
    #         df = df.set_index(["api10", "prod_date"])

    #     return df


class ProductionWellSet(ProdSetBase):
    wells: List[ProductionWell]

    def records(self) -> List[Dict[str, Any]]:
        """ Return the production records for each well as a single list """
        # TODO: add params to toggle which features are included/excluded

        return super().records(using="records")
        # records: List[Dict[str, Any]] = []
        # for well in self.wells:
        #     records += well.records()
        # return records

    # def df(self, create_index: bool = True) -> pd.DataFrame:
    #     df = pd.DataFrame(self.records())
    #     if create_index:
    #         df = df.set_index(["api10", "prod_date"])

    #     return df


if __name__ == "__main__":
    prodwell = {
        "api10": "1234567890",
        "api14": "12345678900000",
        "entity": "12345678900000",
        "entity12": "123456789000",
        "provider": "12345678900000",
        "status": "12345678900000",
        "last_update_at": "2020-04-01T01:59:21.163000",
        "perf_upper_min": 0,
        "perf_lower_max": 0,
        "production": [],
    }

    ProductionWell(**prodwell).dict()
    datetime(2020, 4, 1, 1, 59, 21, 163000)
