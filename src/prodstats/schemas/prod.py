from datetime import date
from typing import List, Optional

import pandas as pd
from pydantic import Field

from schemas.bases import CustomBaseModel

__all__ = ["ProdCalcRecord", "ProdCalcSet"]


class ProdBase(CustomBaseModel):
    class Config:
        allow_population_by_field_name = True
        # extra = "allow"


class ProdCalcRecord(ProdBase):
    """ Schema for production calculation input """

    api10: str = Field(..., min_length=10, max_length=10)
    api14: str = Field(..., min_length=14, max_length=14)
    entity: str
    entity12: str = Field(..., min_length=12, max_length=12)
    # year: int
    # month: int
    prod_date: date = Field(..., alias="first_date")
    days_in_month: Optional[int] = Field(..., alias="last_day")
    # last_date: date
    # liquid: Optional[int]
    # liquid_uom: str
    oil: Optional[int]
    oil_uom: Optional[str]
    gas: Optional[int]
    gas_uom: Optional[str]
    # casinghead_gas: Optional[int]
    # casinghead_gas_uom: Optional[str]
    water: Optional[int]
    water_uom: Optional[str]
    gor: Optional[int]
    gor_uom: Optional[str]
    water_cut: Optional[float]
    perf_upper: Optional[int] = Field(..., alias="perf_upper_max")
    perf_lower: Optional[int] = Field(..., alias="perf_lower_min")
    perfll: Optional[int]
    products: Optional[str]
    wells: Optional[int]


class ProdCalcSet(ProdBase):
    production: List[ProdCalcRecord]

    def dict(self) -> dict:
        return super().dict()["production"]

    def df(self) -> pd.DataFrame:
        return pd.DataFrame(self.dict()).set_index(["api10", "prod_date"])
