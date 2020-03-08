from datetime import date

from schemas.bases import CustomBaseModel

__all__ = ["ProdRecord", "ProdSet"]


class ProdBase(CustomBaseModel):
    class Config:
        allow_population_by_field_name = True
        # extra = "allow"


class ProdRecord(ProdBase):
    year: int
    month: int
    last_day: int
    first_date: date
    last_date: date
    liquid: int
    liquid_uom: str
    oil: int
    oil_uom: str
    gas: int
    gas_uom: str
    casinghead_gas: int
    casinghead_gas_uom: str
    water: int
    water_uom: str
    gor: int
    gor_uom: str
    water_cut: float
    well_count: int
    oil_well_count: int


class ProdSet(ProdBase):
    pass
