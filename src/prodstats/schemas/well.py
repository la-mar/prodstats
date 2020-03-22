from datetime import date, datetime
from typing import Dict, List, Optional

from pydantic import Field

from schemas.bases import CustomBaseModel

__all__ = [
    "WellDates",
    "FracParams",
    "WellElevations",
    "WellDepths",
    "IPTest",
    "IPTests",
    "WellRecord",
]


class WellBase(CustomBaseModel):
    class Config:
        allow_population_by_field_name = True


class WellDates(WellBase):
    permit_date: Optional[date] = Field(None, alias="permit")
    spud_date: Optional[date] = Field(None, alias="spud")
    comp_date: Optional[date] = Field(None, alias="comp")
    rig_release_date: Optional[date] = Field(None, alias="rig_release")
    last_activity_date: Optional[date] = Field(None, alias="ihs_last_update")


# class WellStatus(WellBase):
#     status: Optional[str] = Field(None, alias="current")
#     status_code: Optional[str] = Field(None, alias="current_code")
#     activity_status: Optional[str] = Field(None, alias="activity")
#     activity_status_code: Optional[str] = Field(None, alias="activity_code")


class FracParams(WellBase):
    fluid: Optional[int] = Field(None, alias="fluid_total")
    proppant: Optional[int] = Field(None, alias="proppant_total")


class WellElevations(WellBase):
    ground_elev: Optional[int] = Field(None, alias="ground")
    kb_elev: Optional[int] = Field(None, alias="kb")


class WellDepths(WellBase):
    tvd: Optional[int] = None
    md: Optional[int] = None
    perf_upper: Optional[int] = None
    perf_lower: Optional[int] = None
    plugback_depth: Optional[int] = None


class IPTest(WellBase):
    test_number: int
    test_date: date
    type_code: str
    test_method: Optional[str] = None
    completion: Optional[int] = None
    oil: Optional[int] = None
    oil_uom: Optional[str] = None
    gas: Optional[int] = None
    gas_uom: Optional[str] = None
    water: Optional[int] = None
    water_uom: Optional[str] = None
    choke: Optional[str] = None
    depth_top: Optional[int] = None
    depth_top_uom: Optional[str] = None
    depth_base: Optional[int] = None
    depth_base_uom: Optional[str] = None
    sulfer: Optional[bool] = None
    oil_gravity: Optional[int] = None
    oil_gravity_uom: Optional[str] = None
    gor: Optional[int] = None
    gor_uom: Optional[str] = None
    perf_upper: Optional[int] = None
    perf_upper_uom: Optional[str] = None
    perf_lower: Optional[int] = None
    perf_lower_uom: Optional[str] = None
    perfll: Optional[int] = None
    perfll_uom: Optional[str] = None


class IPTests(WellBase):
    api14: str
    ip: List[IPTest]

    def records(self) -> List[Dict]:
        data = self.dict()
        return [{"api14": self.api14, **d} for d in data["ip"]]


class WellRecord(WellBase):
    api14: str
    api10: str
    hole_direction: str
    status: str
    is_producing: Optional[bool] = None
    operator: str
    operator_alias: Optional[str] = None
    hist_operator: str = Field(None, alias="operator_original")
    hist_operator_alias: str = Field(None, alias="operator_original_alias")

    permit_number: Optional[str] = None
    permit_status: Optional[str] = None

    perfll: Optional[int] = None

    provider: str
    provider_last_update_at: datetime = Field(..., alias="last_update_at")

    dates: WellDates
    elev: WellElevations = Field({}, alias="elevations")
    frac: FracParams
    ip: List[IPTest]

    def record(
        self, flatten_keys: list = ["dates", "elev", "frac"], exclude: set = {"ip"}
    ) -> Dict:
        data = self.dict(exclude=exclude)
        for key in flatten_keys:
            data.update(data.pop(key, {}))
        return data


if __name__ == "__main__":
    from util.jsontools import load_json

    ihs_wells = load_json(f"tests/fixtures/ihs_wells.json")

    obj = WellRecord(**ihs_wells[0])
    obj.record()

    depths = WellDepths(**ihs_wells[0])
    depths.dict()

    ip = IPTests(**ihs_wells[0])
    ip.records()
