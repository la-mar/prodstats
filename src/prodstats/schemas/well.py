import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from pydantic import Field, root_validator, validator
from shapely.geometry import LineString, Point, asShape

from schemas.bases import CustomBaseModel

__all__ = [
    "WellDates",
    "FracParameters",
    "FracParameterSet",
    "WellElevations",
    "WellDepths",
    "WellDepthSet",
    "IPTest",
    "IPTests",
    "IPTestSet",
    "WellRecord",
    "WellRecordSet",
    "WellSurvey",
    "WellSurveySet",
    "WellSurveyPoint",
    "WellSurveyPointSet",
    "WellLocation",
    "WellLocationSet",
]

logger = logging.getLogger(__name__)


class WellBase(CustomBaseModel):
    class Config:
        allow_population_by_field_name = True


class WellDates(WellBase):
    permit_date: Optional[date] = Field(None, alias="permit")
    spud_date: Optional[date] = Field(None, alias="spud")
    comp_date: Optional[date] = Field(None, alias="comp")
    rig_release_date: Optional[date] = Field(None, alias="rig_release")
    last_activity_date: Optional[date] = Field(None, alias="ihs_last_update")


class FracParameters(WellBase):
    api14: str
    fluid: Optional[int] = Field(None, alias="fluid_total")
    fluid_uom: Optional[str] = Field(None, alias="fluid_total_uom")
    proppant: Optional[int] = Field(None, alias="proppant_total")
    proppant_uom: Optional[str] = Field(None, alias="proppant_total_uom")
    provider: str
    provider_last_update_at: datetime = Field(..., alias="last_update_at")

    @root_validator(pre=True)
    def preprocess(cls, values):

        if "frac" in values.keys():
            frac = values.get("frac", {})
        else:
            frac = values
        return {**values, **frac}

    @validator("provider_last_update_at")
    def localize(cls, v: Any) -> Any:
        return super().localize(v)


class FracParameterSet(WellBase):
    wells: List[FracParameters]

    def records(self) -> List[Dict[str, Any]]:
        return [wd.dict() for wd in self.wells]

    def df(self, create_index: bool = True) -> pd.DataFrame:
        df = pd.DataFrame(self.records())
        if create_index and "api14" in df.columns:
            df = df.set_index("api14")
        return df


class WellElevations(WellBase):
    ground_elev: Optional[int] = Field(None, alias="ground")
    kb_elev: Optional[int] = Field(None, alias="kb")


class WellDepths(WellBase):
    api14: str
    tvd: Optional[int] = None
    md: Optional[int] = None
    perf_upper: Optional[int] = None
    perf_lower: Optional[int] = None
    plugback_depth: Optional[int] = None


class WellDepthSet(WellBase):
    wells: List[WellDepths]

    def records(self) -> List[Dict[str, Any]]:
        return [wd.dict() for wd in self.wells]

    def df(self, create_index: bool = True) -> pd.DataFrame:
        df = pd.DataFrame(self.records())
        if create_index and "api14" in df.columns:
            df = df.set_index("api14")
        return df


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
    ip: Optional[List[IPTest]] = None

    def records(self) -> List[Dict]:
        data = self.dict()
        return [{"api14": self.api14, **d} for d in data["ip"]]

    @root_validator(pre=True)
    def filter_failing_records(cls, values):
        api14 = values.get("api14")
        ips = values.get("ip")
        culled = []
        if ips:
            for ip in ips:
                try:
                    if ip:
                        IPTest(**ip)
                        culled.append(ip)
                except Exception:
                    logger.debug(f"{api14}: filtered row from ip tests")
        return {"api14": api14, "ip": culled}


class IPTestSet(WellBase):
    wells: List[IPTests]

    def records(self) -> List[Dict[str, Any]]:
        """ Return the well records as a single list """
        records: List[Dict[str, Any]] = []
        for well in self.wells:
            records += well.records()
        return records

    def df(self, create_index: bool = True) -> pd.DataFrame:
        df = pd.DataFrame(self.records())
        if create_index and "api14" in df.columns:
            df = df.set_index("api14")
        return df


class WellRecord(WellBase):
    api14: str
    api10: str
    hole_direction: str
    status: str
    county: Optional[str]
    county_code: Optional[int]
    state: Optional[str]
    state_code: Optional[int]
    basin: Optional[str]
    sub_basin: Optional[str]
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

    dates: Optional[WellDates] = WellDates()
    elev: WellElevations = Field({}, alias="elevations")

    def record(self, flatten_keys: list = ["dates", "elev"]) -> Dict[str, Any]:
        data = self.dict()
        for key in flatten_keys:
            data.update(data.pop(key, {}))
        return data

    @validator("provider_last_update_at")
    def localize(cls, v: Any) -> Any:
        return super().localize(v)


class WellRecordSet(WellBase):
    wells: Optional[List[WellRecord]] = None

    def records(self) -> List[Dict[str, Any]]:
        """ Return the well records as a single list """
        records: List[Dict] = []
        if self.wells:
            records = [well.record() for well in self.wells]
        return records

    def df(self, create_index: bool = True) -> pd.DataFrame:
        df = pd.DataFrame(self.records())
        if create_index and "api14" in df.columns:
            df = df.set_index("api14")

        return df


class WellGeometryBase(WellBase):
    class Config:
        arbitrary_types_allowed = True


class WellSurvey(WellGeometryBase):

    api14: str
    survey_type: str
    survey_method: str
    survey_date: Optional[date] = Field(None, alias="survey_end_date")
    survey_top: Optional[int] = None
    survey_top_uom: Optional[str] = None
    survey_base: Optional[int] = None
    survey_base_uom: Optional[str] = None
    wellbore: Optional[LineString] = Field(None, alias="line")

    @root_validator(pre=True)
    def preprocess(cls, values):
        api14 = values.get("api14")
        survey = values.get("survey", {})
        if survey["line"]:
            survey["line"] = asShape(survey["line"])
        return {"api14": api14, **survey}


class WellSurveySet(WellGeometryBase):
    wells: Optional[List[WellSurvey]] = None

    def records(self) -> List[Dict[str, Any]]:
        records: List[Dict] = []
        if self.wells:
            for x in self.wells:
                records.append(x.dict())
        return records

    def df(self, create_index: bool = True) -> pd.DataFrame:
        df = pd.DataFrame(self.records())
        if create_index and "api14" in df.columns:
            df = df.set_index("api14")
        return df

    @root_validator(pre=True)
    def filter_failing_records(cls, values) -> Dict[str, Any]:
        wells = values.get("wells")
        culled = []
        for well in wells:
            api14 = well.get("api14")
            try:
                if well:
                    WellSurvey(**well)
                    culled.append(well)
            except Exception:
                logger.debug(f"{api14}: filtered row from surveys")
        return {"wells": culled}


class WellSurveyPoint(WellGeometryBase):

    api14: str
    md: int
    tvd: int
    dip: float
    geom: Point = Field(None, alias="point")

    @root_validator(pre=True)
    def preprocess(cls, values) -> Dict[str, Any]:
        point = values.get("point")
        geom = values.get("geom")
        if point:
            values["point"] = asShape(point)
        elif geom:
            values["geom"] = asShape(geom)
        return values


class WellSurveyPoints(WellGeometryBase):

    points: Optional[List[WellSurveyPoint]] = None

    @root_validator(pre=True)
    def preprocess(cls, values) -> Dict[str, Any]:
        api14 = values.get("api14")
        survey = values.get("survey", {})
        transformed: List[Dict] = []
        if survey.get("points"):
            for idx, pt in enumerate(survey["points"]):
                pt["geom"] = asShape(pt["geom"])
                pt["api14"] = api14
                try:
                    WellSurveyPoint(**pt)
                    transformed.append(pt)
                except Exception as e:
                    logger.debug(f"{api14}: filtered survey point #{idx} -- {e}")

        return {"points": transformed}

    def records(self) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        if self.points:
            records = [x.dict() for x in self.points]
        return records

    def df(self, create_index: bool = True) -> pd.DataFrame:
        df = pd.DataFrame(self.records())
        if create_index and "api14" in df.columns:
            df = df.set_index("api14")
        return df


class WellSurveyPointSet(WellGeometryBase):
    wells: Optional[List[WellSurveyPoints]] = None

    def records(self) -> List[Dict[str, Any]]:
        records: List[Dict] = []
        if self.wells:
            for x in self.wells:
                records += x.records()
        return records

    def df(
        self, create_index: bool = True, sort_by: str = "md", ascending: bool = True,
    ) -> pd.DataFrame:
        df = pd.DataFrame(self.records())
        if create_index and "api14" in df.columns:
            df = df.set_index(["api14", "md"])
        return df


class WellLocation(WellGeometryBase):
    api14: str
    name: str
    block: Optional[str]
    section: Optional[str]
    abstract: Optional[str]
    survey: Optional[str]
    metes_bounds: Optional[str]
    geom: Optional[Point] = Field(None, alias="point")

    @validator("geom", pre=True)
    def geojson_to_shape(cls, v):
        return asShape(v)


class WellLocationSet(WellGeometryBase):
    wells: Optional[List[WellLocation]] = None

    @validator("wells", pre=True)
    def preprocess(cls, v: List[Dict]) -> List[Dict[str, Any]]:
        locations: List[Dict[str, Any]] = []
        if v:
            for well in v:
                data = []
                api14 = well.get("api14")
                for key in ["shl", "bhl", "pbhl"]:
                    loc = well.get(key, None)
                    if api14 and loc:
                        data.append({"api14": api14, "name": key, **loc})
                locations += data

        return locations

    def records(self) -> List[Dict[str, Any]]:
        records: List[Dict] = []
        if self.wells:
            for x in self.wells:
                records.append(x.dict())
        return records

    def df(self, create_index: bool = True) -> pd.DataFrame:
        df = pd.DataFrame(self.records())
        if create_index and "api14" in df.columns:
            df = df.set_index(["api14", "name"])
        return df


if __name__ == "__main__":
    from util.jsontools import load_json

    ihs_wells = load_json(f"tests/fixtures/ihs_wells.json")

    obj = WellRecord(**ihs_wells[0])
    obj.record()

    objset = WellRecordSet(wells=ihs_wells)
    objset.df().T

    depths = WellDepths(**ihs_wells[0])
    depths.dict()

    depthset = WellDepthSet(wells=ihs_wells)
    depthset.records()

    ip = IPTests(**ihs_wells[0])
    ip.ip

    ipset = IPTestSet(wells=ihs_wells)
    ipset.records()

    frac = FracParameters(**ihs_wells[0])
    frac.dict()

    fracs = FracParameterSet(wells=ihs_wells)
    fracs.records()
