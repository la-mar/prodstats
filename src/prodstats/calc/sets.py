from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

import pandas as pd

import db.models


class SetItem:

    __slots__ = ("name", "model", "df")

    def __init__(self, name: str, model: db.models.Model, df: pd.DataFrame):
        self.name: str = name
        self.model: db.models.Model = model
        self.df: pd.DataFrame = df

    def __repr__(self):
        return f"SetItem: name={self.name} model={self.model} df={self.df.shape[0]}"

    def __iter__(self):
        for x in self.__slots__:
            yield getattr(self, x)


class DataSet:
    models: Dict[str, db.models.Model] = None  # type: ignore

    __data_slots__: Tuple = ("frame1", "frame2")

    def __init__(self, models: Dict[str, db.models.Model]):
        self.models: Dict[str, db.models.Model] = models

    def __repr__(self):
        s = " ".join([f"{k}={v}" for k, v in self.describe().items()])
        return s

    def __iter__(self):
        for x in self.__data_slots__:
            yield getattr(self, x, None)

    @property
    def names(self):
        return self.__data_slots__

    def describe(self) -> Dict[str, int]:
        result: Dict[str, Any] = {}
        for x in self.__data_slots__:

            value = getattr(self, x, pd.DataFrame())
            if value is not None:
                result[x] = value.shape[0]
            else:
                result[x] = 0

        return result

    def items(self):
        for x in self.__data_slots__:
            yield SetItem(
                name=x,
                model=self.models.get(x, None),
                df=getattr(self, x, pd.DataFrame()),
            )


class ProdSet(DataSet):

    __data_slots__: Tuple = ("header", "monthly", "stats")

    def __init__(
        self,
        header: pd.DataFrame = None,
        monthly: pd.DataFrame = None,
        stats: pd.DataFrame = None,
    ):

        super().__init__(
            models={
                "header": db.models.ProdHeader,
                "monthly": db.models.ProdMonthly,
                "stats": db.models.ProdStat,
            }
        )
        self.header = header
        self.monthly = monthly
        self.stats = stats


class WellSet(DataSet):
    __data_slots__: Tuple = ("wells", "depths", "fracs", "ips", "stats", "links")

    def __init__(
        self,
        wells: pd.DataFrame = None,
        depths: pd.DataFrame = None,
        fracs: pd.DataFrame = None,
        ips: pd.DataFrame = None,
        stats: pd.DataFrame = None,
        links: pd.DataFrame = None,
    ):

        super().__init__(
            models={
                "wells": db.models.WellHeader,
                "depths": db.models.WellDepth,
                "fracs": db.models.FracParameters,
                "ips": db.models.IPTest,
                "stats": db.models.WellStat,
                "links": db.models.WellLink,
            }
        )
        self.wells = wells
        self.depths = depths
        self.fracs = fracs
        self.ips = ips
        self.stats = stats
        self.links = links


class WellGeometrySet(DataSet):
    __data_slots__: Tuple = ("locations", "surveys", "points")

    def __init__(
        self,
        locations: pd.DataFrame = None,
        surveys: pd.DataFrame = None,
        points: pd.DataFrame = None,
        geometry_columns: Dict[str, List] = None,
    ):

        super().__init__(
            models={
                "locations": db.models.WellLocation,
                "surveys": db.models.Survey,
                "points": db.models.SurveyPoint,
            }
        )
        self.locations = locations
        self.surveys = surveys
        self.points = points

        # merge defaults and passed values
        self.geometry_columns = {
            **{
                "locations": ["geom"],
                "surveys": ["wellbore", "lateral_only", "stick", "bent_stick"],
                "points": ["geom"],
            },
            **(geometry_columns or {}),
        }

    def shapes_as_wkb(
        self, geometry_columns: Dict[str, List] = None
    ) -> WellGeometrySet:
        geometry_columns = {**self.geometry_columns, **(geometry_columns or {})}
        for name, model, df in self.items():
            geom_cols: List[str] = geometry_columns[name]
            setattr(self, name, df.shapes.shapes_to_wkb(geom_cols))
        return self

    def wkb_as_shapes(
        self, geometry_columns: Dict[str, List] = None
    ) -> WellGeometrySet:
        geometry_columns = {**self.geometry_columns, **(geometry_columns or {})}
        for name, model, df in self.items():
            geom_cols: List[str] = geometry_columns[name]
            setattr(self, name, df.shapes.wkb_to_shapes(geom_cols))
        return self

    def to_geojson(
        self,
        output_dir: Union[str, Path],
        suffix: str = None,
        subset: List[str] = None,
    ):
        """ dump the underlying dataframes to geojson

        Arguments:
            output_dir {Union[str, Path]} -- directory to save the generated geojson files

        Keyword Arguments:
            suffix {str} -- add a suffix to the filename of each output file (default: {None})
            subset {List[str]} -- export only a subset of the underlying
                                  dataframes (default: {None})

        Raises:
            ValueError: [description]
        """

        subset = list(subset or self.__data_slots__)

        for x in subset:
            if x not in self.names:
                raise ValueError(
                    f"{x} is not a valid subset. Must be one of {self.names}"
                )

        if not isinstance(output_dir, Path):
            output_dir = Path(output_dir)

        if suffix:
            suffix = f"_{suffix}"
        else:
            suffix = ""

        if not output_dir.is_dir():
            raise ValueError("output_dir must be a directory")

        if "locations" in subset:
            if self.locations is not None and not self.locations.empty:
                self.locations.shapes.to_geojson(
                    output_dir / f"well_locations{suffix}.geojson", geometry="geom"
                )

        if "surveys" in subset:
            if self.surveys is not None and not self.surveys.empty:

                self.surveys.loc[:, ["wellbore"]].shapes.to_geojson(
                    output_dir / f"wellbores{suffix}.geojson", geometry="wellbore"
                )
                self.surveys.loc[:, ["lateral_only"]].shapes.to_geojson(
                    output_dir / f"laterals{suffix}.geojson", geometry="lateral_only"
                )
                self.surveys.loc[:, ["stick"]].shapes.to_geojson(
                    output_dir / f"sticks{suffix}.geojson", geometry="stick"
                )
                self.surveys.loc[:, ["bent_stick"]].shapes.to_geojson(
                    output_dir / f"bent_sticks{suffix}.geojson", geometry="bent_stick"
                )

        if "points" in subset:
            if self.points is not None and not self.points.empty:
                self.points.shapes.to_geojson(
                    output_dir / f"survey_points{suffix}.geojson", geometry="geom"
                )
