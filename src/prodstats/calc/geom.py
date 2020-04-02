import logging
from pathlib import Path
from typing import List, Union

import geopandas as gp
import numpy as np
import pandas as pd
from shapely.geometry import LineString, Point

import const
import schemas as sch
import util.geo as geo
from calc.sets import WellGeometrySet
from collector import IHSClient, IHSPath
from util.pd import column_as_set, validate_required_columns
from util.types import PandasObject

logger = logging.getLogger(__name__)


@pd.api.extensions.register_dataframe_accessor("shapes")
class Shapes:
    def __init__(self, obj: PandasObject):
        self._obj: PandasObject = obj

    @classmethod
    async def from_ihs(
        cls,
        path: IHSPath,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        create_index: bool = True,
        **kwargs,
    ) -> WellGeometrySet:

        data = await IHSClient.get_wells(
            api14s=api14s, api10s=api10s, path=path, **kwargs
        )

        locations = sch.WellLocationSet(wells=data).df()
        surveys = sch.WellSurveySet(wells=data).df()
        points = sch.WellSurveyPointSet(wells=data).df()

        return WellGeometrySet(locations=locations, surveys=surveys, points=points)

    def mark_lateral_points(self, dip_threshold: int = None) -> pd.DataFrame:
        dip_threshold = dip_threshold or const.LATERAL_DIP_THRESHOLD

        points = self._obj
        validate_required_columns(["dip"], points.columns)

        points.loc[points.dip > dip_threshold, "is_in_lateral"] = True
        return points

    def index_survey_points(self, dip_threshold: int = None) -> pd.DataFrame:
        points = self._obj
        validate_required_columns(["dip"], points.columns)
        validate_required_columns(["api14", "md"], points.index.names)

        calc_column_names = [
            "is_in_lateral",
            "is_heel_point",
            "is_mid_point",
            "is_toe_point",
        ]
        # points.loc[:, calc_column_names] = np.nan
        # add placeholders (with defaults) for columns to be calculated
        points = pd.concat([points, pd.DataFrame(columns=calc_column_names)])
        points.loc[:, calc_column_names] = False

        points["sequence"] = points.groupby(level=0).cumcount() + 1
        points = points.shapes.mark_lateral_points()

        heel_point_index = (
            points.loc[points.is_in_lateral]
            .reset_index(level=1)
            .groupby(level=0)
            .first()
            .set_index("md", append=True)
            .index
        )

        mid_point_sequence_index = (
            points.loc[points.is_in_lateral]
            .groupby(level=0)
            .sequence.median()
            .apply(np.floor)
            .to_frame()
            .astype(int)
            .set_index("sequence", append=True)
            .index
        )

        mid_point_index = (
            points.reset_index(level=1)
            .set_index("sequence", append=True)
            .loc[mid_point_sequence_index]
            .reset_index(level=1)
            .set_index("md", append=True)
            .index
        )

        toe_point_index = (
            points.loc[points.is_in_lateral]
            .reset_index(level=1)
            .groupby(level=0)
            .last()
            .set_index("md", append=True)
            .index
        )

        points.loc[heel_point_index, "is_heel_point"] = True
        points.loc[mid_point_index, "is_mid_point"] = True
        points.loc[toe_point_index, "is_toe_point"] = True

        heel_start_seq_by_group = points.loc[heel_point_index, "sequence"]

        # ensure all points after the heel point are marked as in the lateral.
        # The dip filter doesnt always cant all of the points
        for api14 in points.groupby(level=0).groups:
            heel_start_seq = heel_start_seq_by_group.loc[api14].iloc[0]
            group = points.xs(api14, level=0, drop_level=False)
            group_in_lateral_index = group.loc[group.sequence >= heel_start_seq].index
            points.loc[group_in_lateral_index, "is_in_lateral"] = True

        return points

    def as_line(self, geometry: str = "geom", label: str = "line") -> pd.Series:
        points = self._obj
        validate_required_columns([geometry], points.columns)

        # .to_numpy() is implemented by both pandas and geopandas geoarrays
        # whereas .values is not.
        return (
            points[geometry]
            .groupby(level=0)
            .apply(lambda x: LineString(x.to_numpy().tolist()))
            .rename(label)
        )

    def as_stick(self, geometry: str = "geom", label: str = "stick") -> pd.Series:
        points = self._obj
        validate_required_columns([geometry], points.columns)
        validate_required_columns(["md"], points.index.names)
        return (
            points.groupby(level=0)
            .agg(["first", "last"])
            .stack()
            .shapes.as_line(geometry=geometry, label=label)
        )

    def as_bent_stick(
        self, geometry: str = "geom", label: str = "bent_stick"
    ) -> pd.Series:
        points = self._obj
        validate_required_columns([geometry], points.columns)
        validate_required_columns(["md"], points.index.names)
        bent_stick_points = (
            pd.concat(
                [
                    points.reset_index(level=1)  # first and last points
                    .groupby(level=0)
                    .agg(["first", "last"])
                    .stack()
                    .reset_index(level=1, drop=True)
                    .set_index("md", append=True),
                    points.loc[points.is_kop],  # kop points
                ]
            )
            .sort_index()
            .loc[:, [geometry]]
        )

        return bent_stick_points.shapes.as_line(geometry=geometry, label=label)

    def as_gdf(
        self, geometry: str = "geom", crs: int = 4326, **kwargs
    ) -> gp.GeoDataFrame:
        return gp.GeoDataFrame(self._obj, geometry=geometry, crs=crs, **kwargs)

    def to_geojson(
        self, path: Union[str, Path], geometry: str, crs: int = 4326
    ) -> Path:
        return geo.to_geojson(self._obj.shapes.as_gdf(geometry=geometry, crs=crs), path)

    def relative_trajectory_angle(
        self,
        geometry: str = "geom",
        preceeding: int = 1,
        following: int = 1,
        max_soft_angle: float = 150.0,
        max_hard_angle: float = 130.0,
    ) -> gp.GeoDataFrame:
        """ calculate the central angle of a point relative to an arbitrary
            number points before and after it, element wise. The triangle formed
            by the three points is used to calculate the measurement of the angle
            touching the current point (theta).


        Keyword Arguments:
            preceeding {int} -- number of points preceeding the current point to be
                                considered when calculating the central angle. (default: {1})
            following {int} -- number of points following the current point to be
                                considered when calculating the central angle. (default: {1})

        Returns:
            gp.GeoDataFrame
        """

        points = self.as_gdf()

        validate_required_columns([geometry], points.columns)
        rel_prev = points.groupby(level=0).shift(preceeding).shapes.as_gdf()
        rel_next = points.groupby(level=0).shift(-following).shapes.as_gdf()

        hyp = rel_prev.distance(rel_next).rename("hyp")
        adj = points.distance(rel_next).rename("adj")
        opp = points.distance(rel_prev).rename("opp")

        tri = hyp.to_frame().join(adj).join(opp)  # .mul(100000)

        # law of cosines:
        #   c2 = a2 + b2 − 2ab cos(C) -> acos((adj^2 + opp^2 - hyp^2) / (2 * adj * opp))
        tri[["hyp_sq", "adj_sq", "opp_sq"]] = tri.loc[:, ["hyp", "adj", "opp"]].pow(2)
        tri["theta"] = (
            tri.adj_sq.add(tri.opp_sq)
            .sub(tri.hyp_sq)
            .div(tri.adj.mul(tri.opp).mul(2))
            .apply(np.arccos)
            .apply(np.rad2deg)
        )

        # mark the soft corners (bends)
        soft_corner_mask = (tri.theta > 0) & (tri.theta < max_soft_angle)
        tri["is_soft_corner"] = False
        tri.loc[soft_corner_mask, "is_soft_corner"] = True

        # mark the hard corners (steep bends)
        hard_corner_mask = (tri.theta > 0) & (
            tri.theta < max_hard_angle
        )  # | (tri.theta > 200)
        tri["is_hard_corner"] = False
        tri.loc[hard_corner_mask, "is_hard_corner"] = True

        return tri.loc[:, ["theta", "is_soft_corner", "is_hard_corner"]].dropna(
            how="any"
        )

    def as_3d(self, geometry: str = "geom") -> pd.DataFrame:
        """transform 2d points into 3d points using the MD index as Z """
        xyz = self._obj
        validate_required_columns([geometry], xyz.columns)
        validate_required_columns(["md"], xyz.index.names)

        xyz["x"] = xyz[geometry].apply(lambda x: x.x)
        xyz["y"] = xyz[geometry].apply(lambda x: x.y)

        xyz = (
            xyz.reset_index(level=1)
            .set_index("md", append=True, drop=False)
            .rename(columns={"md": "z"})
        )
        xyz = xyz.loc[:, ["x", "y", "z", geometry]]

        # reacreate points with z
        xyz[geometry] = xyz.apply(lambda row: Point(row.x, row.y, row.z), axis=1)
        return xyz

    def find_kop(self) -> pd.Series:
        """ Return an educated guess as to the location of a survey's kickoff point, element-wise.
            The calling dataframe be a dataframe of survey points. """

        points = self._obj
        validate_required_columns(["is_in_lateral", "sequence"], points.columns)
        angles = points.shapes.relative_trajectory_angle()

        points = points.join(angles.loc[~points.is_in_lateral])
        points["theta"] = angles.theta
        points.loc[:, ["is_soft_corner", "is_hard_corner"]] = points.loc[
            :, ["is_soft_corner", "is_hard_corner"]
        ].fillna(False)
        points.loc[:, "theta"] = points.loc[:, "theta"].fillna(0)

        max_hard_corner_mask = (
            points.loc[points.is_hard_corner]
            .sequence.groupby(level=0)
            .max()
            .rename("hard")
        )

        max_soft_corner_mask = (
            points.loc[points.is_soft_corner]
            .sequence.groupby(level=0)
            .max()
            .rename("soft")
        )

        last_non_lateral_point_mask = (
            points.loc[~points.is_in_lateral]
            .sequence.groupby(level=0)
            .max()
            .rename("last_non_lateral")
        )

        # create a frame from the masks created above
        empty = pd.DataFrame(index=points.groupby(level=0).max().index)
        joined = (
            empty.join(max_hard_corner_mask)
            .join(max_soft_corner_mask)
            .join(last_non_lateral_point_mask)
        )

        # determine the sequence index of the kop by traversing the joined dataframe's columns
        # from left to right and taking the first non-na value for each row.
        kop_seq_index = (
            joined.fillna(method="bfill", axis=1)
            .iloc[:, 0]
            .rename("kop_seq")
            .fillna(-1)
            .astype(int)
            .to_frame()
            .set_index("kop_seq", append=True)
            .index
        )

        # mark kop points using sequence index
        points["is_kop"] = False
        kop_index = (
            points.reset_index(level=1)
            .set_index("sequence", append=True)
            .loc[kop_seq_index]
            .reset_index(level=1)
            .set_index("md", append=True)
            .index
        )

        points.loc[kop_index, "is_kop"] = True

        return points.loc[:, ["theta", "is_soft_corner", "is_hard_corner", "is_kop"]]

    def shapes_to_wkb(
        self, geometry_columns: List[str], srid: int = 4326
    ) -> pd.DataFrame:
        df = self._obj.copy(deep=True)
        for field in geometry_columns:
            df[field] = df[field].apply(lambda x: geo.shape_to_wkb(x, srid=srid))
        return df

    def wkb_to_shapes(self, geometry_columns: List[str]) -> pd.DataFrame:
        df = self._obj.copy(deep=True)
        for field in geometry_columns:
            df[field] = df[field].apply(geo.wkb_to_shape)
        return df

    def column_as_set(self, column_name: str) -> set:
        return column_as_set(self._obj, column_name)


if __name__ == "__main__":

    import loggers

    loggers.config()

    api14s = [
        "42461409160000",
        "42383406370000",
        "42461412100000",
        "42461412090000",
        "42461411750000",
        "42461411740000",
        "42461411730000",
        "42461411720000",
        "42461411600000",
        "42461411280000",
        "42461411270000",
        "42461411260000",
        "42383406650000",
        "42383406640000",
        "42383406400000",
        "42383406390000",
        "42383406380000",
        "42461412110000",
        "42383402790000",
        "42461397940000",
    ]
    api10s = None
    path = IHSPath.well_h_geoms
    kwargs: dict = {}
    create_index = True

    pd.set_option("display.float_format", lambda x: "%.2f" % x)
    pd.set_option("precision", 2)

    async def async_wrapper():

        locations, surveys, points = await pd.DataFrame.shapes.from_ihs(
            path, api14s=api14s
        )

        # points
        points = points.shapes.index_survey_points()
        kops = points.shapes.find_kop()
        points = points.join(kops)

        # surveys
        laterals = points[points.is_in_lateral].shapes.as_line(label="lateral_only")
        sticks = points.shapes.as_stick()
        bent_sticks = points.shapes.as_bent_stick()
        surveys = surveys.join(laterals).join(sticks).join(bent_sticks)

        """
        extract depths and save to WellDepths model
        """

        geomset = WellGeometrySet(locations=locations, surveys=surveys, points=points)
        # geomset.shapes_as_wkb().wkb_as_shapes()

        geomset.to_geojson(output_dir="data", subset=["xxxxxx"])

        from db import db

        await db.startup()

        model = geomset.models["locations"]
        for_load = locations.shapes.shapes_to_wkb(["geom"])
        await model.bulk_upsert(for_load)

        model = geomset.models["surveys"]
        for_load = surveys.shapes.shapes_to_wkb(
            ["wellbore", "lateral_only", "stick", "bent_stick"]
        )
        await model.bulk_upsert(for_load)

        model = geomset.models["points"]
        for_load = points.shapes.shapes_to_wkb(["geom"])
        await model.bulk_upsert(for_load)
