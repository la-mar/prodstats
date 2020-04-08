import asyncio
import logging
from typing import Coroutine, Dict, List, Union

import pandas as pd

import schemas as sch
from calc.sets import WellSet
from collector import FracFocusClient, IHSClient, IHSPath
from const import HoleDirection
from util.pd import validate_required_columns
from util.types import PandasObject

logger = logging.getLogger(__name__)


@pd.api.extensions.register_dataframe_accessor("wells")
class Well:
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
    ) -> WellSet:

        data = await IHSClient.get_wells(
            api14s=api14s, api10s=api10s, path=path, **kwargs
        )
        wells = sch.WellRecordSet(wells=data).df(create_index=create_index)
        depths = sch.WellDepthSet(wells=data).df(create_index=create_index)
        fracs = sch.FracParameterSet(wells=data).df(create_index=create_index)
        ips = sch.IPTestSet(wells=data).df(create_index=create_index)

        return WellSet(wells=wells, depths=depths, fracs=fracs, stats=None, ips=ips)

    @classmethod
    async def from_fracfocus(
        cls,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        create_index: bool = True,
        **kwargs,
    ) -> pd.DataFrame:
        """ Get frac job data for the given api14s/api10s from the Frac Focus service """

        fracs = pd.DataFrame()

        try:
            data = await FracFocusClient.get_jobs(
                api14s=api14s, api10s=api10s, **kwargs
            )
            fracs = sch.FracParameterSet(wells=data).df(create_index=create_index)
        except Exception as e:
            logger.error(f"Failed to fetch FracFocus data -- {e}")

        return fracs

    @classmethod
    async def from_multiple(
        cls,
        hole_dir: Union[HoleDirection, str],
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        create_index: bool = True,
        use_ihs: bool = True,
        use_fracfocus: bool = True,
        use_drillinginfo: bool = True,
        ihs_kwargs: Dict = None,
        fracfocus_kwargs: Dict = None,
        drillinginfo_kwargs: Dict = None,
        **kwargs,
    ) -> WellSet:
        """ Return an dataset enriched from multiple sources """

        ihs_kwargs = ihs_kwargs or {}
        fracfocus_kwargs = fracfocus_kwargs or {}
        drillinginfo_kwargs = drillinginfo_kwargs or {}

        if not isinstance(hole_dir, HoleDirection):
            hole_dir = HoleDirection(str(hole_dir).upper())

        if hole_dir == HoleDirection.H:
            ihs_path = IHSPath.well_h
        elif hole_dir == HoleDirection.V:
            ihs_path = IHSPath.well_v
        else:
            raise TypeError(f"hole_dir must be specified!")

        wellset = WellSet(wells=None, depths=None, fracs=None, stats=None, ips=None)
        coros: List[Coroutine] = []

        if use_ihs:
            coros.append(
                cls.from_ihs(ihs_path, api14s=api14s, api10s=api10s, **ihs_kwargs)
            )

        if use_fracfocus:
            coros.append(
                cls.from_fracfocus(api14s=api14s, api10s=api10s, **fracfocus_kwargs)
            )

        results: List[Union[WellSet, pd.DataFrame]] = await asyncio.gather(*coros)

        if use_ihs:
            wellset = results.pop(0)

        if use_fracfocus:
            fracfocus = results.pop(0)
            fracs = wellset.fracs

            if fracs is None:
                fracs = pd.DataFrame()

            fracs = fracs.combine_first(fracfocus)
            fracs = fracs[(~fracs.fluid.isna()) & (~fracs.proppant.isna())]
            wellset.fracs = fracs

        return wellset

    async def enrich_frac_parameters(self, dropna: bool = True):
        fracs = self._obj
        validate_required_columns(["fluid", "proppant"], fracs.columns)

        api14s = fracs.reset_index().util.column_as_set("api14")
        fracfocus = await pd.DataFrame.wells.from_fracfocus(api14s=api14s)
        fracs = fracs.combine_first(fracfocus)
        if dropna:
            fracs = fracs[(~fracs.fluid.isna()) & (~fracs.proppant.isna())]
        return fracs


if __name__ == "__main__":

    import loggers
    import calc.geom  # noqa
    import util.pd  # noqa

    # import random

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
    ]
    api10s = None
    path = IHSPath.well_h
    kwargs: dict = {}
    create_index = True

    # async def async_wrapper():

    #     wells, depths, fracs, _, ips = await pd.DataFrame.wells.from_multiple(
    #         hole_dir="H", api14s=api14s
    #     )

    #     geoms = await pd.DataFrame.shapes.from_ihs(IHSPath.well_h_geoms, api14s=api14s)

    #     lateral_length = (
    #         geoms.points.loc[geoms.points.is_in_lateral]
    #         .reset_index(level=1)
    #         .loc[:, "md"]
    #         .groupby(level=0)
    #         .agg(["min", "max"])
    #         .apply(lambda row: row["max"] - row["min"], axis=1)
    #         .rename("lateral_length")
    #     )

    #     depth_stats = geoms.points.shapes.depth_stats()

    #     #! TODO: Start here -- make this work
    #     depth_stats.xs("avg", level=2).drop("name", axis=1).reset_index(level=1).pivot(
    #         columns="property_name"
    #     )

    #     depths.dropna(how="all")

    #     from db.models import WellHeader

    #     dir(WellHeader)
    #     model_cols = {*WellHeader.c.names}
    #     df_cols = {*wells.columns.tolist()}

    #     [c for c in model_cols if c not in df_cols]
    #     [c for c in df_cols if c not in model_cols]

    #     """
    #         change status to provider_status
    #         determine real status
    #         set is_producing flag
    #         set status

    #     """
