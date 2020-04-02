import logging
from typing import List, Union

import pandas as pd

import schemas as sch
from calc.sets import WellSet
from collector import FracFocusClient, IHSClient, IHSPath
from util.pd import column_as_set
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

        data = await FracFocusClient.get_jobs(api14s=api14s, api10s=api10s, **kwargs)
        fracs = sch.FracParameterSet(wells=data).df(create_index=create_index)
        return fracs

    def column_as_set(self, column_name: str) -> set:
        return column_as_set(self._obj, column_name)


if __name__ == "__main__":

    import loggers

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

    async def async_wrapper():

        wells, depths, fracs, _, ips = await pd.DataFrame.wells.from_ihs(
            IHSPath.well_h, api14s=api14s
        )

        # fracs = fracs.dropna(how="any")
        fracfocus: pd.DataFrame = await pd.DataFrame.wells.from_fracfocus(api14s=api14s)
        fracs = fracs.combine_first(fracfocus)  # enrich frac params from fracfocus

        from db.models import WellHeader

        dir(WellHeader)
        model_cols = {*WellHeader.c.names}
        df_cols = {*wells.columns.tolist()}

        [c for c in model_cols if c not in df_cols]
        [c for c in df_cols if c not in model_cols]

        """
            change status to provider_status
            determine real status
            set is_producing flag
            set status


        """
