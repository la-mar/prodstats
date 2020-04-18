import asyncio
import logging
from typing import Any, Coroutine, Dict, List, Union

import numpy as np
import pandas as pd

import schemas as sch
from calc.sets import WellSet
from collector import FracFocusClient, IHSClient, IHSPath
from const import HoleDirection
from db.models import ProdHeader
from util.pd import validate_required_columns, x_months_ago
from util.types import PandasObject

logger = logging.getLogger(__name__)

STATUS_INDICATOR_MAP = {
    "is_other": "OTHER",
    "is_inactive_pa": "INACTIVE-PA",
    "is_ta": "TA",
    "is_producing": "PRODUCING",
    "is_completed": "COMPLETED",
    "is_duc": "DUC",
    "is_drilling": "DRILLING",
    "is_permit": "PERMIT",
    "is_stale_permit": "STALE-PERMIT",
}

PRODUCING_STATES = [
    "PRODUCING",
    "COMPLETED",
    "PERMIT",
    "STALE-PERMIT",
    "TA",
    "DRILLING",
    "DUC",
]


@pd.api.extensions.register_dataframe_accessor("wells")
class Well:
    def __init__(self, obj: PandasObject):
        self._obj: PandasObject = obj

    @staticmethod
    def _to_wellset(data: List[Dict[str, Any]], create_index: bool) -> WellSet:
        wells = sch.WellRecordSet(wells=data).df(create_index=create_index)
        depths = sch.WellDepthSet(wells=data).df(create_index=create_index)
        fracs = sch.FracParameterSet(wells=data).df(create_index=create_index)
        ips = sch.IPTestSet(wells=data).df(create_index=create_index)

        return WellSet(wells=wells, depths=depths, fracs=fracs, ips=ips)

    @classmethod
    async def from_ihs(
        cls,
        path: IHSPath,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        create_index: bool = True,
        **kwargs,
    ) -> WellSet:

        data: List[Dict[str, Any]] = await IHSClient.get_wells(
            api14s=api14s, api10s=api10s, path=path, **kwargs
        )
        return cls._to_wellset(data, create_index)

    @classmethod
    async def from_fracfocus(
        cls,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        create_index: bool = True,
        **kwargs,
    ) -> WellSet:
        """ Get frac job data for the given api14s/api10s from the Frac Focus service """

        fracs = None

        try:
            data = await FracFocusClient.get_jobs(
                api14s=api14s, api10s=api10s, **kwargs
            )

            df = sch.FracParameterSet(wells=data).df(create_index=create_index)
            if not df.empty:
                fracs = df
        except Exception as e:
            logger.error(f"Failed to fetch FracFocus data -- {e}")

        return WellSet(fracs=fracs)

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

            if wellset.fracs is None:
                wellset.fracs = fracfocus.fracs
            else:
                if fracfocus.fracs is not None:
                    wellset.fracs = fracfocus.fracs.wells.combine_frac_parameters(
                        fracfocus.fracs
                    )

        return wellset

    @classmethod
    async def from_sample(  # TODO: merge this with from_multiple to leverage the same logic
        cls,
        path: IHSPath,
        n: int = None,
        frac: float = None,
        area: str = None,
        create_index: bool = True,
    ) -> WellSet:
        optcount = sum([n is not None, frac is not None])
        if optcount < 1:
            raise ValueError("One of ['n', 'frac'] must be specified")
        if optcount > 1:
            raise ValueError("Only one of ['n', 'frac'] can be specified")

        # if isinstance(path, IHSPath):
        wellset = cls._to_wellset(
            await IHSClient.get_sample(path, n=n, frac=frac, area=area), create_index,
        )
        fracs = wellset.fracs
        fracfocus: WellSet = await cls.from_fracfocus(api14s=api14s, api10s=api10s)
        fracs = fracs.combine_first(fracfocus.fracs)
        fracs = fracs[(~fracs.fluid.isna()) & (~fracs.proppant.isna())]
        wellset.fracs = fracs
        return wellset

    def combine_frac_parameters(
        self, other: pd.DataFrame, dropna: bool = True
    ) -> pd.DataFrame:
        fracs = self._obj
        validate_required_columns(["fluid", "proppant"], fracs.columns)
        validate_required_columns(["fluid", "proppant"], other.columns)
        validate_required_columns(["api14"], fracs.index.names)
        validate_required_columns(["api14"], other.index.names)

        fracs = fracs.combine_first(other)
        if dropna:
            fracs = fracs[(~fracs.fluid.isna()) & (~fracs.proppant.isna())]
        return fracs

    def status_indicators(self, indicators_only: bool = False,) -> pd.DataFrame:
        df = self._obj

        required_columns = [
            "status",
            "spud_date",
            "comp_date",
            "permit_date",
            "last_prod_date",
        ]

        validate_required_columns(
            required_columns, df.columns,
        )

        """ Original logic:

        Is_GoodSymCode
            if Uppercase([IHS_Status]) not in ('OIL PRODUCER', 'OIL-WO', 'AT TOTAL DEPTH', 'WELL START', 'WELL PERMIT', 'TREATD', 'CANCEL', 'GAS PRODUCER', 'GAS-WO', 'TA') then [IHS_Status] else Null() endif

        Is_Other
            if IsNull([LastProd]) and ([SPUD_DATE] < "1971-01-01" or [SPUD_DATE] < DateTimeAdd(DateTimeToday(),-36,"months")) then "OTHER" else Null() endif
            # if last_prod is None and (spudded before 1971 or spudded < 3 years ago)

        Is_InactivePA
            if [LastProd] < DateAdd([ProductionDateCutoff],-12,"months") and !IsNull([LastProd]) then "INACTIVE-PA" else Null() endif

        Is_TA
            if [LastProd] < DateAdd([ProductionDateCutoff],-3,"months") and [LastProd] >= DateAdd([ProductionDateCutoff],-12,"months") then "TA" else Null() endif

        Is_Producing
            if [LastProd] >= DateAdd([ProductionDateCutoff],-3,"months") then "PRODUCING" else Null() endif

        Is_Completed
            if [COMP_DATE] >= DateAdd([ProductionDateCutoff],-9,"months") then "COMPLETED" else Null() endif

        Is_DUC
            if [SPUD_DATE] < DateAdd(MonthStart(DateTimeToday()),-1,"months") then "DUC" else Null() endif

        Is_Drilling
            if !IsNull([SPUD_DATE]) then "DRILLING" else Null() endif

        Is_Permit
            if [PERMIT_DATE] >= DateAdd(DateAdd(MonthStart(DateTimeToday()),-1,"months"),-36,"months") then "PERMIT" else Null() endif

        Is_StalePermit
            if [PERMIT_DATE] < DateAdd(DateAdd(MonthStart(DateTimeToday()),-1,"months"),-36,"months") then "STALE-PERMIT" else Null() endif

        Status
            if !IsNull([Is_GoodSymCode]) then [Is_GoodSymCode]
            elseif !IsNull([Is_Other]) then [Is_Other]
            elseif !IsNull([Is_InactivePA]) then [Is_InactivePA]
            elseif !IsNull([Is_TA]) then [Is_TA]
            elseif !IsNull([Is_Producing]) then [Is_Producing]
            elseif !IsNull([Is_Completed]) then [Is_Completed]
            elseif !IsNull([Is_DUC]) then [Is_DUC]
            elseif !IsNull([Is_Drilling]) then [Is_Drilling]
            elseif !IsNull([Is_Permit]) then [Is_Permit]
            elseif !IsNull([Is_StalePermit]) then [Is_StalePermit]
            else "OTHER" endif

        """  # noqa

        last_prod_norm_date = x_months_ago(3)

        to_recategorize = [
            "OIL PRODUCER",
            "OIL-WO",
            "AT TOTAL DEPTH",
            "WELL START",
            "WELL PERMIT",
            "TREATD",
            "CANCEL",
            "GAS PRODUCER",
            "GAS-WO",
            "TA",
        ]

        df.loc[~df.status.isin(to_recategorize), "is_keeper_status"] = True

        other_mask = (df.last_prod_date.isna()) & (df.spud_date < x_months_ago(36))
        df.loc[other_mask, "is_other"] = True

        inactive_pa_mask = (~df.last_prod_date.isna()) & (
            df.last_prod_date < x_months_ago(12, relative_to=last_prod_norm_date)
        )
        df.loc[inactive_pa_mask, "is_inactive_pa"] = True

        is_ta_mask = (
            df.last_prod_date < x_months_ago(3, relative_to=last_prod_norm_date)
        ) & (df.last_prod_date < x_months_ago(12, relative_to=last_prod_norm_date))
        df.loc[is_ta_mask, "is_ta"] = True

        is_producing_mask = df.last_prod_date >= x_months_ago(
            3, relative_to=last_prod_norm_date
        )
        df.loc[is_producing_mask, "is_producing"] = True

        is_completed_mask = df.comp_date.notnull()
        df.loc[is_completed_mask, "is_completed"] = True

        is_duc_mask = df.spud_date < x_months_ago(3, relative_to=last_prod_norm_date)
        df.loc[is_duc_mask, "is_duc"] = True

        is_drilling_mask = df.spud_date.notnull()
        df.loc[is_drilling_mask, "is_drilling"] = True

        is_permit_mask = df.permit_date >= x_months_ago(36)
        df.loc[is_permit_mask, "is_permit"] = True

        is_stale_permit_mask = df.permit_date >= x_months_ago(36)
        df.loc[is_stale_permit_mask, "is_stale_permit"] = True

        indicators = [
            "is_keeper_status",
            "is_other",
            "is_inactive_pa",
            "is_ta",
            "is_producing",
            "is_completed",
            "is_duc",
            "is_drilling",
            "is_permit",
            "is_stale_permit",
        ]

        df.loc[:, indicators] = df.loc[:, indicators].fillna(False)

        if indicators_only:
            return_columns = indicators
        else:
            return_columns = required_columns + indicators

        return df.loc[:, return_columns]

    async def last_prod_date(
        self, path: IHSPath, prefer_local: bool = False
    ) -> pd.DataFrame:  # TODO: passing path here is clunky # noqa
        """ Fetch the last production dates from IHS service (default) or from
            the application's local database """

        prod_headers = None
        api14s_in = self._obj.index.values.tolist()

        if prefer_local:
            logger.debug("fetching production headers from app database")
            prod_header_columns = ["primary_api14", "last_prod_date"]
            prod_headers = (
                await ProdHeader.select(*prod_header_columns)
                .where(ProdHeader.primary_api14.in_(api14s_in))
                .gino.all()
            )
            prod_headers = (
                pd.DataFrame(prod_headers, columns=prod_header_columns)
                .rename(columns={"primary_api14": "api14"})
                .set_index("api14")
            )
            if prod_headers.empty:
                prod_headers = None

        if prod_headers is None:
            logger.debug("fetching production headers from ihs service")
            prod = await IHSClient.get_production(path, api14s=api14s_in, related=False)

            prod_df = pd.DataFrame(prod)

            if not prod_df.empty:
                prod_headers = (
                    prod_df.set_index("api14")
                    .dates.apply(pd.Series)
                    .loc[:, "last_prod"]
                    .rename("last_prod_date")
                    .to_frame()
                )

                prod_headers = prod_headers.dropna()

                # TODO: log how many failed conversions occurred (count NaTs?)
                prod_headers = pd.to_datetime(
                    prod_headers.last_prod_date, errors="coerce"
                ).to_frame()

                # an api14 can sometimes be tied to more than one producing entity. To handle this,
                # take the max last_prod_date for each api14.
                prod_headers = prod_headers.groupby(level=0).max()
            else:
                prod_headers = pd.DataFrame(
                    index=pd.Index([], dtype="object", name="api14"),
                    columns=["last_prod_date"],
                )

            api14s_out = prod_headers.index.values.tolist()
            logger.debug(
                f"Found production headers for {len(api14s_out)} of {len(api14s_in)} wells"
            )
        return prod_headers

    def assign_status(
        self,
        # target_column: str = "new_status",
        status_column: str = "status",
        how: str = "waterfall",
        status_indicator_map: Dict[str, str] = None,
        detail: bool = False,
        as_labels: bool = False,
        empty_label_placeholder: str = ".",
    ) -> pd.DataFrame:
        """ Assign well status using indicators existing in the passed DataFrame or
            using pd.DataFrame.wells.status_indicators() to generate them if they arent
            present.

        Keyword Arguments:
            target_column {str} -- column name for status assignments (default: "new_status")
            how {str} -- assignment methodology to use, currently the only available option
                is the default. (default: "waterfall")
            status_column {str} -- name of column containing the original stati from
                the data provider (default: "status")
            detail {bool} -- return the intermediate calculations used in assignments
            status_indicator_map {Dict[str, str]} -- status_indicator_map of indicator column names
                and their corresponding status value to be used in waterfall assignment.
                The precidence of assignment is inferred from the order or items in the
                status_indicator_map (default: const.STATUS_INDICATOR_MAP).

        Raises:
            ValueError

        Returns:
            pd.DataFrame
        """
        wells = self._obj

        well_columns = [
            "status",
            "spud_date",
            "comp_date",
            "permit_date",
            "last_prod_date",
        ]
        validate_required_columns(
            well_columns, wells.columns,
        )

        target_column = "new_status"
        status: pd.DataFrame = wells.loc[:, well_columns]
        status = status.wells.status_indicators()

        status_indicator_map = (
            status_indicator_map if status_indicator_map else STATUS_INDICATOR_MAP
        )

        # seed with keeper values from original status column
        status.loc[status.is_keeper_status, target_column] = status.loc[
            status.is_keeper_status, status_column
        ]

        if how == "waterfall":
            for column_name, label in status_indicator_map.items():
                selected = status.loc[
                    status[target_column].isna() & status[column_name], column_name
                ]

                if label is not None:
                    selected = selected.replace({True: label})

                status.loc[status[target_column].isna(), target_column] = selected
        else:
            raise ValueError(f"Invalid how value: use 'waterfall'")

        if not detail:
            status = status.loc[:, [target_column]]

        # overwrite original status with new status
        if status_column in status.columns:
            status = status.drop(columns=[status_column])

        status = status.rename(columns={target_column: status_column})

        if as_labels:
            status = status.replace(
                {
                    "is_other": {True: "OTHER"},
                    "is_inactive_pa": {True: "INACTIVE-PA"},
                    "is_ta": {True: "TA"},
                    "is_producing": {True: "PRODUCING"},
                    "is_completed": {True: "COMPLETED"},
                    "is_duc": {True: "DUC"},
                    "is_drilling": {True: "DRILLING"},
                    "is_permit": {True: "PERMIT"},
                    "is_stale_permit": {True: "STALE-PERMIT"},
                }
            ).replace({False: empty_label_placeholder})

        return status

    def is_producing(
        self, status_column: str = "status", producing_states: List[str] = None
    ) -> pd.Series:

        validate_required_columns([status_column], self._obj.columns)
        producing_states = producing_states or PRODUCING_STATES
        return self._obj.status.isin(producing_states)

    def merge_lateral_lengths(self) -> pd.DataFrame:
        """ Merge perfll and lateral_length into a single column, preferring perfll """

        wells = self._obj

        if "lateral_length" not in wells.columns:
            wells["lateral_length"] = np.nan

        required_columns = ["perfll", "lateral_length"]
        validate_required_columns(required_columns, wells.columns)

        # ? get lateral_length, preferring perll over lateral_length
        latlens = wells.loc[:, required_columns]
        latlens.loc[latlens.perfll.notnull(), "lateral_length"] = np.nan
        latlens = (
            latlens.reset_index()
            .melt(
                id_vars="api14",
                var_name="lateral_length_type",
                value_name="lateral_length",
            )
            .set_index("api14")
            .dropna(how="any")
        )
        return latlens

    def process_fracs(self):

        fracs = self._obj
        validate_required_columns(
            ["fluid", "proppant", "lateral_length", "lateral_length_type"],
            fracs.columns,
        )

        fracs = fracs.dropna(how="all", subset=["fluid", "proppant"])

        # TODO: validate fluid/proppant UOM and convert to BBL/LB where necessary

        # convert lb & bbl to lb/ft & bbl/ft
        per_ft = (
            fracs.loc[:, ["fluid", "proppant"]]
            .div(fracs["lateral_length"], axis=0)
            .rename(columns={"fluid": "fluid_bbl_ft", "proppant": "proppant_lb_ft"})
        )

        fracs = fracs.join(per_ft)

        # rename fluid & proppant and drop uoms
        fracs = fracs.rename(
            columns={"fluid": "fluid_bbl", "proppant": "proppant_lb"}
        ).drop(columns=["fluid_uom", "proppant_uom"])

        fracs = fracs.dropna(
            how="all",
            subset=[
                "fluid_bbl",
                "proppant_lb",
                "lateral_length_type",
                "lateral_length",
                "fluid_bbl_ft",
                "proppant_lb_ft",
            ],
        )

        return fracs

    def melt_depths(self) -> pd.DataFrame:

        validate_required_columns(["api14"], self._obj.index.names)

        depths_melted = (
            self._obj.dropna(how="all")
            .reset_index()
            .melt(id_vars=["api14"], var_name="property_name")
        )
        depths_melted["aggregate_type"] = None
        depths_melted["name"] = depths_melted.property_name
        depths_melted = depths_melted.set_index(
            ["api14", "property_name", "aggregate_type"]
        )
        return depths_melted


if __name__ == "__main__":

    import loggers
    import calc.geom  # noqa
    import calc.prod  # noqa
    import util.pd  # noqa
    from db.models import (  # noqa
        WellHeader,
        FracParameters,
        WellStat,
        WellDepth,
        WellLink,
        IPTest,
    )

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

    wellset = util.aio.async_to_sync(
        pd.DataFrame.wells.from_ihs(path=path, api14s=api14s)
    )
    wells, depths, fracs, ips, *other = wellset

    async def coro():
        from db import db

        if not db.is_bound():
            await db.startup()

        load_cols = (WellDepth.api14, WellDepth.name, WellDepth.value)
        md_tvd_from_db = (
            await WellDepth.query.where(
                WellDepth.api14.in_(api14s) & WellDepth.name.in_(["md", "tvd"])
            )
            .gino.load(load_cols)
            .all()
        )

        md_tvd_from_db = pd.DataFrame.from_records(md_tvd_from_db).pivot(
            index=0, columns=1
        )
        md_tvd_from_db.columns = md_tvd_from_db.columns.droplevel(0)
        md_tvd_from_db.index.name = "api14"
        md_tvd_from_db.columns.name = ""
        md_tvd_from_db


# # geoms = await pd.DataFrame.shapes.from_ihs(IHSPath.well_h_geoms, api14s=api14s)

# # fracfocus = await pd.DataFrame.wells.from_fracfocus(api14s=api14s)

# wellset = await pd.DataFrame.wells.from_sample(
#     IHSPath.well_h_sample, n=100, area="tx-upton"
# )

# wells, depths, fracs, ips, *other = wellset

# depths.wells.melt_depths()

# api14s = wells.util.column_as_set("api14")

# geoms = await pd.DataFrame.shapes.from_ihs(IHSPath.well_v_geoms, api14s=api14s)
# geoms
# geoms.surveys
