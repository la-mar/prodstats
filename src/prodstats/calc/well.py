import asyncio
import logging
from typing import Coroutine, Dict, List, Union

import pandas as pd

import schemas as sch
from calc.sets import WellSet
from collector import FracFocusClient, IHSClient, IHSPath
from const import HoleDirection
from db import db
from db.models import ProdHeader
from util.pd import validate_required_columns, x_months_ago
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

    def status_indicators(self, indicators_only: bool = False, as_labels: bool = False):
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

        provider_status_targets = [
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

        df.loc[~df.status.isin(provider_status_targets), "is_keeper_status"] = True

        other_mask = (df.last_prod_date.isna()) | (df.spud_date < x_months_ago(36))
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

        is_completed_mask = df.comp_date >= x_months_ago(9)
        df.loc[is_completed_mask, "is_completed"] = True

        is_duc_mask = df.comp_date >= x_months_ago(3, relative_to=last_prod_norm_date)
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

        if as_labels:
            df = df.replace(
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
            ).replace({False: None})

        return df.loc[:, return_columns]

    def assign_status(
        self,
        mapping: Dict[str, str],
        target_column: str = "new_status",
        how: str = "waterfall",
        original_status_column: str = "status",
        detail: bool = False,
    ) -> pd.DataFrame:
        """ Assign well status using indicators existing in the passed DataFrame or
            using pd.DataFrame.wells.status_indicators() to generate them if they arent
            present.

        Arguments:
            mapping {Dict[str, str]} -- mapping of indicator column names and their
                corresponding status value to be used in waterfall assignment. The
                precidence of assignment is inferred from the order or items in the
                mapping.

        Keyword Arguments:
            target_column {str} -- column name for status assignments (default: {"new_status"})
            how {str} -- assignment methodology to use, currently the only available option
                is the default. (default: {"waterfall"})
            original_status_column {str} -- name of column containing the original stati from
                the data provider (default: {"status"})
            detail {bool} -- return the intermediate calculations used in assignments

        Raises:
            ValueError

        Returns:
            pd.DataFrame
        """

        df = self._obj

        if "is_keeper_status" not in df.columns:
            df = df.wells.status_indicators(indicators_only=True)

        # seed with keeper values from original status column
        df.loc[df.is_keeper_status, target_column] = df.loc[
            df.is_keeper_status, original_status_column
        ]

        if how == "waterfall":
            for column_name, label in mapping.items():
                selected = df.loc[
                    df[target_column].isna() & df[column_name], column_name
                ]

                if label is not None:
                    selected = selected.replace({True: label})

                df.loc[df[target_column].isna(), target_column] = selected
        else:
            raise ValueError(f"Invalid how value: use 'waterfall'")

        if not detail:
            df = df.loc[:, [target_column]]

        return df

    async def production_headers(self, fields: List[str]) -> pd.DataFrame:
        """ Fetch the related production headers for the wells in the current DataFrame. """

        api14s_in = self._obj.index.values.tolist()
        prod_header_columns = ["primary_api14"] + fields
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

        api14s_out = prod_headers.index.values.tolist()

        logger.debug(
            f"Found production headers for {len(api14s_out)} of {len(api14s_in)} wells"
        )

        return prod_headers


if __name__ == "__main__":

    import loggers
    import calc.geom  # noqa
    import calc.prod  # noqa
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

    async def async_wrapper():

        # from util.jsontools import load_json

        # upton_api14s = load_json("./data/upton_api14s.json")
        # upton_prod_ids = load_json("./data/upton_prod_ids.json")
        # upton_entity12s = [x[:12] for x in upton_prod_ids]

        if not db.is_bound():
            await db.startup()

        wells, depths, fracs, _, ips = await pd.DataFrame.wells.from_multiple(
            hole_dir="H", api14s=api14s
        )

        geoms = await pd.DataFrame.shapes.from_ihs(IHSPath.well_h_geoms, api14s=api14s)

        # merge depth stats from wells and geoms
        depth_stats = geoms.points.shapes.depth_stats()
        depths_melted = (
            depths.dropna(how="all")
            .reset_index()
            .melt(id_vars=["api14"], var_name="property_name")
        )
        depths_melted["aggregate_type"] = None
        depths_melted["name"] = depths_melted.property_name
        depths_melted = depths_melted.set_index(
            ["api14", "property_name", "aggregate_type"]
        )

        depth_stats = depth_stats.append(depths_melted)
        depth_stats = depth_stats.dropna(subset=["value"])
        # load depth_stats to WellDepths

        # enrich well_headers
        wells["lateral_length"] = geoms.points.shapes.lateral_length()
        wells["provider_status"] = wells.status.str.upper()

        # calculate WELL STATUS
        status = wells.loc[:, ["status", "spud_date", "comp_date", "permit_date"]]
        status = status.join(
            await wells.wells.production_headers(fields=["last_prod_date"])
        )

        status = status.wells.status_indicators()
        new_status = status.wells.assign_status(
            mapping={
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
        )

        new_status.new_status.fillna(0).groupby(
            new_status.new_status
        ).count().sort_values(ascending=False)

        wells["status"] = new_status

        producing_states = [
            "PRODUCING",
            "COMPLETED",
            "PERMIT",
            "STALE-PERMIT",
            "TA",
            "DRILLING",
            "DUC",
        ]

        wells["is_producing"] = wells.status.isin(producing_states)
        wells[["status", "is_producing"]]

        """
        status.fillna(0).groupby("status").count().spud_date.sort_values(ascending=False)

        status
        OIL PRODUCER          1120
        ABANDON LOCATION       280
        AT TOTAL DEPTH         125
        WELL PERMIT            125
        OIL-WO                  92
        WELL START              81
        TREATD                  80
        GAS PRODUCER            69
        DRY & ABANDONED         44
        CANCEL                  40
        GAS-WO                  37
        JUNKED & ABANDONED       9
        ABD-OW                   8
        D&AW                     8
        ABD-GW                   3
        J&AW                     3
        SUSPENDED WELL           2
        WI-EOR                   1
        SWDOP                    1
        TA                       1
        TAW                      1
        J&AWOG                   1
        """

        # waterfall status assignment

        # status.loc[status.is_keeper_status, "new_status"] = status.loc[
        #     status.is_keeper_status, "status"
        # ]

        # status.loc[status.new_status.isna(), "new_status"] = status.loc[
        #     status.new_status.isna() & status.is_other, "is_other"
        # ].replace({True: "OTHER"})

        # status.loc[status.new_status.isna(), "new_status"] = status.loc[
        #     status.new_status.isna() & status.is_inactive_pa, "is_inactive_pa"
        # ].replace({True: "INACTIVE-PA"})

        # status.loc[status.new_status.isna(), "new_status"] = status.loc[
        #     status.new_status.isna() & status.is_inactive_pa, "is_inactive_pa"
        # ].replace({True: "INACTIVE-PA"})

        # self = status.wells

        """
            Is_Producing
                if Uppercase([Status]) in ('PRODUCING',
                'COMPLETED',
                'PERMIT',
                'STALE-PERMIT',
                'TA',
                'DRILLING',
                'DUC') then "PRODUCING" else "NON_PRODUCING" endif
        """
        #

        # from db.models import WellHeader

        # dir(WellHeader)
        # model_cols = {*WellHeader.c.names}
        # df_cols = {*wells.columns.tolist()}

        # [c for c in model_cols if c not in df_cols]
        # [c for c in df_cols if c not in model_cols]

        """
            change status to provider_status
            determine real status
            set is_producing flag
            set status


        """
