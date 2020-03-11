import itertools
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

import config as conf
import const
from collector import IHSClient, IHSPath
from schemas import ProductionWellSet
from util.enums import Enum

logger = logging.getLogger(__name__)

PandasObject = Union[pd.DataFrame, pd.Series]


class ProdStatInterval(str, Enum):
    FIRST = "first"
    LAST = "last"
    PEAKNORM = "peaknorm"
    ALL = "all"


class ProdSet:
    def __init__(
        self,
        header: pd.DataFrame = None,
        monthly: pd.DataFrame = None,
        stats: pd.DataFrame = None,
    ):
        self.header = header
        self.monthly = monthly
        self.stats = stats

    def __repr__(self):
        h = self.header.shape[0]
        m = self.monthly.shape[0]
        s = self.stats.shape[0]
        return f"header={h} monthly={m} stats={s}"


CALC_MONTHS: List[Optional[int]] = [1, 3, 6, 12, 18, 24]
CALC_NORM_VALUES = [
    (1000, "1k"),
    (3000, "3k"),
    (5000, "5k"),
    (7500, "7500"),
    (10000, "10k"),
]
CALC_INTERVALS = ProdStatInterval.members()
CALC_AGG_TYPES = ["sum"]
CALC_INCLUDE_ZEROES = [True, False]


def _validate_required_columns(required: List[str], columns: List[str]):
    missing = []
    for x in required:
        if x not in columns:
            missing.append(x)

    if len(missing) > 0:
        raise KeyError(f"Missing columns: {missing}")


@pd.api.extensions.register_dataframe_accessor("prodstats")
class ProdStats:
    def __init__(self, obj: PandasObject):
        # self._validate(obj)
        self._obj: PandasObject = obj

    @classmethod
    async def from_ihs(
        cls,
        path: IHSPath,
        entities: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        entity12s: Union[str, List[str]] = None,
        create_index: bool = True,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch production records from the internal IHS service.

        Arguments:
            id {Union[str, List[str]]} -- can be a single or list of producing entities or api10s

        Keyword Arguments:
            params {Dict} -- additional params to pass to client.get() (default: None)
            resource_path {str} -- url resource bath (default: "prod/h")
            timeout {int} -- optional timeout period for production requests

        Returns:
            pd.DataFrame -- DataFrame of monthly production for the given ids
        """
        data = await IHSClient.get_production_wells(
            entities=entities, api10s=api10s, entity12s=entity12s, path=path, **kwargs
        )
        wellset = ProductionWellSet(wells=data)
        return wellset.df(create_index=create_index)

    def make_aliases(
        self,
        columns: List[str],
        agg_type: str,
        include_zeroes: bool,
        range_name: ProdStatInterval = None,
        months: Union[int, str] = None,
        norm_by_label: str = None,
    ) -> Dict[str, str]:

        nonzero = "_nonzero" if not include_zeroes else ""
        norm_by_label = f"_per{norm_by_label}" if norm_by_label else ""
        range_name = (
            range_name.value if range_name != range_name.ALL else ""  # type:ignore
        )
        range_label = f"_{range_name}{months}mo" if months and range_name else ""

        return {
            k: f"{k}_{agg_type}{range_label}{norm_by_label}{nonzero}" for k in columns
        }

    def interval(
        self,
        monthly: pd.DataFrame,
        range_name: ProdStatInterval,
        months: int = None,
        columns: List[str] = ["oil", "gas", "water", "boe"],
        agg_type: str = "sum",
        include_zeroes: bool = True,
        norm_by_ll: int = None,
        norm_by_label: str = None,
        melt: bool = True,
    ) -> pd.DataFrame:

        if not isinstance(range_name, ProdStatInterval):
            raise TypeError(
                f"inverval must be of type ProdStatInterval, not {type(range_name)}"
            )

        if range_name == range_name.ALL and months is not None:
            raise ValueError("Must not specify months when range_name is set to ALL")

        if norm_by_ll is not None and not norm_by_label:
            raise ValueError(
                "Must pass a value for norm_by_label when norm_by_ll is not None"
            )

        alias_map: Dict[str, str] = self.make_aliases(
            columns=columns,
            agg_type=agg_type,
            range_name=range_name,
            months=months,
            include_zeroes=include_zeroes,
            norm_by_label=norm_by_label,
        )

        agg_map: Dict[str, str] = {k: agg_type for k in columns}
        mask: pd.Series = None
        if range_name == ProdStatInterval.FIRST:
            mask = monthly.prod_month <= months
        elif range_name == ProdStatInterval.LAST:
            mask = (self._obj.prod_months - monthly.prod_month) <= months
        elif range_name == ProdStatInterval.PEAKNORM:
            mask = (monthly.peak_norm_month >= 0) & (monthly.peak_norm_month <= months)
        else:
            mask = monthly.prod_month.notnull()

        df = monthly.loc[mask, columns + ["prod_month", "peak_norm_month"]]

        aggregated = df.groupby(level=0).agg(agg_map).rename(columns=alias_map)
        aggregated["start_month"] = df.groupby(level=0).prod_month.min()
        aggregated["end_month"] = df.groupby(level=0).prod_month.max()
        aggregated["start_date"] = (
            df.reset_index(level=1).groupby(level=0).prod_date.min()
        )
        aggregated["end_date"] = (
            df.reset_index(level=1).groupby(level=0).prod_date.max()
        )
        aggregated["includes_zeroes"] = include_zeroes

        aggregated["ll_norm_value"] = norm_by_ll or np.nan
        aggregated["is_ll_norm"] = pd.notnull(aggregated.ll_norm_value)
        aggregated["is_peak_norm"] = (
            True if range_name == ProdStatInterval.PEAKNORM else False
        )
        aggregated["aggregate_type"] = agg_type
        aggregated["comments"] = None

        api10 = ""
        if "api10" in aggregated.columns and aggregated.shape[0] > 0:
            api10 = aggregated.iloc[0].api10

        logger.debug(
            f"{api10} calculated prodstats: {list(alias_map.values())}",
            extra={"api10": api10, **alias_map},
        )
        if melt:
            return aggregated.reset_index().melt(
                id_vars=[c for c in aggregated.columns if c not in alias_map.values()]
                + ["api10"],
                var_name="name",
                value_name="value",
            )
        else:
            return aggregated

    @staticmethod
    def _generate_option_sets(
        months: List[Optional[int]] = CALC_MONTHS,
        norm_values: List[Tuple[int, str]] = CALC_NORM_VALUES,
        range_names: List[ProdStatInterval] = CALC_INTERVALS,
        agg_types: List[str] = CALC_AGG_TYPES,
        include_zeroes: List[bool] = CALC_INCLUDE_ZEROES,
    ) -> List[Dict[str, Any]]:
        interval_configs = list(
            itertools.product(
                months, norm_values, range_names, agg_types, include_zeroes
            )
        )

        option_sets = []
        for params in interval_configs:
            option_sets.append(
                {
                    "months": params[0],
                    "norm_by_ll": params[1][0],
                    "norm_by_label": params[1][1],
                    "range_name": params[2],
                    "agg_type": params[3],
                    "include_zeroes": params[4],
                }
            )

        logger.debug(f"generated {len(option_sets)} option sets")
        return option_sets

    @classmethod
    def generate_option_sets(
        cls,
        months: List[Optional[int]] = CALC_MONTHS,
        norm_values: List[Tuple[int, str]] = CALC_NORM_VALUES,
        range_names: List[ProdStatInterval] = CALC_INTERVALS,
        agg_types: List[str] = CALC_AGG_TYPES,
        include_zeroes: List[bool] = CALC_INCLUDE_ZEROES,
    ) -> List[Dict[str, Any]]:
        bounded_intervals = [x for x in CALC_INTERVALS if x != ProdStatInterval.ALL]
        bounded = cls._generate_option_sets(
            months=months,
            norm_values=norm_values,
            range_names=bounded_intervals,
            agg_types=agg_types,
            include_zeroes=include_zeroes,
        )
        unbounded = cls._generate_option_sets(
            months=[None],
            norm_values=norm_values,
            range_names=[ProdStatInterval.ALL],  # type: ignore
            agg_types=agg_types,
            include_zeroes=include_zeroes,
        )

        return bounded + unbounded

    def stats(
        self,
        monthly: pd.DataFrame,
        option_sets: List[Dict[str, Any]] = None,
        melt: bool = True,
    ) -> pd.DataFrame:
        """ Calcuate the aggregate production stats defined by the passed option sets.
            If no option sets are passed, self.generate_option_sets() is used to create them
            from the app configuration."""

        option_sets = option_sets or self.generate_option_sets()

        stats = pd.DataFrame()
        for opts in option_sets:
            stats = stats.append(self.interval(monthly=monthly, **opts))
        return stats

    def calc(self) -> ProdSet:

        df = self._obj.sort_values(["api14", "status"], ascending=False)

        header = (
            df.groupby(level=0)
            .agg(
                {
                    "api14": "first",
                    "entity12": "first",
                    "status": "first",
                    "perf_upper": "first",
                    "perf_lower": "first",
                    "products": "first",
                    "perfll": "first",
                    "provider": "first",
                    "provider_last_update_at": "first",
                }
            )
            .rename(columns={"api14": "primary_api14"})
        )

        header["related_wells"] = (
            df.loc[:, ["api14", "entity", "status"]]
            .reset_index(level=1, drop=True)
            .drop_duplicates()
            .groupby(level=0)
            .apply(lambda x: x.to_dict(orient="records"))
        )

        monthly = df.copy(deep=True)
        monthly = monthly.loc[
            :,
            [
                "oil",
                "gas",
                "water",
                "perfll",
                "water_cut",
                "days_in_month",
                "gor",
                "api14",
            ],
        ]
        monthly["boe"] = monthly.oil + (monthly.gas.div(const.MCF_TO_BBL_FACTOR))
        # monthly = monthly.sort_values("api14", ascending=False)
        monthly = monthly.groupby(level=[0, 1]).first().drop(columns=["api14"])
        monthly = monthly.sort_index(ascending=True)
        monthly["prod_month"] = monthly.groupby(level=0).cumcount() + 1
        monthly["prod_days"] = (
            monthly.groupby(level=[0, 1])
            .sum()
            .groupby(level=[0])
            .days_in_month.cumsum()
        )

        peak30 = monthly.prodcalc.peak30()
        header = header.join(peak30)

        monthly["peak_norm_month"] = monthly.prod_month - header.peak30_month + 1
        monthly["oil_percent"] = monthly.oil.div(monthly.boe).mul(100)
        monthly["peak_norm_days"] = (
            monthly[monthly.peak_norm_month >= 0]
            .groupby(level=[0, 1])
            .sum()
            .groupby(level=[0])
            .days_in_month.cumsum()
        )

        monthly["oil_avg_daily"] = monthly.oil.div(monthly.days_in_month)
        monthly["gas_avg_daily"] = monthly.gas.div(monthly.days_in_month)
        monthly["water_avg_daily"] = monthly.water.div(monthly.days_in_month)
        monthly["boe_avg_daily"] = monthly.boe.div(monthly.days_in_month)

        header["prod_months"] = monthly.groupby(level=0).prod_month.max()
        header["prod_days"] = monthly.groupby(level=0).prod_days.max()
        header["peak_norm_months"] = monthly.groupby(level=0).peak_norm_month.max()
        header["peak_norm_days"] = monthly.groupby(level=0).peak_norm_days.max()
        header["first_prod_date"] = (
            monthly.reset_index(level=1).groupby(level=0).prod_date.min()
        )
        header["last_prod_date"] = (
            monthly.reset_index(level=1).groupby(level=0).prod_date.max()
        )

        monthly = monthly.join(monthly.prodcalc.norm_to_ll(value=1000, suffix="1k"))
        monthly = monthly.join(monthly.prodcalc.norm_to_ll(value=5000, suffix="5k"))
        monthly = monthly.join(monthly.prodcalc.norm_to_ll(value=7500, suffix="7500"))
        monthly = monthly.join(monthly.prodcalc.norm_to_ll(value=10000, suffix="10k"))
        monthly = monthly.drop(columns=["perfll"])

        stats = header.prodstats.stats(monthly, melt=False)

        return ProdSet(header=header, monthly=monthly, stats=stats)


@pd.api.extensions.register_dataframe_accessor("prodcalc")
class ProductionCalculator:
    peak_norm_limit: int = conf.PEAK_NORM_LIMIT

    def __init__(self, obj: PandasObject):
        # self._validate(obj)
        self._obj: PandasObject = obj

    def norm_to_ll(
        self,
        value: int,
        suffix: str,
        columns: List[str] = ["oil", "gas", "water", "boe"],
    ) -> pd.DataFrame:
        """ Normalize to an arbitrary lateral length """

        _validate_required_columns(
            columns + ["prod_month", "perfll"], self._obj.columns
        )

        alias_map = {k: f"{k}_norm_{suffix}" for k in columns}
        factors = (self._obj["perfll"] / value).values
        return self._obj.div(factors, axis=0).loc[:, columns].rename(columns=alias_map)

    def peak30(self) -> pd.DataFrame:
        """ Generate peak30 statistics, bounded by the configured peak_norm_limit """
        _validate_required_columns(["oil", "prod_month"], self._obj.columns)
        peak_range = self._obj.prod_month <= self.peak_norm_limit
        df = self._obj.loc[peak_range, :]

        peak30 = (
            df.loc[
                df.groupby(level=0).oil.idxmax().dropna().values, ["oil", "prod_month"],
            ]
            .reset_index(level=1)
            .rename(
                columns={
                    "prod_date": "peak30_date",
                    "oil": "peak30_oil",
                    "prod_month": "peak30_month",
                }
            )
        )
        peak30["peak30_gas"] = df.groupby(level=0).gas.max()

        return peak30[["peak30_date", "peak30_oil", "peak30_gas", "peak30_month"]]


if __name__ == "__main__":

    import loggers
    import random

    # import util
    from timeit import default_timer as timer

    loggers.config(level=20)

    async def async_wrapper():
        # id = ["14207C017575", "14207C020251"]
        # # id = ["14207C017575"]
        # id = [
        #     "14207C0155111H",
        #     "14207C0155258418H",
        #     "14207C0155258421H",
        #     "14207C01552617H",
        #     "14207C015535211H",
        #     "14207C015535212H",
        #     "14207C0155368001H",
        #     "14207C0155368002H",
        #     "14207C01558022H",
        #     "14207C0155809H",
        #     "14207C017575",
        #     "14207C020251",
        # ]

        ts = timer()
        ids = await IHSClient.get_ids("tx-upton", path=IHSPath.prod_h_ids)

        ids = random.choices(ids, k=5)

        df = await pd.DataFrame.prodstats.from_ihs(entities=ids, path=IHSPath.prod_h)

        ps = df.prodstats.calc()

        from db import db
        from db.models import ProdHeader, ProdMonthly, ProdStat

        await db.startup()
        await ProdHeader.bulk_upsert(ps.header, batch_size=100)
        await ProdMonthly.bulk_upsert(ps.monthly, batch_size=100)
        await ProdStat.bulk_upsert(ps.stats, reset_index=False, batch_size=1000)
        exc_time = round(timer() - ts, 2)
        print(f"exc_time: {exc_time}")
        # prodstats = prodstats.replace({np.nan: None})

        # aggregate prodstats

        # header.iloc[10]
        # header.head(25)
        # header.shape

        # monthly.iloc[10]
        # monthly.head(25)
        # monthly.shape

        # monthly.join()

        # dir(monthly.groupby(level=[0, 1]))
