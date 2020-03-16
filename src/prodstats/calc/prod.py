import itertools
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

import config as conf
import const
from collector import IHSClient, IHSPath
from schemas import ProductionWellSet
from util import hf_number
from util.enums import Enum

logger = logging.getLogger(__name__)

PandasObject = Union[pd.DataFrame, pd.Series]


class ProdStatRange(str, Enum):
    FIRST = "first"
    LAST = "last"
    PEAKNORM = "peaknorm"
    ALL = "all"


class ProdSet:

    __slots__ = ("header", "monthly", "stats")

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
        s = " ".join([f"{k}={v}" for k, v in self.describe().items()])
        return s

    def describe(self) -> Dict[str, int]:
        result = {}
        for x in self.__slots__:
            value = getattr(self, x)
            if value is not None:
                result[x] = value.shape[0]
            else:
                result[x] = 0

        return result

    def __iter__(self):
        for x in self.__slots__:
            yield getattr(self, x)


CALC_MONTHS: List[Optional[int]] = [1, 3, 6, 12, 18, 24]
CALC_NORM_VALUES = [
    (None, None),
    (1000, "1k"),
    (3000, "3k"),
    (5000, "5k"),
    (7500, "7500"),
    (10000, "10k"),
]
CALC_RANGES = ProdStatRange.members()
CALC_AGG_TYPES = ["sum", "mean"]
CALC_INCLUDE_ZEROES = [True, False]
CALC_PROD_COLUMNS: List[str] = ["oil", "gas", "water", "boe"]


def _validate_required_columns(required: List[str], columns: List[str]):
    missing = []
    for x in required:
        if x not in columns:
            missing.append(x)

    if len(missing) > 0:
        raise KeyError(f"Missing columns: {missing}")


@pd.api.extensions.register_dataframe_accessor("prodstats")
class ProdStats:
    peak_norm_limit: int = conf.PEAK_NORM_LIMIT
    ranges = ProdStatRange

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

    @staticmethod
    def make_aliases(
        columns: List[str],
        agg_type: str,
        include_zeroes: bool,
        range_name: ProdStatRange,
        months: Union[int, str] = None,
        norm_by_label: str = None,
    ) -> Dict[str, str]:
        """ Compose field names for the aggregate outputs of a list of columns

            Example: oil -> oil_sum_peaknorm6mo_per1k
        """
        nonzero = "_nonzero" if not include_zeroes else ""
        norm_by_label = f"_per{norm_by_label}" if norm_by_label else ""
        range_name = (
            range_name.value if range_name != ProdStatRange.ALL else ""  # type:ignore
        )
        range_label = f"_{range_name}{months}mo" if months and range_name else ""
        agg_type = agg_type if agg_type != "mean" else "avg"

        return {
            k: f"{k}_{agg_type}{range_label}{norm_by_label}{nonzero}" for k in columns
        }

    def monthly_by_range(self, range_name: ProdStatRange, months: int = None):
        """ Get a named range from the monthly production of each api10 in the given dataframe.
            This method will correctly handle monthly production that has already been filtered
            (e.g. removed records containing zero/na) """

        monthly = self._obj
        _validate_required_columns(["api10", "prod_date"], monthly.index.names)

        if range_name == ProdStatRange.PEAKNORM:
            _validate_required_columns(["peak_norm_month"], monthly.columns)

        if range_name == ProdStatRange.ALL and months is not None:
            raise ValueError("Must not specify months when range_name is set to ALL")
        elif range_name != ProdStatRange.ALL and not months:
            raise ValueError("Must specify months when range_name is not ALL")

        if range_name == ProdStatRange.FIRST:
            df = monthly.groupby(level=0).head(months)
        elif range_name == ProdStatRange.LAST:
            df = monthly.groupby(level=0).tail(months)
        elif range_name == ProdStatRange.PEAKNORM:
            df = (
                monthly.loc[monthly.peak_norm_month > 0, :]
                .groupby(level=0)
                .head(months)
            )
        else:
            df = monthly

        return df

    def prod_bounds_by_well(self):
        monthly = self._obj
        _validate_required_columns(["prod_month"], monthly.columns)
        _validate_required_columns(["api10", "prod_date"], monthly.index.names)

        df = pd.DataFrame(index=monthly.groupby(level=0).first().index)

        df["start_month"] = monthly.groupby(level=0).prod_month.min()
        df["end_month"] = monthly.groupby(level=0).prod_month.max()
        df["start_date"] = monthly.reset_index(level=1).groupby(level=0).prod_date.min()
        df["end_date"] = monthly.reset_index(level=1).groupby(level=0).prod_date.max()

        return df

    def melt(self, prodstat_names: Iterable[str]):

        df = self._obj

        _validate_required_columns(["api10"], df.index.names)

        return df.reset_index().melt(
            id_vars=[c for c in df.columns if c not in prodstat_names] + ["api10"],
            var_name="name",
            value_name="value",
        )

    def aggregate_range(
        self,
        range_name: ProdStatRange,
        agg_map: Dict[str, str],
        alias_map: Dict[str, str],
        include_zeroes: bool,
        months: int = None,
    ):

        monthly = self._obj

        if not monthly.index.is_monotonic_increasing:
            # force sorting prior to function call for performance reasons
            raise ValueError(
                f"DataFrame index is not monotonic. Is the DataFrame's index sorted in ascending order?"  # noqa
            )

        df = monthly

        if include_zeroes:
            # aggregate all target columns
            df = df.prodstats.monthly_by_range(range_name=range_name, months=months)
            aggregated = df.groupby(level=0).agg(agg_map).rename(columns=alias_map)
            aggregated = aggregated.join(df.prodstats.prod_bounds_by_well())

            # melt to EAV=type schema
            aggregated = aggregated.prodstats.melt(prodstat_names=alias_map.values())
            result = aggregated

        else:
            # iterate provided columns and drop 0/na rows on in the context of
            # each column's aggregate
            result = pd.DataFrame()
            for name, agg in agg_map.items():
                df = monthly[monthly[name] > 0]
                df = df.prodstats.monthly_by_range(range_name=range_name, months=months)

                # aggregate the current column
                aggregated = (
                    df[name].to_frame().groupby(level=0).agg({name: agg_map[name]})
                ).rename(columns=alias_map)

                # capture group ranges here since range can change between iterations
                aggregated = aggregated.join(df.prodstats.prod_bounds_by_well())

                # melt to EAV=type schema
                aggregated = aggregated.prodstats.melt(
                    prodstat_names=alias_map.values()
                )

                result = result.append(aggregated)

        return result.set_index(["api10", "name"])

    def interval(
        self,
        # monthly: pd.DataFrame,
        range_name: ProdStatRange,
        columns: List[str],
        months: int = None,
        agg_type: str = "sum",
        include_zeroes: bool = True,
        norm_by_ll: int = None,
        norm_by_label: str = None,
    ) -> pd.DataFrame:

        monthly = self._obj

        if not isinstance(range_name, ProdStatRange):
            raise TypeError(
                f"inverval must be of type ProdStatRange, not {type(range_name)}"
            )

        if (norm_by_ll is not None and not norm_by_label) or (
            norm_by_label is not None and not norm_by_ll
        ):
            raise ValueError(
                "Must pass a value for norm_by_label when norm_by_ll is not None"
            )

        alias_map: Dict[str, str] = monthly.prodstats.make_aliases(
            columns=columns,
            agg_type=agg_type,
            range_name=range_name,
            months=months,
            include_zeroes=include_zeroes,
            norm_by_label=norm_by_label,
        )

        agg_map: Dict[str, str] = {k: agg_type for k in columns}

        aggregated = monthly.prodstats.aggregate_range(
            range_name=range_name,
            months=months,
            agg_map=agg_map,
            alias_map=alias_map,
            include_zeroes=include_zeroes,
        )

        if norm_by_ll is not None:
            # factors = self._obj.perfll / norm_by_ll
            factors = monthly.perfll / norm_by_ll
            aggregated.value = aggregated.value.div(factors, axis=0)

        aggregated["includes_zeroes"] = include_zeroes

        aggregated["ll_norm_value"] = norm_by_ll or np.nan
        aggregated["is_ll_norm"] = pd.notnull(aggregated.ll_norm_value)
        aggregated["is_peak_norm"] = (
            True if range_name == ProdStatRange.PEAKNORM else False
        )
        aggregated["aggregate_type"] = agg_type
        aggregated["property_name"] = (
            aggregated.reset_index(level=1)
            .name.str.split("_")
            .apply(lambda x: x[0] if x else None)
            .values
        )
        aggregated["comments"] = None

        api10 = ""
        if (
            "api10" in {*aggregated.columns, *aggregated.index.names}
            and aggregated.shape[0] > 0
        ):
            api10 = aggregated.reset_index().iloc[0].api10

        logger.debug(
            f"{api10} calculated prodstats: {list(alias_map.values())}",
            extra={"api10": api10, **alias_map},
        )

        return aggregated

    @staticmethod
    def _generate_option_sets(
        months: List[Optional[int]],
        norm_values: List[Tuple[Optional[int], Optional[str]]],
        range_names: List[ProdStatRange],
        agg_types: List[str],
        include_zeroes: List[bool],
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
        norm_values: List[Tuple[Optional[int], Optional[str]]] = CALC_NORM_VALUES,
        range_names: List[ProdStatRange] = CALC_RANGES,
        agg_types: List[str] = CALC_AGG_TYPES,
        include_zeroes: List[bool] = CALC_INCLUDE_ZEROES,
    ) -> List[Dict[str, Any]]:
        bounded_intervals = [x for x in range_names if x != ProdStatRange.ALL]
        unbounded_intervals = [x for x in range_names if x == ProdStatRange.ALL]
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
            range_names=unbounded_intervals,
            agg_types=agg_types,
            include_zeroes=include_zeroes,
        )

        return bounded + unbounded

    def stats(self, option_sets: List[Dict[str, Any]] = None, **kwargs) -> pd.DataFrame:
        """ Calcuate the aggregate production stats defined by the passed option sets.
            If no option sets are passed, self.generate_option_sets() is used to create them
            from the app configuration."""

        monthly = self._obj
        option_sets = option_sets or self.generate_option_sets()

        stats = pd.DataFrame()
        for opts in option_sets:
            stats = stats.append(monthly.prodstats.interval(**kwargs, **opts))
        return stats

    def boe(self) -> pd.Series:
        _validate_required_columns(["oil", "gas"], self._obj.columns)
        return self._obj.oil + (self._obj.gas.div(const.MCF_TO_BBL_FACTOR))

    def preprocess(self) -> ProdSet:

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
        monthly["boe"] = monthly.prodstats.boe()
        monthly["oil_percent"] = monthly.oil.div(monthly.boe).mul(100)

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

        return ProdSet(header=header, monthly=monthly)

    def normalize_monthly_production(
        self, norm_sets: List[Tuple[Optional[int], Optional[str]]] = None, **kwargs
    ):
        monthly = self._obj
        for value, suffix in norm_sets or CALC_NORM_VALUES:
            if value is not None and suffix is not None:
                monthly = monthly.join(
                    monthly.prodstats.norm_to_ll(value=value, suffix=suffix, **kwargs)
                )

        return monthly

    def daily_avg(
        self, columns: List[str], days_column: str,
    ):

        monthly = self._obj
        for col in columns:
            monthly[f"{col}_avg_daily"] = monthly[col].div(monthly[days_column])

        return monthly

    def pdp(
        self,
        range_name: ProdStatRange,
        dollars_per_bbl: int,
        factor: float,
        months: int = None,
    ) -> pd.DataFrame:

        _validate_required_columns(["oil", "boe", "days_in_month"], self._obj.columns)

        monthly = self._obj

        alias_map = self.make_aliases(
            columns=["oil", "boe"],
            agg_type=f"pdp",
            include_zeroes=True,
            range_name=range_name,
            months=months,
            norm_by_label=f"{hf_number(dollars_per_bbl)}bbl",
        )

        monthly_nonzero_oil = (
            monthly.prodstats.monthly_by_range(range_name, months)
            .loc[monthly.oil > 0]
            .loc[:, ["oil", "boe", "days_in_month"]]
        )
        pdp_by_well = (
            monthly_nonzero_oil.groupby(level=0)
            .sum()
            .prodstats.daily_avg(columns=["oil", "boe"], days_column="days_in_month")
            .loc[:, ["oil_avg_daily", "boe_avg_daily"]]
            .rename(
                columns={
                    "oil_avg_daily": alias_map["oil"],
                    "boe_avg_daily": alias_map["boe"],
                }
            )
            .mul(dollars_per_bbl)
            .mul(factor)
        )

        return pdp_by_well.astype(int)

    def calc(
        self,
        monthly_prod_columns: List[str] = ["oil", "gas", "water", "boe"],
        prodstat_columns: List[str] = ["oil", "gas", "water", "boe", "gor"],
        norm_option_sets: List[Tuple[Optional[int], Optional[str]]] = None,
        prodstat_option_sets: List[Dict[str, Any]] = None,
    ) -> ProdSet:
        header, monthly, _ = self.preprocess()

        peak30 = monthly.prodstats.peak30()
        header = header.join(peak30)

        pdp = monthly.prodstats.pdp(
            range_name=ProdStatRange.LAST, months=3, dollars_per_bbl=30000, factor=0.75
        )
        header = header.join(pdp)

        monthly["peak_norm_month"] = monthly.prod_month - header.peak30_month + 1
        monthly["peak_norm_days"] = (
            monthly[monthly.peak_norm_month > 0]
            .groupby(level=[0, 1])
            .sum()
            .groupby(level=[0])
            .days_in_month.cumsum()
        )

        monthly = monthly.prodstats.daily_avg(
            columns=monthly_prod_columns, days_column="days_in_month"
        )

        header["peak_norm_months"] = monthly.groupby(level=0).peak_norm_month.max()
        header["peak_norm_days"] = monthly.groupby(level=0).peak_norm_days.max()

        prod_dates_by_well = (
            monthly.prodstats.prod_bounds_by_well()
            .rename(
                columns={
                    "end_month": "prod_months",
                    "start_date": "first_prod_date",
                    "end_date": "last_prod_date",
                }
            )
            .drop(columns=["start_month"])
        )

        header = header.join(prod_dates_by_well)
        header["prod_days"] = monthly.groupby(level=0).prod_days.max()

        monthly = monthly.prodstats.normalize_monthly_production(
            norm_sets=norm_option_sets, columns=monthly_prod_columns
        )

        stats = monthly.prodstats.stats(
            columns=prodstat_columns, option_sets=prodstat_option_sets
        )

        monthly = monthly.drop(columns=["perfll"])

        return ProdSet(header=header, monthly=monthly, stats=stats)

    def norm_to_ll(self, value: int, suffix: str, columns: List[str],) -> pd.DataFrame:
        """ Normalize to an arbitrary lateral length """

        _validate_required_columns(
            columns + ["prod_month", "perfll"], self._obj.columns
        )
        columns = [x for x in columns if x in self._obj.columns]
        alias_map = {k: f"{k}_norm_{suffix}" for k in columns}
        factors = (self._obj["perfll"] / value).values
        return self._obj.loc[:, columns].div(factors, axis=0).rename(columns=alias_map)

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

    # import random

    import util
    from timeit import default_timer as timer

    from db import db
    from db.models import ProdHeader, ProdMonthly, ProdStat

    # from typing import Coroutine
    import asyncio

    loggers.config(level=20)

    async def async_wrapper():
        # ids = ["14207C017575", "14207C020251"]
        ids = ["14207C0155111H", "14207C0155258418H"]
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

        await db.startup()

        # ids = random.choices(ids, k=100)

        # for chunk in util.chunks(ids, n=100):

        async def process(ids: List[str], return_data: bool = True) -> Dict:
            ts = timer()

            df = await pd.DataFrame.prodstats.from_ihs(
                entities=ids, path=IHSPath.prod_h
            )
            download_time = round(timer() - ts, 2)
            ts = timer()

            ps = df.prodstats.calc()
            process_time = round(timer() - ts, 2)

            ts = timer()

            await ProdHeader.bulk_upsert(ps.header, batch_size=100)
            await ProdMonthly.bulk_upsert(ps.monthly, batch_size=100)
            await ProdStat.bulk_upsert(ps.stats, batch_size=1000)
            persist_time = round(timer() - ts, 2)
            total_time = round(download_time + process_time + persist_time, 2)

            logger.warning(
                f"\n{download_time=:>5}s\n{process_time=:>5}s\n{persist_time=:>5}s\n{total_time=:>5}s\n"  # noqa
            )

            result = {
                "times": {
                    "download": download_time,
                    "process": process_time,
                    "persist": persist_time,
                },
                "counts": ps.describe(),
            }

            if return_data:
                result["data"] = ps

            return result

        async def run(
            batch_size: int,
            return_data: bool,
            area_name: str = None,
            ids: List[str] = None,
        ):

            if area_name and ids:
                raise ValueError(f"Only one of 'area_name' and 'ids' can be specified")
            elif not area_name and not ids:
                raise ValueError(f"One of 'area_name' and 'ids' must be specified")

            if area_name:
                ids = await IHSClient.get_ids(area_name, path=IHSPath.prod_h_ids)

            results: List[Dict] = []
            for chunk in util.chunks(ids, n=batch_size):
                results.append(await process(chunk, return_data))

            return results

        # results = asyncio.run_until_complete
        loop = asyncio.get_event_loop()

        batch_size = 1000
        return_data = True
        area_name = None
        ids = ["14207C0155258418H", "14207C0155258421H"]

        result = loop.run_until_complete(
            run(
                batch_size=batch_size,
                return_data=return_data,
                area_name=area_name,
                ids=ids,
            )
        )
        # result[0]["data"].monthly.head(100)

        def metrics(data: List[Dict[str, Dict]]):
            times = (
                pd.DataFrame([x["times"] for x in result])
                .sum()
                .rename("seconds")
                .to_frame()
            )
            times.index.name = "metric"
            times["minutes"] = times.seconds / 60
            times["time_percent"] = times.seconds / times.seconds.sum() * 100

            c = pd.DataFrame([x["counts"] for x in result]).sum().to_dict()
            counts = pd.DataFrame(index=times.index, data=[c] * 3)

            rates = counts.T / times.seconds
            rates = rates.T
            rates.columns = [f"{x}_per_sec" for x in rates.columns]
            rates = rates.astype(int)

            counts.columns = [f"{x}_count" for x in counts.columns]
            composite = times.join(counts).join(rates).T.astype(int)

            return composite

        metrics(result)

        # async for coro in aiter(ids):
        #     return await coro

        # self = ps.header.prodstats
        # monthly = ps.monthly
        # ps.monthly.groupby(level=0).tail(6).head(100)
        # 4246134936
        # 4246135660
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
