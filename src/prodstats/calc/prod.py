import itertools
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

import const
import util
from calc.sets import ProdSet
from collector import IHSClient, IHSPath
from const import ProdStatRange
from schemas import ProductionWellSet
from util.pd import validate_required_columns
from util.types import PandasObject

logger = logging.getLogger(__name__)

CALC_MONTHS: List[Optional[int]] = [1, 3, 6, 12, 18, 24, 30, 36, 42, 48]
CALC_NORM_VALUES = [  # change to List[int] now that the label can be derived
    (None, None),
    (1000, "1k"),
    # (3000, "3k"),
    # (5000, "5k"),
    # (7500, "7500"),
    # (10000, "10k"),
]
MONTHLY_NORM_VALUES = [
    (None, None),
    (1000, "1k"),
    (3000, "3k"),
    (5000, "5k"),
    (7500, "7500"),
    (10000, "10k"),
]


CALC_RANGES = ProdStatRange.members()
# CALC_AGG_TYPES = ["sum", "mean"]
CALC_AGG_TYPES = ["sum"]
CALC_INCLUDE_ZEROES = [True, False]
CALC_PROD_COLUMNS: List[str] = ["oil", "gas", "water", "boe"]


@pd.api.extensions.register_dataframe_accessor("prodstats")
class ProdStats:
    peak_norm_limit: int = const.PEAK_NORM_LIMIT
    ranges = ProdStatRange

    def __init__(self, obj: PandasObject):
        # self._validate(obj)
        self._obj: PandasObject = obj

    @classmethod
    async def from_ihs(
        cls,
        path: IHSPath,
        entities: Union[str, List[str]] = None,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        entity12s: Union[str, List[str]] = None,
        create_index: bool = True,
        **kwargs,
    ) -> ProdSet:
        """Fetch production records from the internal IHS service.

        Arguments:
            path {IHSPath} -- url resource bath (i.e. IHSPath.prod_h)

        Keyword Arguments:
            entities {Union[str, List[str]]} -- can be a single or list of producing entities
            api14s {Union[str, List[str]]} -- can be a single or list of API14 numbers
            api10s {Union[str, List[str]]} -- can be a single or list of API10 numbers
            entity12s {Union[str, List[str]]} -- can be a single or list of 12-digit
                producing entity numbers
            create_index {bool} -- attempt to return the dataframe with the default
                index applied [api10, prod_date] (default: True)

        Returns:
            ProdSet
        """

        data = await IHSClient.get_production(
            entities=entities,
            api14s=api14s,
            api10s=api10s,
            entity12s=entity12s,
            path=path,
            **kwargs,
        )
        df = ProductionWellSet(wells=data).df(create_index=create_index)
        prodset: ProdSet = df.prodstats.preprocess_header_and_monthly_records()
        return prodset

    def preprocess_header_and_monthly_records(self) -> ProdSet:
        """ Split raw monthly production into production headers and monthly records
            and return them as a ProdSet.

        Returns:
            ProdSet
        """
        df = self._obj.sort_values(["api14", "status"], ascending=False)

        # * fore provider date to UTC
        if "provider_last_update_at" in df.columns:
            # force remove tzinfo then localize to UTC
            df["provider_last_update_at"] = (
                df["provider_last_update_at"].dt.tz_localize(None).dt.tz_localize("utc")
            )

        # * group input dataframe by api10 to create header records
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

        # * identify related wells
        related_wells = (
            df.groupby(level=0)
            .first()
            .groupby("entity12")
            .agg({"api14": "unique", "entity": "count"})
        ).rename(columns={"api14": "related_wells", "entity": "related_well_count"})

        related_wells.related_wells = related_wells.related_wells.apply(
            lambda x: x.tolist()
        )

        header = (
            header.reset_index().merge(related_wells, on="entity12").set_index("api10")
        )

        # * monthly calculations
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

        # * ensure deduplication of api10+prod_month and sort
        monthly = monthly.groupby(level=[0, 1]).first().drop(columns=["api14"])
        monthly["prod_month"] = monthly.prodstats.prod_month()
        monthly = monthly.sort_index(ascending=True)

        return ProdSet(header=header, monthly=monthly)

    @staticmethod
    def make_aliases(
        columns: List[str],
        agg_type: str,
        include_zeroes: bool,
        range_name: ProdStatRange,
        months: Union[int, str] = None,
        norm_by_label: str = None,
        **kwargs,
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
        validate_required_columns(["api10", "prod_date"], monthly.index.names)

        if range_name == ProdStatRange.PEAKNORM:
            validate_required_columns(["peak_norm_month"], monthly.columns)

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

    def melt(self, prodstat_names: Iterable[str]):

        df = self._obj

        validate_required_columns(["api10"], df.index.names)

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
    ) -> pd.DataFrame:

        monthly = self._obj

        if not monthly.index.is_monotonic_increasing:
            # force sorting prior to function call for performance reasons
            raise ValueError(
                f"Index is not monotonic. Is the DataFrame's index sorted in ascending order?"  # noqa
            )

        df = monthly
        df.loc[:, list(agg_map.keys())] = df.loc[:, list(agg_map.keys())].astype(float)

        if include_zeroes:

            # aggregate all target columns
            df = df.prodstats.monthly_by_range(
                range_name=range_name, months=months
            ).copy(deep=True)

            aggregated = df.groupby(level=0).agg(agg_map).rename(columns=alias_map)
            aggregated = aggregated.join(df.prodstats._prod_bounds_by_well())

            # melt to EAV type schema
            aggregated = aggregated.prodstats.melt(prodstat_names=alias_map.values())
            result = aggregated

        else:  # aggregate each column separately and rejoin
            result = pd.DataFrame()
            for name, agg in agg_map.items():
                df = monthly[monthly[name] > 0]
                df = df.prodstats.monthly_by_range(range_name=range_name, months=months)

                # aggregate the current column
                aggregated = (
                    df[name].to_frame().groupby(level=0).agg({name: agg_map[name]})
                ).rename(columns=alias_map)

                # capture group ranges here since range can change between iterations
                aggregated = aggregated.join(df.prodstats._prod_bounds_by_well())

                # melt to EAV=type schema
                aggregated = aggregated.prodstats.melt(
                    prodstat_names=alias_map.values()
                )

                result = result.append(aggregated)

        return result.set_index(["api10", "name"])

    def calc_prodstat(
        self,
        range_name: ProdStatRange,
        columns: Union[str, List[str]],
        months: int = None,
        agg_type: str = "sum",
        include_zeroes: bool = True,
        norm_value: int = None,
        norm_suffix: str = None,
    ) -> pd.DataFrame:

        monthly = self._obj

        range_name = ProdStatRange(range_name)
        columns = util.ensure_list(columns)

        if norm_value:
            norm_suffix = norm_suffix or util.humanize.short_number(norm_value).lower()

        alias_map: Dict[str, str] = monthly.prodstats.make_aliases(
            columns=columns,
            agg_type=agg_type,
            range_name=range_name,
            months=months,
            include_zeroes=include_zeroes,
            norm_by_label=norm_suffix,
        )

        agg_map: Dict[str, str] = {k: agg_type for k in columns}

        aggregated = monthly.prodstats.aggregate_range(
            range_name=range_name,
            months=months,
            agg_map=agg_map,
            alias_map=alias_map,
            include_zeroes=include_zeroes,
        )

        if norm_value is not None:
            # not using norm_to_ll func because it wont handle the variant length
            # of the melted DataFrame when normalizing just the value column
            factors = monthly.groupby(level=0).first().perfll / norm_value
            values = aggregated["value"].div(factors, axis=0)
            aggregated["value"] = values

        aggregated["includes_zeroes"] = include_zeroes

        aggregated["ll_norm_value"] = norm_value or np.nan
        aggregated["is_ll_norm"] = pd.notnull(aggregated.ll_norm_value)
        aggregated["is_peak_norm"] = (
            True if range_name == ProdStatRange.PEAKNORM else False
        )
        aggregated["aggregate_type"] = agg_type if agg_type != "mean" else "avg"

        # FIXME: parsing property name yields unexpected behavior when 'name' contains
        #       underscores that should remain in the output property name.
        #       example: name='days_in_month' becomes property_name='days'
        aggregated["property_name"] = (
            aggregated.reset_index(level=1)
            .name.str.split("_")
            .apply(lambda x: x[0] if x else None)
            .values
        )
        aggregated["comments"] = None

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
        logger.debug(f"(Prodstats) generated {len(bounded)} bounded option sets")

        unbounded = cls._generate_option_sets(
            months=[None],
            norm_values=norm_values,
            range_names=unbounded_intervals,
            agg_types=agg_types,
            include_zeroes=include_zeroes,
        )
        logger.debug(f"(Prodstats) generated {len(unbounded)} unbounded option sets")

        return bounded + unbounded

    def boe(self) -> pd.Series:
        validate_required_columns(["oil", "gas"], self._obj.columns)
        return self._obj.oil + (self._obj.gas.div(const.MCF_TO_BBL_FACTOR))

    def oil_percent(self) -> pd.Series:
        return self._obj.oil.div(self.boe()).mul(100)

    def prod_month(self) -> pd.Series:
        validate_required_columns(["api10", "prod_date"], self._obj.index.names)
        return self._obj.groupby(level=0).cumcount() + 1

    def prod_days(self, days_in_month_column: str = "days_in_month") -> pd.Series:
        validate_required_columns(["api10", "prod_date"], self._obj.index.names)
        return (
            self._obj.groupby(level=[0, 1])
            .sum()
            .groupby(level=[0])[days_in_month_column]
            .cumsum()
        )

    def peak_norm_month(self, prod_month_column: str = "prod_month") -> pd.Series:
        validate_required_columns([prod_month_column], self._obj.columns)

        if "peak30_month" in self._obj.columns:
            peak30_month = self._obj.peak30_month
        else:
            peak30_month = self.peak30().peak30_month

        # +1 to start count at 1 instead of 0
        return self._obj.prod_month - peak30_month + 1

    def peak_norm_days(
        self,
        peak_norm_month_column: str = "peak_norm_month",
        days_in_month_column: str = "days_in_month",
    ) -> pd.Series:
        validate_required_columns(
            [peak_norm_month_column, days_in_month_column], self._obj.columns
        )

        if peak_norm_month_column in self._obj.columns:
            peak_norm_month = self._obj[peak_norm_month_column]
        else:
            peak_norm_month = self._obj.peak_norm_month()

        # +1 to start count at 1 instead of 0
        return (
            self._obj.loc[peak_norm_month > 0]
            .groupby(level=[0, 1])
            .sum()
            .groupby(level=[0])[days_in_month_column]
            .cumsum()
        )

    def daily_avg_by_month(
        self, columns: List[str], days_column: str = "days_in_month",
    ) -> pd.DataFrame:
        """ calculate the daily average production by month for each column in
            the input columns list."""

        df = pd.DataFrame()
        for col in columns:
            if col in self._obj.columns:
                df[f"{col}_avg_daily"] = self._obj[col].div(self._obj[days_column])
            else:
                logger.debug(f"daily_avg_by_month: '{col}' not found -- skipping")
        return df

    def pdp_by_well(
        self,
        range_name: ProdStatRange,
        dollars_per_bbl: int,
        factor: float,
        months: int = None,
    ) -> pd.DataFrame:

        validate_required_columns(["oil", "boe", "days_in_month"], self._obj.columns)

        monthly = self._obj

        alias_map = self.make_aliases(
            columns=["oil", "boe"],
            agg_type=f"pdp",
            include_zeroes=True,
            range_name=range_name,
            months=months,
            norm_by_label=f"{util.humanize.short_number(dollars_per_bbl).lower()}bbl",
        )

        monthly_nonzero_oil = (
            monthly.prodstats.monthly_by_range(range_name, months)
            .loc[monthly.oil > 0]
            .loc[:, ["oil", "boe", "days_in_month"]]
        )
        pdp_by_well = (
            monthly_nonzero_oil.groupby(level=0)
            .sum()
            .prodstats.daily_avg_by_month(
                columns=["oil", "boe"], days_column="days_in_month"
            )
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

    def norm_to_ll(
        self,
        norm_value: int,
        lateral_lengths: Union[str, pd.Series] = "perfll",
        suffix: str = None,
    ) -> pd.DataFrame:
        """ Normalize to an arbitrary lateral length """

        if isinstance(lateral_lengths, str):
            values = self._obj[lateral_lengths]
            columns = [x for x in self._obj.columns if x != lateral_lengths]
        else:
            values = lateral_lengths
            columns = self._obj.columns.tolist()

        if not suffix:
            suffix = util.humanize.short_number(norm_value).lower()

        # columns = [x for x in columns if x in self._obj.columns]
        alias_map = {k: f"{k}_per{suffix}" for k in columns}
        factors = values / norm_value
        return self._obj.loc[:, columns].div(factors, axis=0).rename(columns=alias_map)

    def peak30(self) -> pd.DataFrame:
        """ Generate peak30 statistics, bounded by the configured peak_norm_limit """

        validate_required_columns(["oil"], self._obj.columns)

        if "prod_month" not in self._obj.columns:
            prod_months = self.prod_month()
        else:
            prod_months = self._obj.prod_month

        peak_range = prod_months <= self.peak_norm_limit
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

    def _prod_bounds_by_well(self) -> pd.DataFrame:
        monthly = self._obj
        validate_required_columns(["api10", "prod_date"], monthly.index.names)

        df = pd.DataFrame(index=monthly.index.levels[0])
        if "prod_month" not in monthly.columns:
            prod_months = self.prod_month()
        else:
            prod_months = monthly.prod_month

        df["start_month"] = prod_months.groupby(level=0).min()
        df["end_month"] = prod_months.groupby(level=0).max()
        df["start_date"] = monthly.reset_index(level=1).prod_date.groupby(level=0).min()
        df["end_date"] = monthly.reset_index(level=1).prod_date.groupby(level=0).max()

        return df

    def prod_dates_by_well(self) -> pd.DataFrame:
        df = (
            self._prod_bounds_by_well()
            .rename(
                columns={
                    "end_month": "prod_months",
                    "start_date": "first_prod_date",
                    "end_date": "last_prod_date",
                }
            )
            .drop(columns=["start_month"])
        )
        df["peak_norm_months"] = self.peak_norm_month().groupby(level=0).max()
        df["peak_norm_days"] = self.peak_norm_days().groupby(level=0).max()
        df["prod_days"] = self.prod_days().groupby(level=0).max()
        return df

    def ratio_over_interval(
        self,
        range_name: ProdStatRange,
        numerator: str = None,
        denominator: str = None,
        prod_column: str = None,
        months: int = None,
        include_zeroes: bool = True,
        method: str = "ratio_of_averages",
    ) -> pd.DataFrame:
        """

            Ratio of Averages:
                - sum all numbers in each column and divide one into the other
                - more susceptible to outlier influence
                - yields percentage of total
                - aggregate_type = None
                - equation: (sum oil / sum days_in_month) over the given range

            Average of Ratios:
                - average of monthly GORs
                - less susceptible to outlier influence
                - yields average percentage of total
                - aggregate_type = "avg"
                - equation: avg(oil_avg_daily)
        """

        if method not in ["ratio_of_averages", "average_of_ratios"]:
            raise ValueError(
                f"method must be one of [ratio_of_averages, average_of_ratios]"
            )

        if method == "ratio_of_averages":
            # * Ratio of Averages
            if numerator is None or denominator is None:
                raise ValueError(
                    f"Must specify 'numerator' and 'denominator' when using ratio_of_averages method"  # noqa
                )
            validate_required_columns([numerator, denominator], self._obj.columns)
            partials = self.calc_prodstat(
                range_name=range_name,
                columns=[numerator, denominator],
                months=months,
                agg_type="sum",
                include_zeroes=include_zeroes,
            ).reset_index(level=1)

            base = partials.loc[partials.property_name == numerator].copy(deep=True)

            calcs = partials.loc[partials.property_name == numerator, ["value"]].rename(
                columns={"value": numerator}
            )

            calcs[denominator] = partials.loc[
                partials.property_name != numerator, "value"
            ].rename(denominator)

            calcs[prod_column] = calcs[numerator].div(calcs[denominator])

            base.name = base.name.str.replace(
                f"{numerator}|{denominator}", prod_column, regex=True
            ).str.replace("_sum", "")
            base.aggregate_type = None
            base.property_name = prod_column
            base.comments = base.comments.apply(
                lambda x: {"method": "ratio_of_averages"}
            )
            base.value = calcs[prod_column]
            base = base.set_index("name", append=True)
            output = base

        else:
            # * Average of Ratios
            if prod_column is None:
                raise ValueError(
                    f"Must specify 'prod_column' when using average_of_ratios method"
                )
            validate_required_columns([prod_column], self._obj.columns)

            output = self.calc_prodstat(
                range_name=range_name,
                columns=[prod_column],
                months=months,
                agg_type="mean",
                include_zeroes=include_zeroes,
            )
            output.comments = output.comments.apply(
                lambda x: {"method": "average_of_ratios"}
            )

        return output

    def gor_by_well(
        self,
        range_name: ProdStatRange,
        numerator: str = "gas",
        denominator: str = "oil",
        prod_column: str = "gor",
        months: int = None,
        include_zeroes: bool = True,
        method: str = "ratio_of_averages",
    ) -> pd.DataFrame:
        """
            gor: manually (sum gas / sum oil) * 1000 over a given range

        """

        if method not in ["ratio_of_averages", "average_of_ratios"]:
            raise ValueError(
                f"method must be one of [ratio_of_averages, average_of_ratios]"
            )

        gor = self.ratio_over_interval(
            numerator=numerator,
            denominator=denominator,
            prod_column=prod_column,
            range_name=range_name,
            months=months,
            include_zeroes=include_zeroes,
            method=method,
        )

        if method == "ratio_of_averages":
            gor.value = gor.value.mul(1000)

        return gor

    def oil_percent_by_well(
        self,
        range_name: ProdStatRange,
        numerator: str = "oil",
        denominator: str = "boe",
        prod_column: str = "oil_percent",
        months: int = None,
        include_zeroes: bool = True,
        method="average_of_ratios",
    ) -> pd.DataFrame:
        """
        Defaults to method = average_of_ratios. ratio_of_averages
        tends to bust when a production value is sparse
        (i.e. no oil in first few months).
        """

        if method not in ["ratio_of_averages", "average_of_ratios"]:
            raise ValueError(
                f"method must be one of [ratio_of_averages, average_of_ratios]"
            )

        oil_percent = self.ratio_over_interval(
            numerator=numerator,
            denominator=denominator,
            prod_column=prod_column,
            range_name=range_name,
            months=months,
            include_zeroes=include_zeroes,
            method=method,
        )

        if method == "ratio_of_averages":
            oil_percent.value = oil_percent.value.mul(100)

        return oil_percent

    def avg_daily_by_well(
        self,
        range_name: ProdStatRange,
        numerator: str,
        denominator: str = "days_in_month",
        prod_column: str = None,
        months: int = None,
        include_zeroes: bool = True,
        method: str = "ratio_of_averages",
    ) -> pd.DataFrame:
        """ Calculate the average daily production over the given range using the
            specified methodology.

        """

        if method not in ["ratio_of_averages", "average_of_ratios"]:
            raise ValueError(
                f"method must be one of [ratio_of_averages, average_of_ratios]"
            )

        avgdaily = self.ratio_over_interval(
            numerator=numerator,
            denominator=denominator,
            prod_column=prod_column or f"{numerator}_avg_daily",
            range_name=range_name,
            months=months,
            include_zeroes=include_zeroes,
            method=method,
        )

        return avgdaily


if __name__ == "__main__":

    import loggers

    api10s = None
    entities = None
    ids = ["14207C017575", "14207C020251", "14207C0201501H"]
    entity12s = [x[:12] for x in ids]
    path = IHSPath.prod_h
    kwargs: Dict = {}
    kwargs

    loggers.config(level=10)

    coro = pd.DataFrame.prodstats.from_ihs(entity12s=entity12s, path=IHSPath.prod_h)
    prodset: ProdSet = util.aio.async_to_sync(coro)
    header, monthly, prodstats, *other = prodset
    self = monthly.prodstats

    pd.DataFrame(pd.DataFrame.prodstats.generate_option_sets())

    option_set = {
        "months": None,
        "norm_by_ll": 1000,
        "norm_by_label": "1k",
        "range_name": ProdStatRange.ALL,
        "agg_type": "sum",
        "include_zeroes": False,
    }

    monthly.prodstats.calc_prodstat(
        range_name=ProdStatRange.FIRST,
        columns=["oil", "gas"],
        months=6,
        agg_type="sum",
        include_zeroes=True,
        norm_by_ll=1000,
    )

    range_name = ProdStatRange.FIRST
    columns = ["oil", "gas"]
    months = 6
    agg_type = "sum"
    include_zeroes = True
    norm_by_ll = 3000
    norm_by_label = None

    # ! START HERE:

    """
        1. make concrete list of prodstat definitions using generation_option_sets()
        2. add definitions for special cases to thed definitions
        3. create prodstat_definitions db model
        4. seed them in the db
        5. query them out at run time and feed them into calc_prodstat()
    """
    monthly.prodstats.oil_percent_by_well(
        range_name=ProdStatRange.ALL, months=None, include_zeroes=True
    )

    monthly.prodstats.gor_by_well(
        range_name=ProdStatRange.FIRST, months=6, include_zeroes=True
    )

    monthly.prodstats.oil_percent_by_well(
        range_name=ProdStatRange.FIRST, months=6, include_zeroes=True
    )

    monthly.prodstats.ratio_over_interval(
        numerator="oil",
        denominator="days_in_month",
        prod_column="oil_avg_daily",
        range_name=ProdStatRange.FIRST,
        months=6,
        include_zeroes=True,
    )

    mask = monthly.prod_month <= 6
    monthly.loc[mask, ["oil", "days_in_month"]].groupby(level=0).sum()

    monthly.prodstats.ratio_over_interval(
        numerator="gas",
        denominator="oil",
        prod_column="gor",
        range_name=ProdStatRange.FIRST,
        months=6,
        include_zeroes=True,
    ).value.mul(1000)

    test = (
        monthly.prodstats.ratio_over_interval(
            numerator="oil",
            denominator="boe",
            prod_column="oil_percent",
            range_name=ProdStatRange.FIRST,
            months=6,
            include_zeroes=True,
        )
        .reset_index(level=1)
        .loc[:, ["value"]]
        .mul(100)
    )

    test["nonzero"] = (
        monthly.prodstats.ratio_over_interval(
            numerator="oil",
            denominator="boe",
            prod_column="oil_percent",
            range_name=ProdStatRange.FIRST,
            months=6,
            include_zeroes=False,
        )
        .reset_index(level=1)
        .loc[:, ["value"]]
        .mul(100)
    )

    test["aor"] = (
        monthly.prodstats.ratio_over_interval(
            numerator="oil",
            denominator="boe",
            prod_column="oil_percent",
            range_name=ProdStatRange.FIRST,
            months=6,
            include_zeroes=True,
            method="average_of_ratios",
        )
        .reset_index(level=1)
        .loc[:, ["value"]]
    )

    test["aor_nonzero"] = (
        monthly.prodstats.ratio_over_interval(
            numerator="oil",
            denominator="boe",
            prod_column="oil_percent",
            range_name=ProdStatRange.FIRST,
            months=6,
            include_zeroes=False,
            method="average_of_ratios",
        )
        .reset_index(level=1)
        .loc[:, ["value"]]
    )

    test["manual"] = (
        monthly[monthly.prod_month <= 6]
        .oil.div(monthly[monthly.prod_month <= 6].boe)
        .mul(100)
        .groupby(level=0)
        .mean()
    )

    test

    # monthly.loc[monthly.prod_month <= 6, ["oil", "boe"]].groupby(level=0).sum().head()
    # calcs.head()

    # monthly.prodstats.aggregate_range(
    #     range_name,
    #     agg_map={"oil": "sum"},
    #     alias_map={"oil": "oil_sum"},
    #     include_zeroes=True,
    #     months=6,
    # )

    # api10 = "4238337283"
    # monthly.loc[api10].head(6)
    # monthly.prodstats.monthly_by_range(range_name, months=6).loc[api10]

    # monthly.groupby(level=0).head(6).loc[api10]
