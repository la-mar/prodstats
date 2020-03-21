import logging

import numpy as np
import pandas as pd
import pytest

from calc.prod import ProdStatRange, _validate_required_columns
from const import IHSPath
from tests.utils import MockAsyncDispatch

logger = logging.getLogger(__name__)


@pytest.fixture
def prod_dispatcher(ihs_producing_wells):
    yield MockAsyncDispatch({"data": ihs_producing_wells})


def test_validate_required_columns_raise():
    with pytest.raises(KeyError):
        _validate_required_columns(required=["a", "b"], columns=["a", "c", "d"])


class TestProdStats:
    def test_instantiate_df_ext(self):
        pd.DataFrame.prodstats

    @pytest.mark.asyncio
    async def test_from_ihs_reshape_prod_records(self, prod_dispatcher):
        df = await pd.DataFrame.prodstats.from_ihs(
            api10s=["a"], path=IHSPath.prod_h, dispatch=prod_dispatcher
        )
        assert df.index.names == ["api10", "prod_date"]
        assert "oil" in df.columns
        assert "gas" in df.columns
        assert "water" in df.columns

    @pytest.mark.parametrize(
        "kwargs,expected",
        [
            (
                {
                    "columns": ["oil", "gas", "water"],
                    "agg_type": "sum",
                    "include_zeroes": True,
                    "range_name": ProdStatRange.FIRST,
                    "months": 6,
                    "norm_by_label": None,
                },
                {
                    "oil": "oil_sum_first6mo",
                    "gas": "gas_sum_first6mo",
                    "water": "water_sum_first6mo",
                },
            ),
            (
                {
                    "columns": ["oil", "gas", "water"],
                    "agg_type": "sum",
                    "include_zeroes": False,
                    "range_name": ProdStatRange.LAST,
                    "months": 3,
                    "norm_by_label": None,
                },
                {
                    "oil": "oil_sum_last3mo_nonzero",
                    "gas": "gas_sum_last3mo_nonzero",
                    "water": "water_sum_last3mo_nonzero",
                },
            ),
            (
                {
                    "columns": ["oil", "gas", "water"],
                    "agg_type": "avg",
                    "include_zeroes": True,
                    "range_name": ProdStatRange.ALL,
                    "months": None,
                    "norm_by_label": None,
                },
                {"oil": "oil_avg", "gas": "gas_avg", "water": "water_avg"},
            ),
            (
                {
                    "columns": ["oil", "gas", "water"],
                    "agg_type": "sum",
                    "include_zeroes": True,
                    "range_name": ProdStatRange.PEAKNORM,
                    "months": 6,
                    "norm_by_label": "1k",
                },
                {
                    "oil": "oil_sum_peaknorm6mo_per1k",
                    "gas": "gas_sum_peaknorm6mo_per1k",
                    "water": "water_sum_peaknorm6mo_per1k",
                },
            ),
        ],
    )
    def test_make_aliases(self, kwargs, expected):
        actual = pd.DataFrame.prodstats.make_aliases(**kwargs)
        assert expected == actual

    def test_prod_bounds_by_well(self):
        data = [
            {"api10": 1234567890, "prod_date": "2019-01-01", "prod_month": 1},
            {"api10": 1234567890, "prod_date": "2019-02-01", "prod_month": 2},
            {"api10": 1234567890, "prod_date": "2019-03-01", "prod_month": 3},
            {"api10": 9999999999, "prod_date": "2019-01-01", "prod_month": 1},
            {"api10": 9999999999, "prod_date": "2019-02-01", "prod_month": 2},
            {"api10": 9999999999, "prod_date": "2019-03-01", "prod_month": 3},
            {"api10": 9999999999, "prod_date": "2019-04-01", "prod_month": 4},
        ]

        df = pd.DataFrame(data).set_index(["api10", "prod_date"])
        result = df.prodstats.prod_bounds_by_well()

        for api10 in list(df.index.levels[0]):
            assert result.loc[api10].start_month == df.loc[api10].prod_month.min()
            assert result.loc[api10].end_month == df.loc[api10].prod_month.max()
            assert result.loc[api10].start_date == df.loc[api10].index[0]
            assert result.loc[api10].end_date == df.loc[api10].index[-1]

    @pytest.mark.parametrize(
        "range,months,result_min_month,result_max_month",
        [
            (ProdStatRange.FIRST, 1, 1, 1),
            (ProdStatRange.LAST, 1, 14, 14),
            (ProdStatRange.PEAKNORM, 1, 3, 3),
            (ProdStatRange.FIRST, 3, 1, 3),
            (ProdStatRange.LAST, 3, 12, 14),
            (ProdStatRange.PEAKNORM, 3, 3, 5),
            (ProdStatRange.FIRST, 6, 1, 6),
            (ProdStatRange.LAST, 6, 9, 14),
            (ProdStatRange.PEAKNORM, 6, 3, 8),
            (ProdStatRange.ALL, None, 1, 14),
        ],
    )
    def test_get_monthly_range(
        self, prod_df, range, months, result_min_month, result_max_month
    ):

        prod_df["prod_month"] = prod_df.groupby(level=0).cumcount() + 1
        peak30 = prod_df.prodstats.peak30()
        prod_df["peak_norm_month"] = prod_df.prod_month - peak30.peak30_month
        df = prod_df.prodstats.monthly_by_range(range, months=months)

        ranges = (
            df.reset_index(level=1)
            .groupby(level=0)
            .prod_month.describe()[["min", "max"]]
            .astype(int)
        )

        assert ranges["min"].min() == ranges["min"].max() == result_min_month
        assert ranges["max"].min() == ranges["max"].max() == result_max_month

    def test_get_monthly_range_catch_range_name_without_months(self, prod_df):
        prod_df["prod_month"] = prod_df.groupby(level=0).cumcount() + 1
        peak30 = prod_df.prodstats.peak30()
        prod_df["peak_norm_month"] = prod_df.prod_month - peak30.peak30_month

        months = None
        for range in ProdStatRange.members():
            if range != ProdStatRange.ALL:
                with pytest.raises(ValueError):
                    prod_df.prodstats.monthly_by_range(range, months=months)
            else:
                prod_df.prodstats.monthly_by_range(range, months=months)

    def test_get_monthly_range_catch_range_name_with_months(self, prod_df):
        prod_df["prod_month"] = prod_df.groupby(level=0).cumcount() + 1
        peak30 = prod_df.prodstats.peak30()
        prod_df["peak_norm_month"] = prod_df.prod_month - peak30.peak30_month

        months = 6
        for range in ProdStatRange.members():
            if range != ProdStatRange.ALL:
                prod_df.prodstats.monthly_by_range(range, months=months)
            else:
                with pytest.raises(ValueError):
                    prod_df.prodstats.monthly_by_range(range, months=months)

    def test_melt(self, prod_df):
        df = prod_df[["oil", "gas"]].groupby(level=0).max()
        melted = df.prodstats.melt(prodstat_names=["oil", "gas"])

        assert {*df.index} == {*melted.api10}
        assert {*df.index} == {*melted.api10}
        assert {*df.columns} == {*melted.name}

    # @pytest.mark.parametrize("include_zeroes", [True, False])
    def test_aggregate_range(self, prod_df):
        prod_df["prod_month"] = prod_df.groupby(level=0).cumcount() + 1
        result = prod_df.prodstats.aggregate_range(
            range_name=ProdStatRange.FIRST,
            agg_map={"oil": "sum", "gas": "sum"},
            alias_map={"oil": "oil_alias", "gas": "gas_alias"},
            include_zeroes=True,
            months=6,
        )
        assert result.start_month.min() == result.start_month.max() == 1
        assert result.end_month.min() == result.end_month.max() == 6

    def test_aggregate_range_nonzero(self, prod_df):
        prod_df = prod_df[["oil", "gas"]].copy(deep=True)
        prod_df["prod_month"] = prod_df.groupby(level=0).cumcount() + 1
        idx = prod_df.xs("2018-12-01", level=1, drop_level=False).index
        prod_df.loc[idx, ["oil", "gas"]] = 0
        result = prod_df.prodstats.aggregate_range(
            range_name=ProdStatRange.FIRST,
            agg_map={"oil": "sum", "gas": "sum"},
            alias_map={"oil": "oil_alias", "gas": "gas_alias"},
            include_zeroes=False,
            months=6,
        )
        assert result.start_month.min() == result.start_month.max() == 1
        assert result.end_month.min() == result.end_month.max() == 7

    def test_aggregate_range_catch_unsorted(self, prod_df):
        prod_df = prod_df.sort_values("oil")
        with pytest.raises(ValueError):
            prod_df.prodstats.aggregate_range(
                range_name=ProdStatRange.FIRST,
                agg_map={"oil": "sum", "gas": "sum"},
                alias_map={"oil": "oil_alias", "gas": "gas_alias"},
                include_zeroes=True,
                months=6,
            )

    def test_inverval_calc(self, prod_df):
        prod_df["prod_month"] = prod_df.groupby(level=0).cumcount() + 1

        result = prod_df.prodstats.interval(
            columns=["oil", "gas"],
            range_name=ProdStatRange.FIRST,
            months=6,
            agg_type="sum",
            include_zeroes=True,
            # norm_by_ll=1000,
            # norm_by_label="1k",
        )
        assert result.start_month.min() == result.start_month.max() == 1
        assert result.end_month.min() == result.end_month.max() == 6

    def test_inverval_calc_with_norm(self, prod_df):
        prod_df["prod_month"] = prod_df.groupby(level=0).cumcount() + 1
        prod_df["boe"] = prod_df.prodstats.boe()

        result = prod_df.prodstats.interval(
            columns=["oil", "gas"],
            range_name=ProdStatRange.FIRST,
            months=6,
            agg_type="sum",
            include_zeroes=True,
            norm_by_ll=1000,
            norm_by_label="1k",
        )

        assert result.start_month.min() == result.start_month.max() == 1
        assert result.end_month.min() == result.end_month.max() == 6

    def test_interval_calc_catch_str_range_type(self, prod_df):

        with pytest.raises(TypeError):
            prod_df.prodstats.interval(
                columns=["oil", "gas"],
                range_name="first",
                months=6,
                agg_type="sum",
                include_zeroes=True,
            )

    def test_interval_calc_catch_bad_norm_combo(self, prod_df):
        prod_df["prod_month"] = prod_df.groupby(level=0).cumcount() + 1

        with pytest.raises(TypeError):
            prod_df.prodstats.interval(
                columns=["oil", "gas"],
                range_name="first",
                months=6,
                agg_type="sum",
                include_zeroes=True,
                norm_by_ll=1000,
            )

        with pytest.raises(ValueError):
            prod_df.prodstats.interval(
                columns=["oil", "gas"],
                range_name=ProdStatRange.FIRST,
                months=6,
                agg_type="sum",
                include_zeroes=True,
                norm_by_label="1k",
            )

    def test_inverval_calc_mean(self, prod_df):
        range_name = ProdStatRange.FIRST
        months = 6
        prod_df["prod_month"] = prod_df.groupby(level=0).cumcount() + 1

        actual = prod_df.prodstats.interval(
            columns=["oil", "gas"],
            range_name=range_name,
            months=months,
            agg_type="mean",
            include_zeroes=True,
        ).value

        expected = (
            prod_df.loc[:, ["oil", "gas"]]
            .groupby(level=0)
            .head(months)
            .groupby(level=0)
            .mean()
            .reset_index()
            .melt(id_vars=["api10"])
            .set_index("api10")
        ).value

        assert {*actual.values} == {*expected.values}

    def test_generate_option_sets(self):  # TODO: better assertions
        opts = pd.DataFrame.prodstats.generate_option_sets(
            months=[1, 6],
            norm_values=[(None, None), (1000, "1k")],
            range_names=[ProdStatRange.PEAKNORM, ProdStatRange.ALL],
            agg_types=["sum"],
            include_zeroes=[True],
        )
        assert len(opts) == 6

    def test_peak30(self, prod_df):
        prod_df["prod_month"] = prod_df.groupby(level=0).cumcount() + 1

        peak30 = prod_df.prodstats.peak30()

        assert peak30.columns.tolist() == [
            "peak30_date",
            "peak30_oil",
            "peak30_gas",
            "peak30_month",
        ]
        assert peak30.iloc[0].peak30_date == pd.Timestamp("2018-11-01")
        assert peak30.iloc[0].peak30_oil == 27727
        assert peak30.iloc[0].peak30_gas == 26699
        assert peak30.iloc[0].peak30_month == 2

    def test_stats(self, prod_df):
        months = [1, 6]
        norm_values = [(None, None), (1000, "1k")]
        range_names = [
            ProdStatRange.PEAKNORM,
        ]
        agg_types = ["sum", "mean"]
        include_zeroes = [True]
        columns = ["oil", "gas"]

        opts = pd.DataFrame.prodstats.generate_option_sets(
            months=months,
            norm_values=norm_values,
            range_names=range_names,
            agg_types=agg_types,
            include_zeroes=include_zeroes,
        )
        prod_df["prod_month"] = prod_df.groupby(level=0).cumcount() + 1
        peak30 = prod_df.prodstats.peak30()
        prod_df["peak_norm_month"] = prod_df.prod_month - peak30.peak30_month + 1

        stats = prod_df.prodstats.stats(option_sets=opts, columns=columns)
        assert stats.shape[0] == len(opts) * 6

        unique_name_count = len({*stats.index.levels[1]})
        expected_unique_name_count = (
            len(columns)
            * len(months)
            * len(agg_types)
            * len(range_names)
            * len(norm_values)
            * len(include_zeroes)
        )

        assert unique_name_count == expected_unique_name_count

    def test_preprocess(self, prod_df):
        prodset = prod_df.prodstats.preprocess()

        assert {*prodset.header.columns} == {
            "entity12",
            "perf_lower",
            "perf_upper",
            "perfll",
            "primary_api14",
            "products",
            "provider",
            "provider_last_update_at",
            "related_wells",
            "status",
        }

        assert {*prodset.monthly.columns} == {
            "boe",
            "days_in_month",
            "gas",
            "gor",
            "oil",
            "oil_percent",
            "perfll",
            "prod_days",
            "prod_month",
            "water",
            "water_cut",
        }

        assert prodset.stats is None

    def test_norm_to_ll(self, prod_df):
        prod_df["prod_month"] = prod_df.groupby(level=0).cumcount() + 1
        df = prod_df.prodstats.norm_to_ll(value=1000, suffix="1k", columns=["oil"])

        assert df.index.names == ["api10", "prod_date"]
        assert df.columns.tolist() == ["oil_norm_1k"]
        assert prod_df.shape[0] == df.shape[0]
        expected = prod_df.oil.div(prod_df.perfll / 1000).groupby(level=0).sum()
        actual = df.groupby(level=0).sum()
        merged = expected.to_frame("original").join(actual)
        assert merged.original.sub(merged.oil_norm_1k).sum() == 0

    def test_norm_to_ll_ignore_missing_prod_columns(self, prod_df):
        prod_df["prod_month"] = prod_df.groupby(level=0).cumcount() + 1
        df = prod_df.prodstats.norm_to_ll(value=1000, suffix="1k", columns=["oil"])

        assert df.index.names == ["api10", "prod_date"]
        assert df.columns.tolist() == ["oil_norm_1k"]
        assert prod_df.shape[0] == df.shape[0]

    def test_norm_to_ll_catch_missing_prod_month(self, prod_df):
        with pytest.raises(KeyError):
            prod_df.prodstats.norm_to_ll(value=1000, suffix="1k", columns=["oil"])

    def test_normalize_monthly_production(self, prod_df):
        prod_df["prod_month"] = prod_df.groupby(level=0).cumcount() + 1

        norm_values = [(None, None), (1000, "1k")]
        df = prod_df.prodstats.normalize_monthly_production(
            norm_values, columns=["oil", "gas"]
        )

        for x in ["oil_norm_1k", "gas_norm_1k"]:
            assert x in df.columns

    def test_daily_avg_by_month(self, prod_df):
        in_df = prod_df.loc[:, ["oil", "gas", "days_in_month"]]
        df = in_df.prodstats.daily_avg(
            columns=["oil", "gas"], days_column="days_in_month"
        )

        for x in ["oil_avg_daily", "gas_avg_daily"]:
            assert x in df.columns

        assert all(in_df.oil.div(in_df.days_in_month).values == df.oil_avg_daily.values)
        assert all(in_df.gas.div(in_df.days_in_month).values == df.gas_avg_daily.values)

    def test_daily_avg_by_well(self, prod_df):
        in_df = prod_df[["oil", "gas", "days_in_month"]].groupby(level=0).sum()
        df = in_df.prodstats.daily_avg(
            columns=["oil", "gas"], days_column="days_in_month"
        )

        for x in ["oil_avg_daily", "gas_avg_daily"]:
            assert x in df.columns

        assert all(in_df.oil.div(in_df.days_in_month).values == df.oil_avg_daily.values)
        assert all(in_df.gas.div(in_df.days_in_month).values == df.gas_avg_daily.values)

    def test_pdp(self, prod_df):
        prod_df["boe"] = prod_df.prodstats.boe()
        pdp = prod_df.prodstats.pdp(
            range_name=ProdStatRange.LAST, months=3, dollars_per_bbl=15, factor=0.6
        )
        series = pdp.iloc[0]
        assert series.oil_pdp_last3mo_per15bbl == 3125
        assert series.boe_pdp_last3mo_per15bbl == 4527

    def test_pdp_handle_range_of_nan_values(self, prod_df):
        prod_df = prod_df.loc[:, ["oil", "gas", "days_in_month"]]
        prod_df["boe"] = prod_df.prodstats.boe()
        prod_df.loc[
            prod_df.groupby(level=0).tail(12).index, ["oil", "boe", "gas"]
        ] = np.nan
        pdp = prod_df.prodstats.pdp(
            range_name=ProdStatRange.LAST, months=3, dollars_per_bbl=15, factor=0.6
        )

        assert pdp.shape == (0, 2)

    def test_pdp_fitler_zero_prod_months(self, prod_df):
        prod_df = prod_df.loc[:, ["oil", "gas", "days_in_month"]]
        prod_df["boe"] = prod_df.prodstats.boe()
        prod_df.loc[prod_df.groupby(level=0).tail(2).index, ["oil", "boe", "gas"]] = 0
        pdp = prod_df.prodstats.pdp(
            range_name=ProdStatRange.LAST, months=3, dollars_per_bbl=15, factor=0.6
        )
        expected = (
            prod_df.prodstats.daily_avg(["oil", "boe"], "days_in_month")
            .mul(15)
            .mul(0.6)
            .rename(columns={"oil_avg_daily": "oil_pdp", "boe_avg_daily": "boe_pdp"})
        )
        expected = (
            expected[expected.oil > 0]
            .groupby(level=0)
            .tail(1)
            .loc[:, ["oil_pdp", "boe_pdp"]]
            .astype(int)
        )

        assert np.array_equal(pdp.values, expected.values)

    def test_calc(self, prod_df):
        norm_opts = [(None, None), (1000, "1k")]
        prodstat_opts = pd.DataFrame.prodstats.generate_option_sets(
            months=[1, 6],
            norm_values=norm_opts,
            range_names=[ProdStatRange.PEAKNORM, ProdStatRange.ALL],
            agg_types=["sum"],
            include_zeroes=[True],
        )

        df = prod_df.xs("4246140916", drop_level=False).copy(deep=True)

        prodset = df.prodstats.calc(
            monthly_prod_columns=["oil", "gas"],
            prodstat_columns=["oil", "gas"],
            norm_option_sets=norm_opts,
            prodstat_option_sets=prodstat_opts,
        )
        # prod_df.prodstats.calc()
        # prod_df.columns.tolist()
        assert prodset.header.shape[0] == 1
        assert prodset.monthly.shape[0] == 14
        assert prodset.stats.shape[0] == 12


# if __name__ == "__main__":
#     from util.jsontools import load_json

#     @pytest.fixture
#     def prod_df(ihs_producing_wells):
#         yield ProductionWellSet(wells=ihs_producing_wells).df()

#     ihs_producing_wells = load_json(f"tests/fixtures/ihs_prod.json")
#     prod_df: pd.DataFrame = next(prod_df.__wrapped__(ihs_producing_wells))
