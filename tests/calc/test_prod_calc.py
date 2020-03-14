import logging

import pandas as pd
import pytest
from tests.utils import MockAsyncDispatch

from calc.prod import ProdSet, ProdStatRange, _validate_required_columns
from collector import IHSPath
from schemas import ProductionWellSet

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def ihs_producing_wells(json_fixture):
    yield json_fixture("ihs_prod.json")


# @pytest.fixture
# def proddf() -> pd.DataFrame:

#     # idx = pd.date_range(start=datetime.now(), periods=12)
#     # pd.DataFrame(index=idx)

#     # from util.jsontools import to_json
#     # to_json(p.records(), "tests/data/prod.json")

#     df = pd.read_json("tests/data/prod.json")
#     df.prod_date = pd.to_datetime(df.prod_date)
#     yield df.set_index(["api10", "prod_date"])


# @pytest.fixture
# def prodrecords(proddf):
#     yield orjson.loads(proddf.reset_index().to_json(orient="records"))


@pytest.fixture
def prod_dispatcher(ihs_producing_wells):
    yield MockAsyncDispatch({"data": ihs_producing_wells})


@pytest.fixture
def prod_df(ihs_producing_wells):
    yield ProductionWellSet(wells=ihs_producing_wells).df()


def test_validate_required_columns_raise():
    with pytest.raises(KeyError):
        _validate_required_columns(required=["a", "b"], columns=["a", "c", "d"])


class TestProdSet:
    def test_describe(self):
        ps = ProdSet(
            *[pd.DataFrame([*[{x: x} for x in range(0, x)]]) for x in range(1, 4)]
        )

        assert repr(ps) == "header=1 monthly=2 stats=3"
        assert ps.describe() == {
            "header": 1,
            "monthly": 2,
            "stats": 3,
        }

    def test_describe_handle_none(self):
        records = [{"key": 1}, {"key": 1}]
        ps = ProdSet(monthly=pd.DataFrame(records))
        assert ps.describe() == {"header": 0, "monthly": 2, "stats": 0}

    def test_iter(self):
        df = pd.DataFrame([{"key": 1}, {"key": 1}])
        ps = ProdSet(header=df, monthly=df, stats=df)
        assert list(ps) == [df] * 3


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
        prod_df = prod_df[["oil", "gas"]]
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

    def test_generate_option_sets(self):  # TODO: better assertions
        opts = pd.DataFrame.prodstats.generate_option_sets(
            months=[1, 6],
            norm_values=[(None, None), (1000, "1k")],
            range_names=[ProdStatRange.PEAKNORM, ProdStatRange.ALL],
            agg_types=["sum"],
            include_zeroes=[True],
        )
        assert len(opts) == 14

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


# if __name__ == "__main__":
#     from util.jsontools import load_json

#     @pytest.fixture
#     def prod_df(ihs_producing_wells):
#         yield ProductionWellSet(wells=ihs_producing_wells).df()

#     ihs_producing_wells = load_json(f"tests/fixtures/ihs_prod.json")
#     prod_df: pd.DataFrame = next(prod_df.__wrapped__(ihs_producing_wells))
#     monthly["prod_month"] = prod_df.groupby(level=0).cumcount() + 1
#     peak30 = monthly.prodstats.peak30()
#     monthly["peak_norm_month"] = monthly.prod_month - peak30.peak30_month + 1
#     columns = ["oil", "gas"]
#     prod_df.prodstats.calc()
