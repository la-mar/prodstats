import logging

import httpx
import orjson
import pandas as pd
import pytest
from tests.utils import MockAsyncDispatch

from calc.prod import ProdSet, ProdStatRange
from collector import IHSPath
from util.jsontools import load_json

logger = logging.getLogger(__name__)


@pytest.fixture
def proddf() -> pd.DataFrame:

    # idx = pd.date_range(start=datetime.now(), periods=12)
    # pd.DataFrame(index=idx)

    # from util.jsontools import to_json
    # to_json(p.records(), "tests/data/prod.json")

    df = pd.read_json("tests/data/prod.json")
    df.prod_date = pd.to_datetime(df.prod_date)
    yield df.set_index(["api10", "prod_date"])


@pytest.fixture
def prodrecords(proddf):
    yield orjson.loads(proddf.reset_index().to_json(orient="records"))


@pytest.fixture(scope="session")
def ihs_prod_records():
    yield load_json("tests/data/prod_from_ihs.json")


base_url = httpx.URL("http://127.0.0.1")


@pytest.fixture
def prod_dispatcher(ihs_prod_records):
    yield MockAsyncDispatch({"data": ihs_prod_records})


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

    def test_get_start_and_end_by_group(self):
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
        result = df.prodstats.get_start_and_end_by_group()

        for api10 in list(df.index.levels[0]):
            assert result.loc[api10].start_month == df.loc[api10].prod_month.min()
            assert result.loc[api10].end_month == df.loc[api10].prod_month.max()
            assert result.loc[api10].start_date == df.loc[api10].index[0]
            assert result.loc[api10].end_date == df.loc[api10].index[-1]


if __name__ == "__main__":
    df: pd.DataFrame = next(proddf.__wrapped__())

    df["prod_month"] = df.groupby(level=0).cumcount() + 1
    df.prodstats.get_start_and_end_by_group()
    df["prod_month"].to_frame().reset_index().head(3).to_json(orient="records")
