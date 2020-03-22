import logging

import pandas as pd
import pytest

import calc.prod  # noqa
import executors as ex
from const import ProdStatRange
from tests.utils import MockAsyncDispatch

logger = logging.getLogger(__name__)


@pytest.fixture
def well_dispatcher(ihs_prod):
    yield MockAsyncDispatch({"data": ihs_prod})


class TestBaseExecutor:
    def test_executor_base(self):
        bexec = ex.BaseExecutor()
        assert bexec.metrics.empty is True
        assert {*bexec.metrics.columns} == {"operation", "name", "time", "count"}

    def test_add_metric(self):
        bexec = ex.BaseExecutor()
        assert bexec.metrics.empty is True
        bexec.add_metric(
            operation="test_operation", name="test_name", time=30, count=10
        )
        assert bexec.metrics.empty is False

        expected = {
            "operation": "test_operation",
            "name": "test_name",
            "time": 30,
            "count": 10,
        }
        actual = bexec.metrics.iloc[0].to_dict()
        assert expected == actual


@pytest.mark.asyncio
class TestProdExecutor:
    async def test_download(self, well_dispatcher):
        pexec = ex.ProdExecutor()
        df = await pexec.download(entities=["a", "b", "c"], dispatch=well_dispatcher)
        assert isinstance(df, pd.DataFrame)
        assert pexec.metrics.shape[0] == 1

    async def test_download_bad_result(self, well_dispatcher):
        pexec = ex.ProdExecutor()
        with pytest.raises(Exception):
            await pexec.download(entities=["a", "b", "c"])

    async def test_process(self, prod_df):
        pexec = ex.ProdExecutor()
        norm_opts = [(None, None), (1000, "1k")]
        prodstat_opts = pd.DataFrame.prodstats.generate_option_sets(
            months=[1, 6],
            norm_values=norm_opts,
            range_names=[ProdStatRange.PEAKNORM, ProdStatRange.ALL],
            agg_types=["sum"],
            include_zeroes=[True],
        )

        ps = await pexec.process(
            prod_df,
            monthly_prod_columns=["oil"],
            prodstat_columns=["oil"],
            norm_option_sets=[(1000, "1k")],
            prodstat_option_sets=prodstat_opts,
        )
        assert isinstance(ps.header, pd.DataFrame)
        assert isinstance(ps.monthly, pd.DataFrame)
        assert isinstance(ps.stats, pd.DataFrame)
        assert pexec.metrics.shape[0] == 3

    async def test_process_catch_bad_args(self, prod_df):
        pexec = ex.ProdExecutor()

        with pytest.raises(Exception):
            await pexec.process(
                prod_df,
                monthly_prod_columns=["oil"],
                prodstat_columns=["oil"],
                norm_option_sets=[(1000, "1k")],
                prodstat_option_sets=[None],
            )

    async def test_persist(self, prod_df, bind):
        pexec = ex.ProdExecutor()
        norm_opts = [(None, None), (1000, "1k")]
        prodstat_opts = pd.DataFrame.prodstats.generate_option_sets(
            months=[1, 6],
            norm_values=norm_opts,
            range_names=[ProdStatRange.PEAKNORM, ProdStatRange.ALL],
            agg_types=["sum"],
            include_zeroes=[True],
        )

        ps = await pexec.process(
            prod_df,
            monthly_prod_columns=["oil"],
            prodstat_columns=["oil"],
            norm_option_sets=[(1000, "1k")],
            prodstat_option_sets=prodstat_opts,
        )

        actual = await pexec.persist(ps)
        expected = ps.header.shape[0] + ps.monthly.shape[0] + ps.stats.shape[0]
        assert expected == actual

    @pytest.mark.parametrize("idname", ["api10s", "entities", "entity12s"])
    async def run(self, well_dispatcher, bind, idname):
        pexec = ex.ProdExecutor()
        kwargs = {idname: ["a", "b", "c"]}
        await pexec.run(**kwargs)
