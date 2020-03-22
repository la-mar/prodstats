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


@pytest.fixture
def prodstat_opts():
    norm_opts = [(None, None), (1000, "1k")]
    yield pd.DataFrame.prodstats.generate_option_sets(
        months=[1, 6],
        norm_values=norm_opts,
        range_names=[ProdStatRange.PEAKNORM, ProdStatRange.ALL],
        agg_types=["sum"],
        include_zeroes=[True],
    )


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

    async def test_process(self, prod_df, prodstat_opts):
        pexec = ex.ProdExecutor()

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

    async def test_persist(self, prod_df, bind, prodstat_opts):
        pexec = ex.ProdExecutor()

        ps = await pexec.process(
            prod_df,
            monthly_prod_columns=["oil"],
            prodstat_columns=["oil"],
            norm_option_sets=[(1000, "1k")],
            prodstat_option_sets=prodstat_opts,
        )
        kwargs = {
            "header_kwargs": {"batch_size": 1000},
            "monthly_kwargs": {"batch_size": 1000},
            "stats_kwargs": {"batch_size": 1000},
        }
        actual = await pexec.persist(ps, **kwargs)
        expected = ps.header.shape[0] + ps.monthly.shape[0] + ps.stats.shape[0]
        assert expected == actual

    @pytest.mark.parametrize("idname", ["api10s", "entities", "entity12s"])
    async def test_run(self, well_dispatcher, bind, idname, prodstat_opts):
        kwargs = {idname: ["a", "b", "c"]}
        download_kwargs = {"dispatch": well_dispatcher}
        process_kwargs = {
            "monthly_prod_columns": ["oil"],
            "prodstat_columns": ["oil"],
            "norm_option_sets": [(1000, "1k")],
            "prodstat_option_sets": prodstat_opts,
        }
        pexec = ex.ProdExecutor(
            download_kwargs=download_kwargs, process_kwargs=process_kwargs
        )
        await pexec.run(**kwargs)

    async def test_run_too_many_args(self):
        pexec = ex.ProdExecutor()
        with pytest.raises(ValueError):
            await pexec.run(api10s=["a"], entities=["b"])

    async def test_run_too_few_args(self):
        pexec = ex.ProdExecutor()
        with pytest.raises(ValueError):
            await pexec.run()

    async def test_run_catch_emtpy_list_args(self):
        pexec = ex.ProdExecutor()
        with pytest.raises(ValueError):
            await pexec.run(entities=[])

    async def test_run_sync(self, well_dispatcher, bind, prodstat_opts):
        kwargs = {"entities": ["a", "b", "c"]}
        download_kwargs = {"dispatch": well_dispatcher}
        process_kwargs = {
            "monthly_prod_columns": ["oil"],
            "prodstat_columns": ["oil"],
            "norm_option_sets": [(1000, "1k")],
            "prodstat_option_sets": prodstat_opts,
        }
        pexec = ex.ProdExecutor(
            download_kwargs=download_kwargs, process_kwargs=process_kwargs
        )
        with pytest.raises(Exception):
            pexec.run_sync(**kwargs)
