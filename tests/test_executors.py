import logging

import pandas as pd
import pytest
from tests.utils import MockAsyncDispatch, rand_str

import calc
import calc.geom  # noqa
import calc.prod  # noqa
import calc.well  # noqa
from calc.sets import DataSet, ProdSet, WellGeometrySet, WellSet  # noqa
from const import HoleDirection, IHSPath, ProdStatRange  # noqa
from db.models import ProdHeader
from db.models import ProdStat as Model
from executors import BaseExecutor, GeomExecutor, ProdExecutor, WellExecutor

logger = logging.getLogger(__name__)


@pytest.fixture
def model_df():
    ids = [(rand_str(length=10), rand_str(length=20)) for i in range(1, 11)]
    good = [{"api10": i, "name": v} for i, v in ids]
    model_df = pd.DataFrame(good)
    yield model_df


@pytest.fixture
def prod_dispatcher(prod_h):
    prod_dispatcher = MockAsyncDispatch({"data": prod_h})
    yield prod_dispatcher


class TestBaseExecutor:
    @pytest.fixture
    def bexec(self):
        yield BaseExecutor(HoleDirection.H)

    def test_executor_init(self):
        bexec = BaseExecutor(HoleDirection.H)
        assert bexec.metrics.empty is True
        assert {*bexec.metrics.columns} == {
            "seconds",
            "executor",
            "hole_direction",
            "operation",
            "name",
            "count",
        }
        assert bexec.download_kwargs == {}
        assert bexec.process_kwargs == {}
        assert bexec.persist_kwargs == {}

    def test_repr(self):
        assert repr(BaseExecutor(HoleDirection.H)) == "BaseExecutor[H]"

    def test_init_validate_hole_dir(self):
        bexec = BaseExecutor("H")
        assert bexec.hole_dir is HoleDirection.H
        with pytest.raises(ValueError):
            BaseExecutor("BAD")

    def test_raise_execution_error(self, bexec, caplog):
        caplog.set_level(10)

        hd = bexec.hole_dir.value
        ct = 10
        name = bexec.__class__.__name__
        with pytest.raises(ValueError):
            bexec.raise_execution_error(
                "test_operation", ct, ValueError("value error message"), {}
            )
        expected = f"({name}[{hd}]) error during test_operationing: {ct} records in batch -- ValueError: value error message"  # noqa
        assert expected in caplog.text

    def test_add_metric(self):
        bexec = BaseExecutor(HoleDirection.H)
        assert bexec.metrics.empty is True
        bexec.add_metric(
            operation="test_operation", name="test_name", seconds=30, count=10
        )
        assert bexec.metrics.empty is False

        expected = {
            "executor": "base",
            "operation": "test_operation",
            "name": "test_name",
            "seconds": 30,
            "count": 10,
            "hole_direction": HoleDirection.H,
        }
        actual = bexec.metrics.iloc[0].to_dict()
        assert expected == actual

    @pytest.mark.asyncio
    async def test_persist(self, bexec, model_df, bind):
        await bexec._persist("entity_name", Model, model_df, reset_index=False)
        actual = len(await Model.pk.values)
        expected = model_df.shape[0]
        assert expected == actual

    @pytest.mark.asyncio
    @pytest.mark.parametrize("df", [pd.DataFrame(), None])
    async def test_persist_empty_or_none(self, bexec, df, caplog, bind):
        caplog.set_level(10)
        await bexec._persist("entity_name", Model, df)
        assert "nothing to persist" in caplog.text
        assert bexec.metrics.empty  # no metrics added


class TestProdExecutor:
    @pytest.fixture
    def pexec(self):
        yield ProdExecutor(HoleDirection.H)

    def test_init_default(self):
        pexec = ProdExecutor(HoleDirection.H)
        assert pexec.metrics.empty is True
        assert pexec.model_kwargs["stats"] == {"batch_size": 1000}

    def test_init_model_kwargs(self):
        header_kwargs = {1: 1}
        monthly_kwargs = {2: 2}
        stats_kwargs = {3: 3}
        pexec = ProdExecutor(
            HoleDirection.H,
            header_kwargs=header_kwargs,
            monthly_kwargs=monthly_kwargs,
            stats_kwargs=stats_kwargs,
        )

        assert pexec.model_kwargs["header"] == header_kwargs
        assert pexec.model_kwargs["monthly"] == monthly_kwargs
        assert pexec.model_kwargs["stats"] == {"batch_size": 1000, **stats_kwargs}

    @pytest.mark.parametrize("hole_dir", HoleDirection.members())
    @pytest.mark.asyncio
    async def test_download(self, prod_dispatcher, hole_dir):
        pexec = ProdExecutor(hole_dir)
        prodset = await pexec.download(
            entities=["a", "b", "c"], dispatch=prod_dispatcher
        )
        # check prodset was returned
        assert isinstance(prodset, ProdSet)
        # check metric was added
        assert pexec.metrics.shape[0] == 1

    @pytest.mark.asyncio
    async def test_download_bad_holedir(self):
        pexec = ProdExecutor(HoleDirection.H)
        pexec.hole_dir = ProdStatRange.FIRST
        with pytest.raises(ValueError):
            await pexec.download(entities=["a", "b", "c"])

    @pytest.mark.parametrize("hole_dir", HoleDirection.members())
    @pytest.mark.cionly
    @pytest.mark.asyncio
    async def test_download_catch_network_error(self, prod_dispatcher, hole_dir):
        pexec = ProdExecutor(hole_dir)
        with pytest.raises(Exception):
            await pexec.download(entities=["a", "b", "c"])

    @pytest.mark.cionly
    @pytest.mark.asyncio
    async def test_process_and_persist_with_default_option_sets(self, prod_df_h, bind):
        prodset_h = prod_df_h.prodstats.to_prodset()
        pexec = ProdExecutor(HoleDirection.H)
        ps = await pexec.process(prodset_h)

        assert ps.header.shape[0] == prod_df_h.index.levels[0].shape[0]
        assert ps.monthly.shape[0] == prod_df_h.shape[0]
        assert ps.stats.shape[0] > 0
        await pexec.persist(ps)

        expected = ps.stats.shape[0]
        actual = await Model.agg.count()
        assert expected == actual

    @pytest.mark.cionly
    @pytest.mark.asyncio
    async def test_process_and_persist_h_one_option_set(self, prod_df_h, bind):
        prodset_h = prod_df_h.prodstats.to_prodset()
        pexec = ProdExecutor(HoleDirection.H)
        opts = calc.prodstat_option_matrix(
            ProdStatRange.FIRST, months=[6], include_zeroes=False
        )
        ps = await pexec.process(prodset_h, prodstat_opts=opts, ratio_opts=opts)
        await pexec.persist(ps)
        expected = ps.stats.shape[0]
        actual = await Model.agg.count()

        assert expected == actual
        assert ps.header.shape[0] == prod_df_h.reset_index().api14.unique().shape[0]
        assert ps.monthly.shape[0] == prod_df_h.shape[0]
        assert ps.stats.shape[0] > 0

    @pytest.mark.cionly
    @pytest.mark.asyncio
    async def test_process_and_persist_v_one_option_set(self, prod_df_v, bind):

        # select an entity12 from the available dataframe that is likely to have
        # more than one, but not too many, associated wells (for test speed)

        entity12 = (
            prod_df_v.groupby("entity12").count().iloc[:, 0].sort_values().index[2]
        )
        prod_df_v = prod_df_v.loc[prod_df_v.entity12 == entity12].copy(deep=True)
        prodset_v = prod_df_v.prodstats.to_prodset()
        pexec = ProdExecutor(HoleDirection.V)
        opts = calc.prodstat_option_matrix(
            ProdStatRange.FIRST, months=[6], include_zeroes=False
        )
        ps = await pexec.process(prodset_v, prodstat_opts=opts, ratio_opts=opts)
        await pexec.persist(ps)
        expected = ps.stats.shape[0]
        actual = await Model.agg.count()

        assert expected == actual
        assert ps.header.shape[0] == prod_df_v.reset_index().api14.unique().shape[0]
        assert ps.monthly.shape[0] == prod_df_v.shape[0]
        assert ps.stats.shape[0] > 0

    # @pytest.mark.cionly
    # @pytest.mark.asyncio
    # async def test_process_and_persist_v_with_default_option_sets(
    #     self, prod_df_v, bind
    # ):

    #     prodset_v = prod_df_v.prodstats.to_prodset()
    #     pexec = ProdExecutor(HoleDirection.V)
    #     ps = await pexec.process(prodset_v)
    #     await pexec.persist(ps)
    #     expected = ps.stats.shape[0]
    #     actual = await Model.agg.count()

    #     assert expected == actual
    #     assert ps.header.shape[0] == prod_df_v.index.levels[0].shape[0]
    #     assert ps.monthly.shape[0] == prod_df_v.groupby(level=[0, 1]).first().shape[0]
    #     assert ps.stats.shape[0] > 0

    @pytest.mark.asyncio
    async def test_process_and_persist_h_tiny_batch(self, prod_df_h, bind):

        api14s = (
            prod_df_h.groupby("api14")
            .count()
            .iloc[:, 0]
            .sort_values()
            .index[:2]
            .values.tolist()
        )

        prod_df_h = prod_df_h.loc[prod_df_h.api14.isin(api14s)]
        prodset_h = prod_df_h.prodstats.to_prodset()
        pexec = ProdExecutor(HoleDirection.H)
        opts = calc.prodstat_option_matrix(
            ProdStatRange.FIRST, months=[6], include_zeroes=False
        )
        ps = await pexec.process(prodset_h, prodstat_opts=opts, ratio_opts=opts)
        await pexec.persist(ps)
        expected = ps.stats.shape[0]
        actual = await Model.agg.count()

        assert expected == actual
        assert ps.header.shape[0] == prod_df_h.reset_index().api14.unique().shape[0]
        assert ps.monthly.shape[0] == prod_df_h.groupby(level=[0, 1]).first().shape[0]
        assert ps.stats.shape[0] > 0

    @pytest.mark.asyncio
    async def test_process_and_persist_v_tiny_batch(self, prod_df_v, bind):

        # select an entity12 from the available dataframe that is likely to have
        # more than one, but not too many, associated wells (for test speed)

        api14s = (
            prod_df_v.groupby("api14")
            .count()
            .iloc[:, 0]
            .sort_values()
            .index[:2]
            .values.tolist()
        )

        prod_df_v = prod_df_v.loc[prod_df_v.api14.isin(api14s)]
        prodset_v = prod_df_v.prodstats.to_prodset()
        pexec = ProdExecutor(HoleDirection.V)
        opts = calc.prodstat_option_matrix(
            ProdStatRange.FIRST, months=[6], include_zeroes=False
        )
        ps = await pexec.process(prodset_v, prodstat_opts=opts, ratio_opts=opts)
        await pexec.persist(ps)
        expected = ps.stats.shape[0]
        actual = await Model.agg.count()

        assert expected == actual
        assert ps.header.shape[0] == prod_df_v.reset_index().api14.unique().shape[0]
        assert ps.monthly.shape[0] == prod_df_v.shape[0]
        assert ps.stats.shape[0] > 0

    @pytest.mark.asyncio
    async def test_arun_h_tiny_batch(self, prod_h, bind):

        prod_h = prod_h[:5]
        api14s = [x["api14"] for x in prod_h]

        dispatch = MockAsyncDispatch({"data": prod_h})
        opts = calc.prodstat_option_matrix(
            ProdStatRange.FIRST, months=[6], include_zeroes=False
        )
        pexec = ProdExecutor(
            HoleDirection.H,
            download_kwargs={"dispatch": dispatch},
            process_kwargs={"prodstat_opts": opts, "ratio_opts": opts},
        )

        ct, ps = await pexec.arun(api14s=api14s)
        actual = await ProdHeader.agg.count()
        assert len(api14s) == actual

    @pytest.mark.asyncio
    async def test_arun_v_tiny_batch(self, prod_v, bind):

        prod_v = prod_v[:5]
        api14s = [x["api14"] for x in prod_v]

        dispatch = MockAsyncDispatch({"data": prod_v})
        opts = calc.prodstat_option_matrix(
            ProdStatRange.FIRST, months=[6], include_zeroes=False
        )
        pexec = ProdExecutor(
            HoleDirection.V,
            download_kwargs={"dispatch": dispatch},
            process_kwargs={"prodstat_opts": opts, "ratio_opts": opts},
        )

        ct, ps = await pexec.arun(api14s=api14s)
        actual = await ProdHeader.agg.count()
        assert len(api14s) == actual

    # def test_run_h_tiny_batch(self, prod_h, bind):
    #     # FIXME: causing asyncpg.exceptions._base.InterfaceError: cannot perform operation: another operation is in progress  # noqa
    #     prod_h = prod_h[:5]
    #     api14s = [x["api14"] for x in prod_h]

    #     dispatch = MockAsyncDispatch({"data": prod_h})
    #     opts = calc.prodstat_option_matrix(
    #         ProdStatRange.FIRST, months=[6], include_zeroes=False
    #     )
    #     pexec = ProdExecutor(
    #         HoleDirection.H,
    #         download_kwargs={"dispatch": dispatch},
    #         process_kwargs={"prodstat_opts": opts, "ratio_opts": opts},
    #     )

    #     ct, ps = pexec.run(api14s=api14s, persist=False)
    #     print(ct)
    #     print(ps)


class TestGeomExecutor:
    @pytest.fixture
    def gexec(self):
        yield GeomExecutor(HoleDirection.H)

    def test_init_default(self):
        gexec = GeomExecutor(HoleDirection.H)
        assert gexec.metrics.empty is True

    def test_init_model_kwargs(self):
        locations_kwargs = {1: 1}
        surveys_kwargs = {2: 2}
        points_kwargs = {3: 3}
        gexec = GeomExecutor(
            HoleDirection.H,
            locations_kwargs=locations_kwargs,
            surveys_kwargs=surveys_kwargs,
            points_kwargs=points_kwargs,
        )

        assert gexec.model_kwargs["locations"] == locations_kwargs
        assert gexec.model_kwargs["surveys"] == surveys_kwargs
        assert gexec.model_kwargs["points"] == {"batch_size": 1000, **points_kwargs}

    @pytest.mark.parametrize("hole_dir", HoleDirection.members())
    @pytest.mark.asyncio
    async def test_download(self, wells_h_dispatcher, hole_dir):
        gexec = GeomExecutor(hole_dir)
        geomset = await gexec.download(
            api14s=["a", "b", "c"], dispatch=wells_h_dispatcher
        )
        assert isinstance(geomset, WellGeometrySet)
        assert gexec.metrics.shape[0] == 1

    @pytest.mark.asyncio
    async def test_download_bad_holedir(self):
        gexec = GeomExecutor(HoleDirection.H)
        gexec.hole_dir = ProdStatRange.FIRST
        with pytest.raises(ValueError):
            await gexec.download(zaza=["a", "b", "c"])

    @pytest.mark.parametrize("hole_dir", HoleDirection.members())
    @pytest.mark.asyncio
    async def test_download_catch_network_error(self, hole_dir):
        gexec = GeomExecutor(hole_dir)
        with pytest.raises(Exception):
            await gexec.download(zaza=["a", "b", "c"])

    @pytest.mark.cionly
    @pytest.mark.asyncio
    async def test_process_and_persist_h_full(self, geomset_h, bind):
        gexec = GeomExecutor(HoleDirection.H)
        dataset: WellGeometrySet = await gexec.process(geomset_h)
        await gexec.persist(dataset)

    @pytest.mark.asyncio
    async def test_process_and_persist_h_small_batch(self, geoms_h, bind):
        geoms = geoms_h[:3]
        geomset = pd.DataFrame.shapes.from_records(geoms, create_index=True)
        gexec = GeomExecutor(HoleDirection.H)
        dataset: WellGeometrySet = await gexec.process(geomset)
        await gexec.persist(dataset)

    @pytest.mark.cionly
    @pytest.mark.asyncio
    async def test_process_and_persist_v_full(self, geomset_v, bind):
        gexec = GeomExecutor(HoleDirection.V)
        dataset: WellGeometrySet = await gexec.process(geomset_v)
        await gexec.persist(dataset)

    @pytest.mark.asyncio
    async def test_process_and_persist_v_small_batch(self, geoms_v, bind):
        geoms = geoms_v[:3]
        geomset = pd.DataFrame.shapes.from_records(geoms, create_index=True)
        gexec = GeomExecutor(HoleDirection.V)
        dataset: WellGeometrySet = await gexec.process(geomset)
        await gexec.persist(dataset)


class TestWellExecutor:
    @pytest.fixture
    def exh(self, wells_h, fracs_h, geoms_h, prod_headers_h):
        ihs_dispatch = MockAsyncDispatch({"data": wells_h})
        fracfocus_dispatch = MockAsyncDispatch({"data": fracs_h})
        geoms_dispatch = MockAsyncDispatch({"data": geoms_h})
        prod_headers_dispatch = MockAsyncDispatch({"data": prod_headers_h})
        exh = WellExecutor(
            HoleDirection.H,
            download_kwargs={
                "dispatch": {"dispatch": ihs_dispatch},
                "ihs_kwargs": {"dispatch": ihs_dispatch},
                "fracfocus_kwargs": {"dispatch": fracfocus_dispatch},
            },
            process_kwargs={
                "geoms_dispatch": geoms_dispatch,
                "prod_headers_dispatch": prod_headers_dispatch,
            },
        )
        yield exh

    @pytest.fixture
    def exv(self, wells_v, fracs_v, geoms_v, prod_headers_v):
        ihs_dispatch = MockAsyncDispatch({"data": wells_v})
        fracfocus_dispatch = MockAsyncDispatch({"data": fracs_v})
        geoms_dispatch = MockAsyncDispatch({"data": geoms_v})

        # geoms = await pd.DataFrame.shapes.from_ihs(
        #     IHSPath.well_h_geoms,
        #     api14s=api14h,
        #     # dispatch=geoms_dispatch,
        # )
        # await IHSClient.get_wells(
        #     IHSPath.well_h_geoms,
        #     api14s=api14h
        #     # , dispatch=geoms_dispatch
        # )
        # geoms_h

        prod_headers_dispatch = MockAsyncDispatch({"data": prod_headers_v})
        exv = WellExecutor(
            HoleDirection.V,
            download_kwargs={
                "dispatch": {"dispatch": ihs_dispatch},
                "ihs_kwargs": {"dispatch": ihs_dispatch},
                "fracfocus_kwargs": {"dispatch": fracfocus_dispatch},
            },
            process_kwargs={
                "geoms_dispatch": geoms_dispatch,
                "prod_headers_dispatch": prod_headers_dispatch,
            },
        )
        yield exv

    def test_init_default(self):
        ex = WellExecutor(HoleDirection.H)
        assert ex.metrics.empty is True

    def test_init_model_kwargs(self):
        wells_kwargs = {1: 1}
        depths_kwargs = {2: 2}
        fracs_kwargs = {3: 3}
        ips_kwargs = {4: 4}
        stats_kwargs = {5: 5}
        links_kwargs = {6: 6}

        ex = WellExecutor(
            HoleDirection.H,
            wells_kwargs=wells_kwargs,
            depths_kwargs=depths_kwargs,
            fracs_kwargs=fracs_kwargs,
            ips_kwargs=ips_kwargs,
            stats_kwargs=stats_kwargs,
            links_kwargs=links_kwargs,
        )

        assert ex.model_kwargs["wells"] == wells_kwargs
        assert ex.model_kwargs["depths"] == depths_kwargs
        assert ex.model_kwargs["fracs"] == fracs_kwargs
        assert ex.model_kwargs["ips"] == ips_kwargs
        assert ex.model_kwargs["stats"] == stats_kwargs
        assert ex.model_kwargs["links"] == links_kwargs

    @pytest.mark.parametrize("hole_dir", HoleDirection.members())
    @pytest.mark.asyncio
    async def test_download(self, hole_dir, wells_h, wells_v, fracs_h, fracs_v):
        wells = wells_h if hole_dir == HoleDirection.H else wells_v
        fracs = fracs_h if hole_dir == HoleDirection.H else fracs_v

        ihs_dispatch = MockAsyncDispatch({"data": wells})
        fracfocus_dispatch = MockAsyncDispatch({"data": fracs})

        ex = WellExecutor(hole_dir)
        wellset = await ex.download(
            api14s=["a", "b", "c"],
            ihs_kwargs={"dispatch": ihs_dispatch},
            fracfocus_kwargs={"dispatch": fracfocus_dispatch},
        )

        assert isinstance(wellset, WellSet)
        assert ex.metrics.shape[0] == 1

    @pytest.mark.asyncio
    async def test_download_bad_holedir(self):
        ex = WellExecutor(HoleDirection.H)
        ex.hole_dir = ProdStatRange.FIRST
        with pytest.raises(ValueError):
            await ex.download(zaza=["a", "b", "c"])

    @pytest.mark.parametrize("hole_dir", HoleDirection.members())
    @pytest.mark.asyncio
    async def test_download_catch_network_error(self, hole_dir):
        ex = WellExecutor(hole_dir)
        with pytest.raises(Exception):
            await ex.download(zaza=["a", "b", "c"])

    @pytest.mark.cionly
    @pytest.mark.asyncio
    async def test_process_and_persist_h_full(self, exh, wellset_h, bind):
        dataset: WellSet = await exh.process(wellset_h)
        await exh.persist(dataset)

    # @pytest.mark.asyncio
    # async def test_process_and_persist_h_small_batch(self, geoms_h, bind):
    #     geoms = geoms_h[:3]
    #     wellset = pd.DataFrame.shapes.from_records(geoms, create_index=True)
    #     ex = WellExecutor(HoleDirection.H)
    #     dataset: WellSet = await ex.process(wellset)
    #     await ex.persist(dataset)

    @pytest.mark.cionly
    @pytest.mark.asyncio
    async def test_process_and_persist_v_full(self, exv, wellset_v, bind):
        dataset: WellSet = await exv.process(wellset_v)
        await exv.persist(dataset)

    # @pytest.mark.asyncio
    # async def test_process_and_persist_v_small_batch(self, geoms_v, bind):
    #     geoms = geoms_v[:3]
    #     wellset = pd.DataFrame.shapes.from_records(geoms, create_index=True)
    #     ex = WellExecutor(HoleDirection.V)
    #     dataset: WellSet = await ex.process(wellset)
    #     await ex.persist(dataset)


if __name__ == "__main__":
    import util
    import loggers
    from db import db

    util.aio.async_to_sync(db.startup())

    loggers.config(level=10)

    # dir(prod_dispatcher)
    # ihs_prod = load_json("tests/fixtures/ihs_prod.json")
    # prod_df_h = ProductionWellSet(wells=ihs_prod).df().copy(deep=True)
    # prodset = prod_df_h.prodstats.preprocess_header_and_monthly_records()

    # prod_dispatcher = next(prod_dispatcher.__wrapped__(ihs_prod))

    # x = len(calc.PRODSTAT_DEFAULT_OPTIONS)
    # y = len(calc.PRODSTAT_DEFAULT_RATIO_OPTIONS)
    # ((x * 4) + (y * (4 * 3))) * 124

    # 54099
