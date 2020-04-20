import logging

import numpy as np
import pandas as pd
import pytest

from const import HoleDirection, IHSPath
from tests.utils import MockAsyncDispatch

logger = logging.getLogger(__name__)


# @pytest.fixture(scope="session")
# def wells_h(json_fixture):
#     yield json_fixture("deo_wells.json")


# @pytest.fixture
# def wellset_h(wells_h):
#     yield pd.DataFrame.wells.from_records(wells_h)


class TestWells:
    def test_instantiate_df_ext(self):
        pd.DataFrame.wells

    @pytest.mark.parametrize("hole_dir", HoleDirection.members())
    def test_from_records(self, hole_dir, wells_h, wells_v):
        wells = wells_h if hole_dir == HoleDirection.H else wells_v

        wellcount = len(wells)
        wellset = pd.DataFrame.wells.from_records(wells, create_index=True)

        assert wellset.wells.shape[0] == wellcount
        assert wellset.depths.shape[0] == wellcount
        assert wellset.fracs.shape[0] == wellcount

    @pytest.mark.parametrize("hole_dir", HoleDirection.members())
    @pytest.mark.asyncio
    async def test_from_fracfocus(self, hole_dir, fracs_h, fracs_v):
        fracs = fracs_h if hole_dir == HoleDirection.H else fracs_v

        api14s = [x["api14"] for x in fracs]
        dispatch = MockAsyncDispatch({"data": fracs})
        wellset = await pd.DataFrame.wells.from_fracfocus(
            api14s=api14s, dispatch=dispatch
        )
        assert {*wellset.fracs.index} == set(api14s)

    @pytest.mark.parametrize("hole_dir", HoleDirection.members())
    @pytest.mark.asyncio
    async def test_from_multiple(self, hole_dir, wells_h, wells_v, fracs_h, fracs_v):
        wells = wells_h if hole_dir == HoleDirection.H else wells_v
        fracs = fracs_h if hole_dir == HoleDirection.H else fracs_v

        ihs_dispatch = MockAsyncDispatch({"data": wells})
        fracfocus_dispatch = MockAsyncDispatch({"data": fracs})

        api14s = [x["api14"] for x in wells]
        wellset = await pd.DataFrame.wells.from_multiple(
            hole_dir,
            api14s=api14s,
            ihs_kwargs={"dispatch": ihs_dispatch},
            fracfocus_kwargs={"dispatch": fracfocus_dispatch},
        )
        assert {*wellset.wells.index} == set(api14s)
        assert len({*wellset.fracs.index}) <= len(set(api14s))

    @pytest.mark.asyncio
    async def test_from_sample(self, wells_h, fracs_h):
        wells = wells_h
        fracs = fracs_h
        path = IHSPath.well_h_sample

        ihs_dispatch = MockAsyncDispatch({"data": wells})
        fracfocus_dispatch = MockAsyncDispatch({"data": fracs})

        api14s = [x["api14"] for x in wells]

        wellset = await pd.DataFrame.wells.from_sample(
            path,
            n=5,
            ihs_kwargs={"dispatch": ihs_dispatch},
            fracfocus_kwargs={"dispatch": fracfocus_dispatch},
        )

        assert {*wellset.wells.index} == set(api14s)
        assert len({*wellset.fracs.index}) <= len(set(api14s))

    def test_combine_frac_parameters(self, wellset_h):
        df1 = pd.DataFrame(
            columns=["fluid", "proppant"],
            index=pd.Index([0, 1, 2, 3, 4, 5], name="api14"),
        )
        df1.index.name = "api14"
        df2 = df1.copy(deep=True)
        df1.loc[[1, 5]] = 10
        df2.loc[[2, 4]] = 5

        actual = df1.wells.combine_frac_parameters(df2, dropna=True)
        expected = np.array([[10, 10], [5, 5], [5, 5], [10, 10]])
        assert np.array_equal(actual.to_numpy(), expected)

    def test_status_assignment(self, wellset_h):
        # TODO: Need good test cases
        """ input format
        api14	        status	        spud_date	comp_date	permit_date	last_prod_date
        42461409160000	OIL PRODUCER	2018-07-11	2018-10-04	2018-06-28	2020-01-31
        42461411600000	TREATD	        2019-05-25	2019-04-16
        42461411270000	OIL PRODUCER	2019-04-05	2019-06-20	2019-03-08	2020-01-31
        42461411260000	OIL PRODUCER	2019-05-15	2019-09-20	2019-04-16
        42383402790000	OIL PRODUCER	2018-06-10	2018-09-07	2018-05-11	2020-01-31

        """
