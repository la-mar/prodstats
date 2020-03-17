import pandas as pd
import pytest

import db.models
from calc.sets import DataSet, ProdSet, SetItem


@pytest.fixture
def item():
    yield SetItem(
        name="test", model=db.models.Model, df=pd.DataFrame([{"key": 1}, {"key": 2}]),
    )


@pytest.fixture
def dataset():
    yield DataSet(models={"frame1": db.models.Model, "frame2": db.models.Model})


class TestSetItem:
    def test_repr(self, item):
        repr(item)

    def test_iter(self, item):
        elements = list(item)
        assert elements[0] == "test"
        assert elements[1] == db.models.Model
        assert isinstance(elements[2], pd.DataFrame)


class TestDataSet:
    def test_repr(self, dataset):
        repr(dataset)

    def test_describe(self, dataset):
        assert dataset.describe() == {"frame1": 0, "frame2": 0}

    def test_iter(self, dataset):
        assert list(dataset) == [None, None]

    def test_items(self, dataset):
        expected = [
            SetItem(name="frame1", model=db.models.Model, df=pd.DataFrame()),
            SetItem(name="frame2", model=db.models.Model, df=pd.DataFrame()),
        ]
        actual = list(dataset.items())

        for idx in range(0, 2):
            assert expected[idx].name == actual[idx].name
            assert expected[idx].model == actual[idx].model
            assert isinstance(expected[idx].df, pd.DataFrame) and isinstance(
                actual[idx].df, pd.DataFrame
            )


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
