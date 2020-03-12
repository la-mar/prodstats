from datetime import date

import pytest

from schemas import ProductionRecord, ProductionWell, ProductionWellSet


@pytest.fixture
def production():
    yield [
        {
            "prod_date": "2020-01-01",
            "days_in_month": "31",
            "oil": "10",
            "oil_uom": "BBL",
            "gas": "10",
            "gas_uom": "BBL",
            "water": "10",
            "water_uom": "BBL",
            "gor": "10",
            "gor_uom": "BBL",
            "water_cut": "10",
        }
        for x in range(0, 10)
    ]


@pytest.fixture
def well(production):
    yield {
        "entity12": "123456789102",
        "entity": "123456789102404",
        "api10": "1234567891",
        "api14": "12345678910000",
        "status": "ACTIVE",
        "provider": "data_provider",
        "last_update_at": "2020-03-12T20:44:16.992142",
        "perf_upper_min": 1000,
        "perf_lower_max": 3000,
        "perf_ll": 2000,
        "products": "O",
        "production": production,
    }


class TestProdRecord:
    def test_convert_aliases(self):
        record = {
            "first_date": "2020-01-01",
            "last_day": "31",
            "liquid": "10",
            "liquid_uom": "BBL",
        }

        actual = ProductionRecord(**record).dict(exclude_none=True)
        expected = {
            "prod_date": date(2020, 1, 1),
            "days_in_month": 31,
            "oil": 10,
            "oil_uom": "BBL",
        }

        assert expected == actual


class TestProdWell:
    def test_records(self, well):
        actual = ProductionWell(**well).records()

        assert len(actual) == len(well["production"])

    def test_df(self, well):
        actual = {*ProductionWell(**well).df(create_index=False).columns.tolist()}
        expected = {
            *[x for x in ProductionWell.__fields__.keys() if x not in ["production"]],
            *ProductionRecord.__fields__.keys(),
        }
        assert actual == expected

    def test_df_with_index(self, well):
        df = ProductionWell(**well).df(create_index=True)
        assert {*df.index.names} == {"api10", "prod_date"}


class TestProdWellSet:
    def test_records(self, well):
        wells = [well for x in range(0, 5)]
        actual = ProductionWellSet(wells=wells).records()
        expected = sum([len(x["production"]) for x in wells])
        assert len(actual) == expected

    def test_df(self, well):
        actual = {*ProductionWell(**well).df(create_index=False).columns.tolist()}
        expected = {
            *[x for x in ProductionWell.__fields__.keys() if x not in ["production"]],
            *ProductionRecord.__fields__.keys(),
        }
        assert actual == expected

    def test_df_with_index(self, well):
        wells = [well for x in range(0, 5)]
        df = ProductionWellSet(wells=wells).df(create_index=True)
        assert {*df.index.names} == {"api10", "prod_date"}
