from datetime import date, datetime

import pytest

import schemas.well as sch


class TestWellDates:
    @pytest.fixture
    def dates(self):
        yield {
            "permit": "2018-06-28",
            "permit_expiration": "2020-06-28",
            "spud": "2018-07-11",
            "comp": "2018-10-04",
            "final_drill": "2018-08-10",
            "rig_release": "2018-08-22",
            "first_report": "2018-06-29",
            "ihs_last_update": "2019-08-07",
        }

    def test_aliases(self, dates):

        parsed = sch.WellDates(**dates).dict()

        actual = {*parsed.keys()}
        expected = {*sch.WellDates().__fields__.keys()}
        assert expected == actual

        for key, value in parsed.items():
            assert isinstance(value, (date, datetime))


class TestFracParams:
    @pytest.fixture
    def fracparms(self):
        yield {
            "api14": "12345678900000",
            "provider": "ihs",
            "last_update_at": "2020-03-21T16:30:52.778000",
            "fluid_total": "737741",
            "proppant_total": "26738000",
        }

    def test_aliases(self, fracparms):
        parsed = sch.FracParameters(**fracparms).dict()
        actual = {*parsed.keys()}
        expected = {*sch.FracParameters(**fracparms).__fields__.keys()}
        assert expected == actual


class TestWellElevations:
    @pytest.fixture
    def elevs(self):
        yield {"ground": "2200", "kb": "2100"}

    def test_aliases(self, elevs):
        parsed = sch.WellElevations(**elevs).dict()
        actual = {*parsed.keys()}
        expected = {*sch.WellElevations().__fields__.keys()}
        assert expected == actual
        for key, value in parsed.items():
            if key not in ["api14"]:
                assert isinstance(value, int)


class TestWellDepths:
    @pytest.fixture
    def depths(self):
        yield {
            "api14": "42461409160000",
            "tvd": "2200",
            "md": "2100",
            "perf_upper": "2200",
            "perf_lower": "2100",
            "plugback_depth": "2100",
        }

    def test_aliases(self, depths):

        parsed = sch.WellDepths(**depths).dict()
        actual = {*parsed.keys()}
        expected = {*sch.WellDepths(**depths).__fields__.keys()}
        assert expected == actual
        for key, value in parsed.items():
            if key not in ["api14"]:
                assert isinstance(value, int)

    def test_extract_from_document(self, ihs_wells):
        data = ihs_wells[0]
        actual = sch.WellDepths(**data).dict()
        expected = {
            "api14": "42461409160000",
            "tvd": 8503,
            "md": 19272,
            "perf_upper": 8805,
            "perf_lower": 19166,
            "plugback_depth": 19189,
        }
        assert expected == actual


class TestWellRecord:
    def test_aliases(self, ihs_wells):
        data = ihs_wells[0]
        obj = sch.WellRecord(**data)
        parsed = obj.dict()
        actual = {*parsed.keys()}
        expected = {*obj.__fields__.keys()}
        assert expected == actual

    def test_convert_to_record(self, ihs_wells):
        data = ihs_wells[0]
        obj = sch.WellRecord(**data)
        record = obj.record()
        fields = {
            *sch.WellDates().__fields__.keys(),
            *sch.WellElevations().__fields__.keys(),
        }
        for field in fields:
            assert field in record.keys()


class TestWellRecordSet:
    def test_records(self, ihs_wells):
        obj = sch.WellRecordSet(wells=ihs_wells)
        records = obj.records()
        assert isinstance(records, list)
        assert isinstance(records[0], dict)
        assert isinstance(records[0]["api14"], str)
        assert len(records) == 2

    def test_df(self, ihs_wells):
        obj = sch.WellRecordSet(wells=ihs_wells)
        df = obj.df()
        assert df.shape[0] == 2
        assert {*df.index.values} == {"42383406370000", "42461409160000"}


class TestIPTest:
    def test_aliases(self, ihs_wells):
        data = ihs_wells[0]
        obj = sch.IPTest(**data["ip"][0])
        parsed = obj.dict()
        actual = {*parsed.keys()}
        expected = {*obj.__fields__.keys()}
        assert expected == actual

    def test_records(self, ihs_wells):
        data = ihs_wells[0]
        records = sch.IPTests(**data).records()
        assert isinstance(records, list)
        assert isinstance(records[0], dict)
        assert isinstance(records[0]["api14"], str)


# if __name__ == "__main__":
#     from util.jsontools import load_json
#     ihs_wells = load_json(f"tests/fixtures/ihs_wells.json")
