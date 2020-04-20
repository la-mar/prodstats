import logging
from datetime import date, datetime

import pytest
from pydantic import ValidationError

import schemas.well as sch

logger = logging.getLogger(__name__)


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
        depths = {
            "api14": "42461409160000",
            "tvd": "2200",
            "md": "2100",
            "perf_upper": "2200",
            "perf_lower": "2100",
            "plugback_depth": "2100",
        }
        yield depths

    def test_aliases(self, depths):

        parsed = sch.WellDepths(**depths).dict()
        actual = {*parsed.keys()}
        expected = {*sch.WellDepths(**depths).__fields__.keys()}
        assert expected == actual
        for key, value in parsed.items():
            if key not in ["api14"]:
                assert isinstance(value, int)

    def test_extract_from_document(self, wells_h):
        data = wells_h[0]
        actual = sch.WellDepths(**data).dict()

        expected = {
            "api14": data["api14"],
            "tvd": data["tvd"],
            "md": data["md"],
            "perf_upper": data["perf_upper"],
            "perf_lower": data["perf_lower"],
            "plugback_depth": data["plugback_depth"],
        }
        assert expected == actual


class TestWellRecord:
    def test_aliases(self, wells_h):
        data = wells_h[0]
        obj = sch.WellRecord(**data)
        parsed = obj.dict()
        actual = {*parsed.keys()}
        expected = {*obj.__fields__.keys()}
        assert expected == actual

    def test_convert_to_record(self, wells_h):
        data = wells_h[0]
        obj = sch.WellRecord(**data)
        record = obj.record()
        fields = {
            *sch.WellDates().__fields__.keys(),
            *sch.WellElevations().__fields__.keys(),
        }
        for field in fields:
            assert field in record.keys()


class TestWellRecordSet:
    def test_records(self, wells_h):
        obj = sch.WellRecordSet(wells=wells_h)
        records = obj.records()
        assert isinstance(records, list)
        assert isinstance(records[0], dict)
        assert isinstance(records[0]["api14"], str)
        assert len(records) == len(wells_h)

    def test_df_record_count(self, wells_h):
        obj = sch.WellRecordSet(wells=wells_h)
        df = obj.df()

        assert df.shape[0] == len(wells_h)
        assert {*df.index.values} == {x["api14"] for x in wells_h}


class TestIPTest:
    def test_aliases(self, wells_h, wells_v):
        for row in wells_h + wells_v:
            for ip in row.get("ip", []):
                try:
                    obj = sch.IPTest(**ip)
                    parsed = obj.dict()
                    actual = {*parsed.keys()}
                    expected = {*obj.__fields__.keys()}
                    assert expected == actual
                except ValidationError as ve:
                    logger.info(ve)

    def test_records(self, wells_h, wells_v):
        for row in wells_h + wells_v:
            records = sch.IPTests(**row).records()
            if records:
                assert isinstance(records, list)
                assert isinstance(records[0], dict)
                assert isinstance(records[0]["api14"], str)
            else:
                assert records == []


# if __name__ == "__main__":
#     from util.jsontools import load_json
#     wells_h = load_json(f"tests/fixtures/wells_h.json")
