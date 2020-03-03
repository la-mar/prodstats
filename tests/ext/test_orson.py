from datetime import datetime

import pytz

from ext.orjson import orjson_dumps


class TestORJsonDumps:
    def test_utc_to_utc(self):
        dt = datetime(year=2019, month=1, day=1, hour=6)
        assert orjson_dumps({"dt": dt}) == orjson_dumps(
            {"dt": pytz.UTC.localize(dt).isoformat()}
        )
