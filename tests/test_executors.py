import logging

import executors as ex

logger = logging.getLogger(__name__)


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
