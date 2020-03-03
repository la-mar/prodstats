import logging

import pytest
from requests_mock import ANY
from tests.utils import get_open_port

import ext.metrics.metrics as metrics
import loggers
from ext.metrics import load, post, post_event, post_heartbeat, to_tags

api_endpoint = f"http://localhost:{get_open_port}"

logger = logging.getLogger(__name__)

# TODO: Logger is throwing error when flushing after tests. Unknown cause.


@pytest.fixture(autouse=True)
def disable_logging_after_test():
    """ Manually silence all loggers after each test to prevent superflous errors
        from a logger trying to write to stdout/stderr after the test has completed.

        Reference: https://bugs.python.org/msg155166 """

    yield
    logging.disable(logging.CRITICAL)


class TestMetrics:
    def test_load_datadog_ext(self, conf, monkeypatch, capfd):  # TODO: needs assertion
        loggers.config(level=10, logger="ext.metrics.metrics")
        monkeypatch.setattr(conf, "DATADOG_API_KEY", "pretend_api_key", raising=True)
        monkeypatch.setattr(conf, "DATADOG_APP_KEY", "pretend_app_key", raising=True)
        monkeypatch.setattr(conf, "DATADOG_ENABLED", True, raising=True)

        load()

        # captured = capfd.readouterr()
        # actual = captured.err.lower()
        # logger.warning(captured)
        # assert "enabled" in actual

    def test_missing_api_key(self, conf, monkeypatch, capfd):
        loggers.config(level=10, logger="ext.metrics.metrics")
        monkeypatch.setattr(conf, "DATADOG_API_KEY", None, raising=True)
        monkeypatch.setattr(conf, "DATADOG_APP_KEY", None, raising=True)
        monkeypatch.setattr(conf, "DATADOG_ENABLED", True, raising=True)

        load()

        # captured = capfd.readouterr()
        # actual = captured.err.lower()
        # assert "missing api key" in actual

    def test_dict_to_tags(self):
        data = {
            "tag_name": "tag_value",
            "tag_name2": "tag_value2",
        }
        assert to_tags(data) == ["tag_name:tag_value", "tag_name2:tag_value2"]

    def test_list_to_tags(self):
        data = ["tag_name:tag_value", "tag_name2:tag_value2"]
        assert to_tags(data) == data

    def test_comma_delimited_string_to_tags(self):
        data = "tag_name:tag_value,tag_name2:tag_value2"
        assert to_tags(data) == ["tag_name:tag_value", "tag_name2:tag_value2"]

    def test_non_delimited_string_to_tags(self):
        data = "thisismytagandiamproudofit"
        assert to_tags(data) == ["thisismytagandiamproudofit"]

    def test_invalid_type_to_tags(self):
        data = 1
        assert to_tags(data) == []

    def test_post_success(self, conf, monkeypatch, capfd, requests_mock):
        requests_mock.register_uri(ANY, ANY, json={"status": "ok"})
        loggers.config(level=10, logger="ext.metrics.metrics")
        monkeypatch.setattr(conf, "DATADOG_API_KEY", None, raising=True)
        monkeypatch.setattr(conf, "DATADOG_APP_KEY", None, raising=True)
        monkeypatch.setattr(conf, "DATADOG_ENABLED", True, raising=True)
        monkeypatch.setattr(metrics, "api_endpoint", api_endpoint, raising=True)

        load()
        post("test", 10)
        # captured = capfd.readouterr()
        # actual = captured.err.lower()
        # assert "success" in actual

    def test_post_failed(self, conf, monkeypatch, capfd, requests_mock):
        loggers.config(level=10, logger="ext.metrics.metrics")
        requests_mock.register_uri(ANY, ANY, json={"status": "ok"})

        monkeypatch.delattr(metrics, "to_tags", raising=True)

        load()
        post("test", 10)
        # captured = capfd.readouterr()
        # actual = captured.err.lower()
        # assert "failed" in actual

    def test_post_event_success(self, conf, monkeypatch, requests_mock):
        requests_mock.register_uri(ANY, ANY, json={"status": "ok"})
        monkeypatch.setattr(conf, "DATADOG_ENABLED", True, raising=True)
        monkeypatch.setattr(metrics, "api_endpoint", api_endpoint, raising=True)

        load()
        post_event("title", "text")

    def test_post_heartbeat(self, conf, monkeypatch, requests_mock):
        requests_mock.register_uri(ANY, ANY, json={"status": "ok"})
        monkeypatch.setattr(conf, "DATADOG_ENABLED", True, raising=True)
        monkeypatch.setattr(metrics, "api_endpoint", api_endpoint, raising=True)

        load()
        post_heartbeat()
