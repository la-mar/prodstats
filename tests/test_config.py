import pytest  # noqa


class TestConfig:
    def test_show_config_attrs(self, conf):
        assert isinstance(conf.items(), dict)
        assert len(conf.items()) > 10  # arbitrary

    def test_get_collector_params(self, conf):
        assert isinstance(conf.with_prefix("collector"), dict)
