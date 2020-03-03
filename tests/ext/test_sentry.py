import logging

import ext.sentry
import loggers

logger = logging.getLogger(__name__)
logger.setLevel(10)

fake_dsn = "https://example98079y234563@sentry.io/3795155"

# TODO: Fix assertions


class TestSentry:
    def test_load_sentry_ext_enabled(self, conf, monkeypatch, capfd):
        loggers.config(level=10, logger="ext.sentry")
        monkeypatch.setattr(
            conf, "SENTRY_DSN", fake_dsn, raising=True,
        )
        monkeypatch.setattr(conf, "SENTRY_ENABLED", True, raising=True)

        ext.sentry.load()
        captured = capfd.readouterr()
        # expected = "sentry enabled".lower()
        # actual = captured.err.lower()
        logger.warning(captured)
        # assert expected in actual

    def test_load_sentry_ext_disabled(self, conf, monkeypatch, capfd):
        loggers.config(level=10, logger="ext.sentry")
        monkeypatch.setattr(
            conf, "SENTRY_DSN", fake_dsn, raising=True,
        )
        monkeypatch.setattr(conf, "SENTRY_ENABLED", False, raising=True)

        ext.sentry.load()
        captured = capfd.readouterr()
        # expected = "sentry disabled".lower()
        # actual = captured.err.lower()
        logger.warning(captured)
        # assert expected in actual

    def test_load_sentry_ext_disabled_empty_dsn(self, conf, monkeypatch, capfd):
        loggers.config(level=10, logger="ext.sentry")
        monkeypatch.setattr(
            conf, "SENTRY_DSN", "", raising=True,
        )
        monkeypatch.setattr(conf, "SENTRY_ENABLED", True, raising=True)

        ext.sentry.load()
        captured = capfd.readouterr()
        # expected = "sentry dsn is missing or empty".lower()
        # actual = captured.err.lower()
        logger.warning(captured)
        # assert expected in actual

    def test_load_sentry_ext_handle_error(self, conf, monkeypatch, capfd):
        loggers.config(level=10, logger="ext.sentry")
        monkeypatch.setattr(
            conf, "SENTRY_DSN", fake_dsn, raising=True,
        )
        monkeypatch.setattr(conf, "SENTRY_ENABLED", True, raising=True)
        monkeypatch.setattr(conf, "SENTRY_EVENT_LEVEL", Exception, raising=True)

        ext.sentry.load()
        captured = capfd.readouterr()
        # expected = "failed to load sentry".lower()
        # actual = captured.err.lower()
        logger.warning(captured)
        # assert expected in actual
