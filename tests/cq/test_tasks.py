import logging

import pytest

import cq.tasks
import db.models as models

logger = logging.getLogger(__name__)


@pytest.mark.usefixtures("celery_session_app")
@pytest.mark.usefixtures("celery_session_worker")
@pytest.mark.usefixtures("celery_enable_logging")
class TestCeleryTasks:
    def test_task(self, caplog):
        cq.tasks.log()

    def test_smoke_test_celery_app(self):
        assert "verified" == cq.tasks.smoke_test.apply().get()

    def test_post_heartbeat(self):
        cq.tasks.post_heartbeat()

    def test_sync_area_manifest(self, sa_engine):
        # import loggers

        # loggers.config(10)
        # results = sa_engine.execute("select * from areas").fetchall()
        # logger.warning(f"results: {results}")
        cq.tasks.sync_area_manifest.apply()


if __name__ == "__main__":
    from db import db

    async def async_wrapper():
        await db.startup()
        records = await models.Area.query.gino.all()
        logger.warning(records)
        # df = pd.DataFrame([x.to_dict() for x in records])
