import logging
import os
import subprocess  # noqa

import psutil
import pytest  # noqa
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from tests.utils import get_open_port

import manage
from db import db
from util.context import working_directory

logger = logging.getLogger(__name__)


def autokill_subprocess(*args, delay: int = 3):
    p = psutil.Process(subprocess.Popen(args).pid)
    try:
        p.wait(timeout=delay)
    except psutil.TimeoutExpired:
        p.kill()


def autokill_pid(pid: int, delay: int = 3):
    p = psutil.Process(pid)
    try:
        p.wait(timeout=delay)
    except psutil.TimeoutExpired:
        p.kill()


class TestMisc:
    def test_import_as_module(self):
        """ simulate executing the module from the command line
            i.e. python -m prodstats """
        import prodstats.__main__  # noqa

    def test_version_match(self):
        from util.toml import version
        from __version__ import __version__

        assert __version__ == version


class TestCLIFast:
    def test_show_routes(self, capfd):  # TODO: needs validation
        manage.routes()
        captured = capfd.readouterr()
        logger.info(captured.out)


@pytest.mark.skipif(
    not os.getenv("CI"),
    reason="Skipping slow CLI tests. These are configured to only run on CI.",
)
class TestCLISlow:
    def test_run_web(self, capfd):
        manage.web(get_open_port())
        captured = capfd.readouterr()
        pid = int(captured.out.split("\n")[0])
        autokill_pid(pid, delay=2)
        captured = capfd.readouterr()
        logger.info(captured)
        assert "Uvicorn running" in captured.err

    def test_launch_dev_server(self, capfd):
        manage.server(get_open_port())
        captured = capfd.readouterr()
        pid = int(captured.out.split("\n")[0])
        autokill_pid(pid, delay=2)
        captured = capfd.readouterr()
        logger.info(captured)
        assert "Uvicorn running" in captured.err

    def test_run_cli(self, capfd):
        autokill_subprocess("prodstats", delay=3)
        captured = capfd.readouterr()

        commands = ["db", "delete", "dev", "run", "test"]
        for c in commands:
            assert c in captured.out

    def test_run_smoke_test(self, capfd):
        autokill_subprocess("prodstats", "test", "smoke-test", delay=1)
        # captured = capfd.readouterr()
        # assert "verified" in captured.out

    def test_print_hr(self):
        manage.hr()

    def test_run_task_execute(self):
        manage.task("test")

    def test_downgrades(self, sa_engine, capfd):

        config: Config = Config("alembic.ini")

        db.drop_all(sa_engine)
        manage.upgrade()
        captured = capfd.readouterr()

        config = Config("alembic.ini")
        script = ScriptDirectory.from_config(config)
        revisions = [
            r.down_revision
            for r in script.walk_revisions()
            if r.down_revision is not None
        ]

        command.downgrade(config, "head:base", sql=True)
        captured = capfd.readouterr()
        logger.error(captured.err)

        for rev in revisions:
            assert rev in captured.err

    def test_db_init(self, capfd, tmpdir):  # TODO: needs validation
        with working_directory(tmpdir):
            manage.init(str(tmpdir), "")
            captured = capfd.readouterr()

        for line in captured.out.split("\n"):
            logger.info(line)

    def test_db_upgrade(self, capfd, tmpdir, conf, sa_engine):

        config: Config = Config("alembic.ini")

        # db.drop_all(sa_engine)
        manage.downgrade("base")
        captured = capfd.readouterr()

        config = Config("alembic.ini")
        script = ScriptDirectory.from_config(config)
        revisions = [
            r.down_revision
            for r in script.walk_revisions()
            if r.down_revision is not None
        ]

        command.upgrade(config, "base:head", sql=True)
        captured = capfd.readouterr()
        logger.error(captured.err)

        for rev in revisions:
            assert rev in captured.err

    def test_db_migrate(self, capfd, tmpdir, conf, sa_engine):  # TODO: needs validation
        db.drop_all(sa_engine)

        with working_directory(tmpdir):
            manage.init("./migrations", "")
            manage.migrate("")
            captured = capfd.readouterr()

        for line in captured.out.split("\n"):
            logger.info(line)

    def test_db_recreate(
        self, capfd, tmpdir, conf, sa_engine
    ):  # TODO: needs validation
        db.drop_all(sa_engine)

        with working_directory(tmpdir):
            manage.recreate("")
            captured = capfd.readouterr()

        for line in captured.out.split("\n"):
            logger.info(line)
