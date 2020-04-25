import logging
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List

import click
import typer
from prodstats.main import app

import config as conf
import loggers

loggers.config()

logger = logging.getLogger()

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"], ignore_unknown_options=True)
CELERY_LOG_LEVEL_NAME: str = loggers.mlevelname(conf.CELERY_LOG_LEVEL)


def get_terminal_columns():
    return shutil.get_terminal_size().columns  # pragma: no cover


def hr():
    return "-" * get_terminal_columns()


# dev_cli = typer.Typer(help="Development tools")
db_cli = typer.Typer(help="Database Management")
test_cli = typer.Typer(help="Test Commands")
delete_cli = typer.Typer(help="Delete things")


# -----------------------------  subcommands  -------------------------------- #


@test_cli.command(help="Execute a smoke test against a worker instance")
def smoke_test():

    # TODO: implement
    logger.warning("verified")


@test_cli.command(help="Placeholder")
def unit_test():
    """ Unittest placeholder """
    # TODO: implement
    # logger.warning("verified")


@db_cli.command(help="Create a directory to manage database migrations")
def init(dir: Path = Path(conf.MIGRATION_DIR), args: List[str] = None):
    cmd = ["alembic", "init", str(dir)] + (args or [])
    subprocess.call(cmd)


@db_cli.command(help="Create a new migration revision")
def migrate(args: List[str] = None):
    cmd = ["alembic", "revision", "--autogenerate", "--head", "head"] + (args or [])
    subprocess.call(cmd)


@db_cli.command(help="Apply pending migrations to the database")
def upgrade(args: List[str] = None):
    cmd = ["alembic", "upgrade", "head"] + (args or [])
    subprocess.call(cmd)


@db_cli.command(help="Downgrade to a previous revision of the database")
def downgrade(revision: str = "-1", args: List[str] = None):
    cmd = ["alembic", "downgrade", revision] + (args or [])
    subprocess.call(cmd)


@db_cli.command(
    help="Drop the current database and rebuild using the existing migrations"
)
def recreate(args: List[str] = None):  # nocover

    if conf.ENV not in ["dev", "development"]:
        logger.error(
            f"""Cant recreate database when not in development mode. Set ENV=development as an environment variable to enable this feature."""  # noqa
        )
        sys.exit(0)

    from sqlalchemy_utils import create_database, database_exists, drop_database

    url = conf.ALEMBIC_CONFIG.url
    if database_exists(url):
        drop_database(url)
    create_database(url)
    upgrade()
    # rv = sqlalchemy.create_engine(url, echo=False)
    # db.drop_all(bind=rv)
    # db.create_all(bind=rv)
    logger.info(f"Recreated database at: {url}")
    # cmd = ["seed_db"]
    # subprocess.call(cmd)


# --- run -------------------------------------------------------------------- #

# run_cli = typer.Typer(help="Execution procedures")


# NOTE: typer doesn't yet support passing unknown options. The workaround below is
#       creating a click parent group and adding each typer group as a sub-group
#       of the click parent, then creating a click group to handle the commands
#       that need dynamic arguments.

cli = click.Group(
    help="Prodstats: Ingest, process, and analyze production and well data"
)
run_cli = click.Group("run", help="Execution procedures")


@run_cli.command(
    help="Launch a web process to serve the api",
    context_settings={"ignore_unknown_options": True},
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def web(args):
    cmd = ["uvicorn", "prodstats.main:app"] + list(args)
    subprocess.call(cmd)  # nocover


@run_cli.command(
    help="Launch a web process with hot reload enabled",
    context_settings={"ignore_unknown_options": True},
)
@click.argument("args", nargs=-1, type=click.UNPROCESSED)
def dev(args):
    cmd = ["uvicorn", "prodstats.main:app", "--reload"] + list(args)
    subprocess.call(cmd)


@run_cli.command(
    help="Launch a Celery worker", context_settings={"ignore_unknown_options": True}
)
@click.argument("celery_args", nargs=-1, type=click.UNPROCESSED)
def worker(celery_args):
    cmd = ["celery", "-A", "cq:celery_app", "worker"] + list(celery_args)
    subprocess.call(cmd)


@run_cli.command(
    help="Launch a Celery Beat scheduler",
    context_settings={"ignore_unknown_options": True},
)
@click.argument("celery_args", nargs=-1, type=click.UNPROCESSED)
def cron(celery_args):
    cmd = ["celery", "-A", "cq:celery_app", "beat"] + list(celery_args)
    subprocess.call(cmd)


@run_cli.command(
    help="Launch a monitoring process running flower",
    context_settings={"ignore_unknown_options": True},
)
@click.argument("celery_args", nargs=-1, type=click.UNPROCESSED)
def flower(celery_args):
    cmd = ["celery", "-A", "cq:celery_app", "flower"] + list(celery_args)
    subprocess.call(cmd)


@run_cli.command(help="Manually send a task to the worker cluster")
@click.argument("task")
def task(task: str):
    """Run a one-off task. Pass the name of the scoped task to run.
        Ex. endpoint_name.task_name"""
    # from cq.tasks import sync_endpoint

    try:
        if "." in task:
            typer.secho(f"{task=}")
        else:
            raise ValueError
    except ValueError:
        typer.secho("Invalid task format. Try specifying ENDPOINT_NAME.TASK_NAME")
        return 0


# --- top -------------------------------------------------------------------- #


@cli.command(help="List api routes")
def routes():
    for r in app.routes:
        typer.echo(f"{r.name:<25} {r.path:<30} {r.methods}")


# --- attach groups ---------------------------------------------------------- #


cli.add_command(run_cli)


cli.add_command(typer.main.get_command(db_cli), "db")
cli.add_command(typer.main.get_command(test_cli), "test")
# cli.add_command(typer.main.get_command(dev_cli), "dev")
# cli.add_command(typer.main.get_command(delete_cli), "delete")


def main(argv: List[str] = sys.argv):
    """
    Args:
        argv (list): List of arguments
    Returns:
        int: A return code
    Does stuff.
    """

    cli()


if __name__ == "__main__":
    cli()
