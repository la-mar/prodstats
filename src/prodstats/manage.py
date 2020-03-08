import logging
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List

import typer
from prodstats.main import app

import config as conf
import loggers

loggers.config()

logger = logging.getLogger()

CONTEXT_SETTINGS = dict(help_option_names=["-h", "--help"], ignore_unknown_options=True)


def get_terminal_columns():
    return shutil.get_terminal_size().columns  # pragma: no cover


def hr():
    return "-" * get_terminal_columns()


cli = typer.Typer(
    help="Prodstats Content Recommendation Service", context_settings=CONTEXT_SETTINGS
)
dev_cli = typer.Typer(help="Development tools")
cli.add_typer(dev_cli, name="dev")

run_cli = typer.Typer(help="Execution procedures")
cli.add_typer(run_cli, name="run")

db_cli = typer.Typer(help="Database Management")
cli.add_typer(db_cli, name="db")

test_cli = typer.Typer(help="Test Commands")
cli.add_typer(test_cli, name="test")

delete_cli = typer.Typer(help="Delete things")
cli.add_typer(delete_cli, name="delete")


@cli.command(help="List api routes")
def routes():
    for r in app.routes:
        typer.echo(f"{r.name:<25} {r.path:<30} {r.methods}")


# -----------------------------  subcommands  -------------------------------- #


@dev_cli.command(help="Launch a web process with hot reload enabled")
def server(port: int = 8000):
    cmd = ["uvicorn", "prodstats.main:app", "--reload", "--port", f"{port}"]
    if conf.TESTING:
        print(subprocess.Popen(cmd).pid)
    else:
        subprocess.call(cmd)  # nocover


@run_cli.command(help="Launch a web process to serve the api")
def web(port: int = 8000):
    cmd = ["uvicorn", "prodstats.main:app", "--port", str(port)]
    if conf.TESTING:
        print(subprocess.Popen(cmd).pid)
    else:
        subprocess.call(cmd)  # nocover


@run_cli.command(help="Invoke an asyncronous task from the command line")
def task(task: str):
    """Run a one-off task. Pass the name of the scoped task to run.
        Ex. endpoint_name.task_name"""

    try:
        typer.secho(f"{task=}")
    except ValueError:
        typer.secho("Invalid task format. Try specifying ENDPOINT_NAME.TASK_NAME")
        return 0


@test_cli.command()
def smoke_test(help="Execute a smoke test against a worker instance"):

    # TODO: implement
    logger.warning("verified")


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
def recreate(args: List[str] = None):
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
