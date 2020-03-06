# flake8: noqa
# Native libraries
import logging
import sys
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

import loggers
from config import ALEMBIC_CONFIG
from db import db
from db.models import *  # noqa

sys.path.extend(["./"])


# To include a model in migrations, add a line here.

###############################################################################


config = context.config
config.set_main_option("sqlalchemy.url", str(ALEMBIC_CONFIG.url))
exclude_tables = config.get_section("alembic:exclude").get("tables", "").split(",")

fileConfig(config.config_file_name)
target_metadata = db

loggers.config(20, formatter="layman")
logger = logging.getLogger(__name__)


def include_object(object, name, type_, reflected, compare_to):  # nocover
    if type_ == "table" and name in exclude_tables:
        return False
    else:
        return True


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    context.configure(
        url=ALEMBIC_CONFIG.url.__to_string__(hide_password=False),
        target_metadata=target_metadata,
        literal_binds=True,
        include_object=include_object,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        {"sqlalchemy.url": ALEMBIC_CONFIG.url.__to_string__(hide_password=False)},
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    def process_revision_directives(context, revision, directives):  # nocover
        """ Dont generate a new migration file if there are no pending operations """
        if config.cmd_opts.autogenerate:
            script = directives[0]
            if script.upgrade_ops.is_empty():
                directives[:] = []
                logger.warning(
                    "No pending operations. Skipping creating an empty revision file."
                )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object,
            process_revision_directives=process_revision_directives,
        )

        with context.begin_transaction():
            context.execute("SET search_path TO public")
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
