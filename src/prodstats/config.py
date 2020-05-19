""" Global app configuration

    All configuration items are 12 Factor compliant, in that their values are inherited
    via a waterfall of definition locations. The value for a configuration item will be
    determined according to following order:
        1) system environment variables
        2) environment variables exposed through a .env file in the project root
        3) inline default value, if present
    If a value isnt found in one of the three locations, the configuration item will
    raise an error.
 """

from __future__ import annotations

import os
import socket
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import uvloop
from httpx import URL as HTTPUrl
from kombu import Queue
from starlette.config import Config
from starlette.datastructures import Secret

from schemas.database_url import DatabaseURL
from util.iterables import filter_by_prefix
from util.toml import project, version
from util.types import StringArray

# --- pandas ----------------------------------------------------------------- #

pd.options.display.max_rows = 1000
pd.set_option("display.float_format", lambda x: "%.2f" % x)
pd.set_option("large_repr", "truncate")
pd.set_option("precision", 2)

# --- uvloop ----------------------------------------------------------------- #

uvloop.install()

# --- general ---------------------------------------------------------------- #

conf: Config = Config(".env")

ENVIRONMENT_MAP: Dict[str, str] = {
    "production": "prod",
    "staging": "stage",
    "development": "dev",
}
ENV: str = conf("ENV", cast=str, default=socket.gethostname())
HOST_NAME: str = conf("HOST_NAME", cast=str, default=socket.gethostname())
TESTING: bool = conf("TESTING", cast=bool, default=False)
DEBUG: bool = conf("DEBUG", cast=bool, default=False)
EXTERNAL_CONFIG_BASE_PATH: Path = Path("./config").resolve()
SECRET_KEY: Secret = conf("SECRET_KEY", cast=Secret)

TASK_BATCH_SIZE: int = conf("PRODSTATS_TASK_BATCH_SIZE", cast=int, default=25)
# TASK_SPREAD_MULTIPLIER: int = conf(
#     "PRODSTATS_TASK_SPREAD_MULTIPLIER", cast=int, default=30
# )

COLLECTOR_CONFIG_PATH: Path = EXTERNAL_CONFIG_BASE_PATH / "collectors.yaml"  # TODO: remove
PARSER_CONFIG_PATH: Path = EXTERNAL_CONFIG_BASE_PATH / "parsers.yaml"  # TODO: remove


# SIX_HOURS = 6
# ONE_DAY = 24
# TWO_DAYS = 48
# THREE_DAYS = 72
# FOUR_DAYS = 96
# FIVE_DAYS = 120
# SIX_DAYS = 144
# ONE_WEEK = 168

PRODSTATS_H_COOLDOWN: int = conf("PRODSTATS_H_COOLDOWN", cast=int, default=48)  # hours
PRODSTATS_V_COOLDOWN: int = conf("PRODSTATS_V_COOLDOWN", cast=int, default=168)  # hours


# --- database --------------------------------------------------------------- #

DATABASE_DRIVER: str = conf("DATABASE_DRIVER", cast=str, default="postgresql+asyncpg")
DATABASE_USERNAME: str = conf("DATABASE_USERNAME", cast=str, default=None)
DATABASE_PASSWORD: Secret = conf("DATABASE_PASSWORD", cast=Secret, default=None)
DATABASE_HOST: str = conf("DATABASE_HOST", cast=str, default="localhost")
DATABASE_PORT: int = conf("DATABASE_PORT", cast=int, default=5432)
DATABASE_NAME: str = conf("DATABASE_NAME", cast=str)
DATABASE_POOL_SIZE_MIN: int = conf("DATABASE_POOL_SIZE_MIN", cast=int, default=1)
DATABASE_POOL_SIZE_MAX: int = conf(
    "DATABASE_POOL_SIZE_MIN", cast=int, default=DATABASE_POOL_SIZE_MIN
)
DATABASE_CONFIG: DatabaseURL = DatabaseURL(
    drivername=DATABASE_DRIVER,
    username=DATABASE_USERNAME,
    password=DATABASE_PASSWORD,
    host=DATABASE_HOST,
    port=DATABASE_PORT,
    database=DATABASE_NAME,
)

# --- alembic ---------------------------------------------------------------- #

# Currently only used to first initialize alembic in manage.py::db::init
# if script_location is later changed in alembic.ini, this would be misleading.
# FIXME: use this path if alembic.ini does not exist, else use script_location
MIGRATION_DIR: Path = Path("./src/prodstats/db/migrations").resolve()

ALEMBIC_CONFIG: DatabaseURL = DatabaseURL(
    drivername="postgresql+psycopg2",  # alembic will fail if using an async driver
    username=DATABASE_USERNAME,
    password=DATABASE_PASSWORD,
    host=DATABASE_HOST,
    port=DATABASE_PORT,
    database=DATABASE_NAME,
)

# --- logging ---------------------------------------------------------------- #

LOG_LEVEL: str = conf("LOG_LEVEL", cast=str, default="20")
LOG_FORMAT: str = conf("LOG_FORMAT", cast=str, default="json")
LOG_HANDLER: str = conf("LOG_HANDLER", cast=str, default="colorized")

# --- extensions ------------------------------------------------------------- #

DATADOG_ENABLED: bool = conf("DATADOG_ENABLED", cast=bool, default=False)
DATADOG_API_KEY: Optional[Secret] = conf(
    "DATADOG_API_KEY", cast=Secret, default=os.getenv("DD_API_KEY", None)
)
DATADOG_APP_KEY: Optional[Secret] = conf(
    "DATADOG_APP_KEY", cast=Secret, default=os.getenv("DD_APP_KEY", None),
)

DATADOG_DEFAULT_TAGS: Dict[str, str] = {
    "environment": ENVIRONMENT_MAP.get(ENV, ENV),
    "service_name": project,
    "service_version": version,
}

IHS_BASE_URL = conf("PRODSTATS_IHS_URL", cast=HTTPUrl)
FRACFOCUS_BASE_URL = conf("PRODSTATS_FRACFOCUS_URL", cast=HTTPUrl)


# --- accessors -------------------------------------------------------------- #


def items() -> Dict:
    """ Return all configuration items as a dictionary. Only items that are fully
        uppercased and do not begin with an underscore are included."""
    return {
        x: globals()[x]
        for x in globals().keys()
        if not x.startswith("_") and x.isupper()
    }


def with_prefix(prefix: str, tolower: bool = True, strip: bool = True) -> Dict:
    """ Return all configuration keys with the given prefix """
    return filter_by_prefix(items(), prefix, tolower=tolower, strip=strip)


# --- celery ----------------------------------------------------------------- #

CELERY_LOG_LEVEL: str = conf("CELERY_LOG_LEVEL", cast=str, default=LOG_LEVEL)
CELERY_LOG_FORMAT: str = conf("CELERY_LOG_FORMAT", cast=str, default=LOG_FORMAT)
CELERY_TASK_MAX_RETRIES: int = conf("CELERY_TASK_MAX_RETRIES", cast=int, default=5)
CELERY_TASK_EXP_BACKOFF: int = conf("CELERY_TASK_EXP_BACKOFF", cast=int, default=60)


class CeleryConfig:

    # --- custom ------------------------------------------------------------- #

    db_pool_min_size: int = conf("CELERY_DB_MIN_POOL_SIZE", cast=int, default=1)
    db_pool_max_size: int = conf(
        "CELERY_DB_MAX_POOL_SIZE", cast=int, default=db_pool_min_size
    )

    # --- broker ------------------------------------------------------------- #

    accept_content: List[str] = conf("", cast=StringArray, default=["json"])
    broker_url: str = conf("PRODSTATS_BROKER_URL", cast=str)
    broker_connection_timeout = 2
    broker_connection_max_retries = 1

    # --- beat --------------------------------------------------------------- #

    beat_scheduler = "redbeat.RedBeatScheduler"
    beat_max_loop_interval: int = conf("CELERY_MAX_LOOP_INTERVAL", cast=int, default=30)
    redbeat_redis_url: str = conf("PRODSTATS_CRON_URL", cast=str)
    redbeat_key_prefix: str = project
    redbeat_lock_timeout: int = conf(
        "REDBEAT_LOCK_TIMEOUT", cast=int, default=beat_max_loop_interval * 2
    )

    # --- task --------------------------------------------------------------- #

    task_track_started: bool = conf(
        "CELERY_TASK_TRACK_STARTED", cast=bool, default=False
    )
    task_always_eager: bool = conf("CELERY_TASK_ALWAYS_EAGER", cast=bool, default=False)
    task_time_limit: int = conf(
        "CELERY_TASK_TIME_LIMIT", cast=int, default=3600
    )  # seconds
    task_serializer: str = conf("CELERY_TASK_SERIALIZER", cast=str, default="json")
    task_default_queue: str = conf(
        "CELERY_DEFAULT_QUEUE", cast=str, default=f"{project}-default"
    )  # sqs default queue name
    task_create_missing_queues: bool = conf(
        "CELERY_TASK_CREATE_MISSING_QUEUES", cast=bool, default=False
    )  # set to True if not managing SQS queues in Terraform
    task_ignore_result: bool = conf(
        "CELERY_TASK_IGNORE_RESULT", cast=bool, default=True
    )
    task_store_errors_even_if_ignored: bool = conf(
        "CELERY_TASK_STORE_ERRORS", cast=bool, default=True
    )

    task_queues = (
        Queue(f"{project}-default", routing_key=f"{project}-default"),
        Queue(f"{project}-h", routing_key=f"{project}-h"),
        Queue(f"{project}-v", routing_key=f"{project}-v"),
    )
    task_routes: Optional[Tuple[str]] = conf(
        "CELERY_ROUTES", cast=tuple, default=("cq.routers.hole_direction_router",)
    )

    # --- worker ------------------------------------------------------------- #

    worker_max_tasks_per_child: int = conf(
        "CELERY_MAX_TASKS_PER_CHILD", cast=int, default=100
    )
    worker_max_memory_per_child: int = conf(
        "CELERY_MAX_MEMORY_PER_CHILD", cast=int, default=15000
    )  # 150mb
    worker_enable_remote_control: bool = conf(
        "CELERY_ENABLE_REMOTE_CONTROL", cast=bool, default=False
    )  # must be false for sqs
    worker_send_task_events: bool = conf(
        "CELERY_SEND_EVENTS", cast=bool, default=False
    )  # must be false for sqs
    worker_prefetch_multiplier: int = conf(
        "CELERY_PREFETCH_MULTIPLIER", cast=int, default=4
    )
    worker_concurrency: int = conf(
        "CELERY_CONCURRENCY", cast=int, default=None
    )  # celery default = number of CPUs (not hyperthreaded CPUs). can increase if tasks tend to be i/o bound # noqa

    # --- results ------------------------------------------------------------ #

    result_backend: str = conf(
        "CELERY_RESULT_BACKEND", cast=str, default=f"db+{ALEMBIC_CONFIG.url}"
    )

    result_expires: int = 86400 * 30  # seconds
    result_serializer: str = "json"
    result_extended: bool = conf("CELERY_RESULT_EXTENDED", cast=bool, default=True)
    # result_expires  # seconds

    # database_engine_options = {"echo": False}

    @classmethod
    def items(cls) -> Dict:
        """ Return all configuration items as a dictionary. Only items that are fully
            uppercased and do not begin with an underscore are included."""
        return {
            x: getattr(cls, x)
            for x in dir(cls)
            if not x.startswith("_") and x not in ["items"]
        }
