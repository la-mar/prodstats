# flake8: noqa isort:skip_file
from starlette.config import environ

environ["TESTING"] = "True"
environ["DATABASE_HOST"] = "localhost"
environ["DATABASE_NAME"] = "circle_test"
environ["LOG_LEVEL"] = "10"
environ["CELERY_LOG_LEVEL"] = "10"
environ["LOG_HANDLER"] = "stream"
environ["CELERY_BROKER_URL"] = "memory://"
environ["CELERY_RESULT_BACKEND"] = "redis://"
environ["PRODSTATS_CRON_URL"] = "redis://"
# environ["CELERY_TASK_ALWAYS_EAGER"] = "True"
environ["DATABASE_POOL_SIZE_MIN"] = "5"
environ["DATABASE_POOL_SIZE_MAX"] = "10"

import random
import string
import os

import gino
import pytest
import sqlalchemy
from sqlalchemy.engine import Engine
from async_asgi_testclient import TestClient
from async_generator import async_generator, yield_
from sqlalchemy_utils import create_database, database_exists, drop_database
import pandas as pd

import config
from db import db
from main import app

from tests.models import TestModel  # noqa
from tests.utils import MockAsyncDispatch
import util
from util.jsontools import load_json
import calc  # noqa

ECHO = False

import pytest

# * custom markers
pytest.mark.cionly = pytest.mark.skipif(
    not util.to_bool(os.getenv("CI")), reason="run on CI only",
)


@pytest.fixture(scope="session")
def celery_config():
    return config.CeleryConfig.items()


@pytest.fixture(scope="session")
def celery_worker_parameters():
    return config.CeleryConfig.items()


@pytest.fixture(scope="session")
def sa_engine():
    url = config.ALEMBIC_CONFIG.url

    if database_exists(url):
        drop_database(url)
    create_database(url)

    rv: Engine = sqlalchemy.create_engine(url, echo=ECHO)
    rv.execute("create extension if not exists postgis;")
    db.create_all(rv)  # create tables
    yield rv
    db.drop_all(rv)
    rv.dispose()


@pytest.fixture
@async_generator
async def engine(sa_engine):
    db.create_all(sa_engine)
    e = await gino.create_engine(config.DATABASE_CONFIG.url, echo=ECHO)
    await yield_(e)
    await e.close()
    db.drop_all(sa_engine)


@pytest.fixture
@async_generator
async def bind(sa_engine):
    db.create_all(sa_engine)
    async with db.with_bind(config.DATABASE_CONFIG.url, echo=ECHO) as e:
        await yield_(e)
    db.drop_all(sa_engine)


@pytest.fixture
def conf():
    yield config


@pytest.fixture
async def client(bind) -> TestClient:
    yield TestClient(app)


# --- json fixtures ---------------------------------------------------------- #


@pytest.fixture(scope="session")
def json_fixture():
    yield lambda x: load_json(f"tests/fixtures/{x}")


# --- horizontal --- #


@pytest.fixture(scope="session")
def wells_h(json_fixture):
    yield json_fixture("wells_h.json")


@pytest.fixture(scope="session")
def prod_h(json_fixture):
    yield json_fixture("prod_h.json")


@pytest.fixture(scope="session")
def prod_headers_h(json_fixture):
    yield json_fixture("prod_headers_h.json")


@pytest.fixture(scope="session")
def fracs_h(json_fixture):
    yield json_fixture("fracs_h.json")


@pytest.fixture(scope="session")
def geoms_h(json_fixture):
    yield json_fixture("geoms_h.json")


# --- vertical --- #


@pytest.fixture(scope="session")
def wells_v(json_fixture):
    yield json_fixture("wells_v.json")


@pytest.fixture(scope="session")
def prod_v(json_fixture):
    yield json_fixture("prod_v.json")


@pytest.fixture(scope="session")
def prod_headers_v(json_fixture):
    yield json_fixture("prod_headers_v.json")


@pytest.fixture(scope="session")
def fracs_v(json_fixture):
    yield json_fixture("fracs_v.json")


@pytest.fixture(scope="session")
def geoms_v(json_fixture):
    yield json_fixture("geoms_v.json")


# --- dataframes ------------------------------------------------------------- #


@pytest.fixture
def prod_df_h(prod_h):
    yield pd.DataFrame.prodstats.from_records(prod_h, create_index=True)


@pytest.fixture
def prod_df_v(prod_v):
    yield pd.DataFrame.prodstats.from_records(prod_v, create_index=True)


# --- sets ------------------------------------------------------------------- #

# --- horizontal --- #


@pytest.fixture
def wellset_h(wells_h):
    yield pd.DataFrame.wells.from_records(wells_h)


@pytest.fixture
def prodset_h(prod_df_h):
    yield prod_df_h.prodstats.to_prodset()


@pytest.fixture
def geomset_h(geoms_h):
    yield pd.DataFrame.shapes.from_records(geoms_h)


# --- vertical --- #


@pytest.fixture
def wellset_v(wells_v):
    yield pd.DataFrame.wells.from_records(wells_v)


@pytest.fixture
def prodset_v(prod_df_v):
    yield prod_df_v.prodstats.to_prodset()


@pytest.fixture
def geomset_v(geoms_v):
    yield pd.DataFrame.shapes.from_records(geoms_v)


# --- dispatchers ------------------------------------------------------------ #

# --- horizontal --- #


@pytest.fixture
def wells_h_dispatcher(wells_h):
    yield MockAsyncDispatch({"data": wells_h})


@pytest.fixture
def prod_h_dispatcher(prod_h):
    yield MockAsyncDispatch({"data": prod_h})


@pytest.fixture
def geoms_h_dispatcher(geoms_h):
    yield MockAsyncDispatch({"data": geoms_h})


# --- vertical --- #


@pytest.fixture
def wells_v_dispatcher(wells_v):
    yield MockAsyncDispatch({"data": wells_v})


@pytest.fixture
def prod_v_dispatcher(prod_v):
    yield MockAsyncDispatch({"data": prod_v})


@pytest.fixture
def geoms_v_dispatcher(geoms_v):
    yield MockAsyncDispatch({"data": geoms_v})


# --- other ------------------------------------------------------------------ #


@pytest.fixture
def random_string(length=8) -> str:
    return _random_string(length)


def _random_string(length=8):
    return "".join(random.choice(string.ascii_letters) for _ in range(length))


if __name__ == "__main__":
    from const import ProdStatRange

    json_fixture = lambda x: load_json(f"tests/fixtures/{x}")

    wells_h = json_fixture("wells_h.json")
    wellset_h = pd.DataFrame.wells.from_records(wells_h, create_index=True)

    fracs_h = json_fixture("fracs_h.json")
    fracs_v = json_fixture("fracs_v.json")

    geoms_h = json_fixture("geoms_h.json")
    geomset_h = pd.DataFrame.shapes.from_records(geoms_h, create_index=True)

    prod_h = json_fixture("prod_h.json")
    prod_headers_h = json_fixture("prod_headers_h.json")
    prod_df_h = pd.DataFrame.prodstats.from_records(prod_h, create_index=True)
    prodset_h = prod_df_h.prodstats.to_prodset()

    wells_v = json_fixture("wells_v.json")
    wellset_v = pd.DataFrame.wells.from_records(wells_v, create_index=True)

    geoms_v = json_fixture("geoms_v.json")
    geomset_v = pd.DataFrame.shapes.from_records(geoms_v, create_index=True)

    prod_v = json_fixture("prod_v.json")
    prod_headers_v = json_fixture("prod_headers_v.json")
    prod_df_v = pd.DataFrame.prodstats.from_records(prod_v, create_index=True)
    prodset_v = prod_df_v.prodstats.to_prodset()

    prod_df_v.loc[prod_df_v.entity12 == "142080037872"]

    range = ProdStatRange.FIRST
    months = 3
    api14h = [x["api14"] for x in wells_h]
    api14v = [x["api14"] for x in wells_v]
