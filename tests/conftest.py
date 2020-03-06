# flake8: noqa isort:skip_file
from starlette.config import environ

environ["TESTING"] = "True"
environ["DATABASE_HOST"] = "localhost"
environ["DATABASE_NAME"] = "circle_test"
environ["LOG_LEVEL"] = "10"
environ["LOG_HANDLER"] = "stream"

import random
import string

import gino
import pytest
import sqlalchemy
from sqlalchemy.engine import Engine
from async_asgi_testclient import TestClient
from async_generator import async_generator, yield_
from sqlalchemy_utils import create_database, database_exists, drop_database

import config
from db import db
from prodstats.main import app
from tests.models import TestModel  # noqa


ECHO = False


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


@pytest.fixture
def random_string(length=8) -> str:
    return _random_string(length)


def _random_string(length=8):
    return "".join(random.choice(string.ascii_letters) for _ in range(length))
