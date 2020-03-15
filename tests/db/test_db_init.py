import gino
import pytest

from db import db


def test_custom_db_attrs():
    for x in ["startup", "shutdown", "create_engine", "qsize", "url"]:
        assert hasattr(db, x)


@pytest.mark.asyncio
async def test_create_engine():
    engine = await db.create_engine()
    assert isinstance(engine, gino.GinoEngine)


def test_qsize(bind):
    assert db.qsize() > 0
