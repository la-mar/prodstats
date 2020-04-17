# flake8: noqa
import logging

import gino

from config import DATABASE_CONFIG, DATABASE_POOL_SIZE_MAX, DATABASE_POOL_SIZE_MIN

logger = logging.getLogger(__name__)

db: gino.Gino = gino.Gino(
    naming_convention={  # passed to sqlalchemy.MetaData
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }
)


async def startup(
    pool_min_size: int = DATABASE_POOL_SIZE_MIN,
    pool_max_size: int = DATABASE_POOL_SIZE_MAX,
):  # nocover (implicitly tested with test client)
    if not db.is_bound():
        await db.set_bind(db.url, min_size=pool_min_size, max_size=pool_max_size)
        logger.debug(f"Connected to {db.url.__to_string__(hide_password=True)}")


async def shutdown():  # nocover (implicitly tested with test client)
    await db.pop_bind().close()
    logger.debug(f"Disconnected from {db.url.__to_string__(hide_password=True)}")


async def create_engine() -> gino.GinoEngine:
    return await gino.create_engine(db.url)
    logger.debug(f"Created engine for {db.url.__to_string__(hide_password=True)}")


def qsize():
    """ Get current number of connections """
    return db.bind.raw_pool._queue.qsize()


# set some properties for convenience
db.qsize, db.startup, db.shutdown, db.create_engine, db.url = (
    qsize,
    startup,
    shutdown,
    create_engine,
    DATABASE_CONFIG.url,
)
