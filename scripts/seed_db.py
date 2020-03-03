import asyncio
from typing import List

from tests.utils import seed_model

from db import DATABASE_CONFIG, db


async def seed_models(models: List[str]):
    async with db.with_bind(DATABASE_CONFIG.url):
        for m in models:
            await seed_model(m, n=50)


def run():
    loop = asyncio.get_event_loop()
    loop.run_until_complete(seed_models())
    loop.close()
