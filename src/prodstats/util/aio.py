import asyncio
from typing import Any, List, Optional

from util.iterables import reduce


def async_to_sync(*coros) -> Optional[List[Any]]:
    return reduce(asyncio.get_event_loop().run_until_complete(asyncio.gather(*coros)))
