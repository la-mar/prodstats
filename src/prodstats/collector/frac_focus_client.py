import asyncio
import logging
from typing import Any, Coroutine, Dict, List, Optional, Union

import httpx

import config as conf
import util
from collector import AsyncClient
from const import Enum, FracFocusPath

logger = logging.getLogger(__name__)

__all__ = ["FracFocusPath", "FracFocusClient"]


class FracFocusClient(AsyncClient):
    base_url: httpx.URL = conf.FRACFOCUS_BASE_URL
    paths: Enum = FracFocusPath

    def __init__(
        self,
        base_url: Optional[Union[httpx.URL, str]] = None,
        headers: Optional[Union[httpx.Headers, Dict]] = None,
        params: Optional[Dict] = None,
        **kwargs,
    ):
        super().__init__(
            base_url=base_url or self.base_url, headers=headers, params=params, **kwargs
        )

    @classmethod
    async def get_jobs(
        cls,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        params: Dict = None,
        timeout: Optional[int] = None,
        concurrency: int = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:

        if api14s and api10s:
            raise ValueError("Exactly one of [api14s, api10s] can be specified")

        if api14s:
            path = FracFocusPath.api14
            ids = api14s
        elif api10s:
            path = FracFocusPath.api10
            ids = api10s
        else:
            raise ValueError("One of [api14s, api10s] must be specified")

        responses: List[httpx.Response] = []
        ids = util.ensure_list(ids)
        concurrency = concurrency or 50

        params = params or {}

        async with cls(**kwargs) as client:
            coros: List[Coroutine] = []
            for id in ids:
                coro = client.get(f"{path.value}/{id}", timeout=timeout,)
                coros.append(coro)

            for idx, chunk in enumerate(util.chunks(coros, concurrency)):
                responses += await asyncio.gather(*chunk)

        data: List[Dict[str, Any]] = []

        for r in responses:
            json: Dict = r.json()  # type: ignore
            if "data" in json.keys():
                data += json["data"]

        return data


if __name__ == "__main__":

    import loggers

    # import random

    loggers.config()

    api14s = [
        "42461409160000",
        "42383406370000",
        "42461412100000",
        "42461412090000",
        "42461411750000",
        "42461411740000",
        "42461411730000",
        "42461411720000",
        "42461411600000",
        "42461411280000",
        "42461411270000",
        "42461411260000",
        "42383406650000",
        "42383406640000",
        "42383406400000",
        "42383406390000",
        "42383406380000",
        "42461412110000",
        "42383402790000",
    ]

    async def async_wrapper():

        data = await FracFocusClient.get_jobs(ids=api14s, path=FracFocusPath.well)
        data
