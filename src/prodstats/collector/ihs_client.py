import asyncio
import logging
from typing import Any, Coroutine, Dict, List, Optional, Union

import httpx

import config as conf
import util
from collector import AsyncClient
from util.enums import Enum

logger = logging.getLogger(__name__)

__all__ = ["IHSPath", "IHSClient"]


class IHSPath(str, Enum):
    prod_h: str = "prod/h"
    prod_v: str = "prod/v"
    well_h: str = "well/h"
    well_v: str = "well/v"

    prod_h_ids: str = "prod/h/ids"
    prod_v_ids: str = "prod/v/ids"
    well_h_ids: str = "well/h/ids"
    well_v_ids: str = "well/v/ids"

    # def __repr__(self):
    #     d = {k: v for k, v in IHSPaths.__dict__.items() if not k.startswith("_")}
    #     return make_repr(d)


class IHSClient(AsyncClient):
    base_url: httpx.URL = conf.IHS_BASE_URL
    paths: Enum = IHSPath

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

    @staticmethod
    async def aiter(coros):
        for coro in coros:
            yield await coro

    @classmethod
    async def get_production_wells(
        cls,
        id: Union[str, List[str]],
        path: IHSPath,
        params: Dict = None,
        timeout: Optional[int] = None,
        concurrency: int = 50,
    ) -> List[Dict[str, Any]]:
        """Fetch production records from the internal IHS service.

        Arguments:
            id {Union[str, List[str]]} -- can be a single or list of producing entities or api10s

        Keyword Arguments:
            params {Dict} -- additional params to pass to client.get() (default: None)
            path {str} -- url resource bath (default: "prod/h")
            timeout {int} -- optional timeout period for production requests

        Returns:
            list -- list of monthly production records for the given ids
        """

        responses: List[httpx.Response] = []
        ids = util.ensure_list(id)
        async with cls() as client:
            coros: List[Coroutine] = []
            for id in ids:
                coro = client.get(
                    path.value,
                    params={
                        "id": ",".join(util.ensure_list(id)),
                        **{**(params or {})},
                    },
                    timeout=timeout,
                )
                coros.append(coro)

            for idx, chunk in enumerate(util.chunks(coros, concurrency)):
                responses += await asyncio.gather(*chunk)

        data: List[Dict[str, Any]] = []

        for r in responses:
            json: Dict = r.json()  # type: ignore
            data += json["data"]

        return data

    @classmethod
    async def get_ids(cls, area: str, path: IHSPath):
        async with cls() as client:
            response = await client.get(f"{path.value}/{area}")
            response.raise_for_status()
            return response.json()["data"][0]["ids"]


if __name__ == "__main__":

    import loggers
    import random

    loggers.config()

    async def async_wrapper():
        ids = ["14207C017575", "14207C020251"]
        # ids = ["14207C017575"]
        ids = [
            "14207C0155111H",
            "14207C0155258418H",
            "14207C0155258421H",
            "14207C01552617H",
            "14207C015535211H",
            "14207C015535212H",
            "14207C0155368001H",
            "14207C0155368002H",
            "14207C01558022H",
            "14207C0155809H",
            "14207C017575",
            "14207C020251",
        ]

        ids = await IHSClient.get_ids("tx-upton", path=IHSPath.prod_h_ids)
        ids = random.choices(ids, k=1)
        data = await IHSClient.get_production_wells(ids, path=IHSPath.prod_h)
        data
        # TODO: add indexes to entity12

        # async for r in IHSClient.iter_get(coros):
        #     print(r)
        # pass  # work on doc


# coros = [client.get(
#                         path.value,
#                         params={
#                             "id": ",".join(util.ensure_list(id)),
#                             **{**(params or {})},
#                         },
#                         timeout=timeout,
#                     ) for x in range(0, 10)]
