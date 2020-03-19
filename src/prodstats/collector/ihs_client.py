import asyncio
import itertools
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

    @classmethod
    async def get_production_wells(
        cls,
        path: IHSPath,
        params: Dict = None,
        entities: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        entity12s: Union[str, List[str]] = None,
        timeout: Optional[int] = None,
        concurrency: int = 50,
        **kwargs,
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
        optcount = sum(
            [entities is not None, api10s is not None, entity12s is not None]
        )
        if optcount < 1:
            raise ValueError(
                "One of ['entities', 'api10s', 'entitiy12s'] must be specified"
            )
        if optcount > 1:
            raise ValueError(
                "Only one of ['entities', 'api10s', 'entitiy12s'] can be specified"
            )

        if entities is not None:
            ids = entities
            param_name = "id"
        elif api10s is not None:
            ids = api10s
            param_name = "api10"

        elif entity12s is not None:
            ids = entity12s
            param_name = "entity12"

        responses: List[httpx.Response] = []
        ids = util.ensure_list(ids)

        params = params or {}

        async with cls(**kwargs) as client:
            coros: List[Coroutine] = []
            for id in ids:
                coro = client.get(
                    path.value, params={param_name: id, **params}, timeout=timeout,
                )
                coros.append(coro)

            for idx, chunk in enumerate(util.chunks(coros, concurrency)):
                responses += await asyncio.gather(*chunk)

        data: List[Dict[str, Any]] = []

        for r in responses:
            json: Dict = r.json()  # type: ignore
            if "data" in json.keys():
                data += json["data"]

        return data

    @classmethod
    async def get_ids_by_area(cls, area: str, path: IHSPath, **kwargs) -> List[str]:
        async with cls(**kwargs) as client:
            response = await client.get(f"{path.value}/{area}")
            response.raise_for_status()
            return response.json()["data"][0]["ids"]

    @classmethod
    async def get_all_ids(
        cls, path: IHSPath, areas: List[str] = None, **kwargs
    ) -> List[str]:
        areas = areas or await cls.get_areas(path, **kwargs)

        coros: List[Coroutine] = []
        for area in areas:
            coros.append(cls.get_ids_by_area(area, path=path))

        ids = await asyncio.gather(*coros)
        return list(itertools.chain(*ids))

    @classmethod
    async def get_areas(
        cls, path: IHSPath, name_only: bool = True, **kwargs
    ) -> List[str]:
        async with cls(**kwargs) as client:
            response = await client.get(f"{path.value}", params={"exclude": "ids"})
            response.raise_for_status()
            data = response.json()["data"]
            if name_only:
                data = [x["name"] for x in data]
            return data


if __name__ == "__main__":

    import loggers

    # import random

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

        # ids = await IHSClient.get_ids("tx-upton", path=IHSPath.prod_h_ids)
        # ids = random.choices(ids, k=1)
        await IHSClient.get_production_wells(
            entity12s=ids, path=IHSPath.prod_h, params={"related": False}
        )

        await IHSClient.get_areas(path=IHSPath.prod_h_ids)

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
