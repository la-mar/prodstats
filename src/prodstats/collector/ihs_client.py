import asyncio
import itertools
import logging
from typing import Any, Coroutine, Dict, List, Optional, Union

import httpx

import config as conf
import util
from collector import AsyncClient
from const import Enum, IHSPath

logger = logging.getLogger(__name__)

__all__ = ["IHSPath", "IHSClient"]


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
    async def _get(
        cls,
        ids: Union[str, List[str]],
        path: IHSPath,
        param_name: str,
        params: Dict = None,
        timeout: Optional[int] = None,
        concurrency: int = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        responses: List[httpx.Response] = []
        ids = util.ensure_list(ids)
        concurrency = concurrency or 50

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
    async def get_production(
        cls,
        path: IHSPath,
        params: Dict = None,
        entities: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        entity12s: Union[str, List[str]] = None,
        timeout: Optional[int] = None,
        concurrency: int = None,
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

        return await cls._get(
            ids=ids,
            path=path,
            param_name=param_name,
            params=params,
            timeout=timeout,
            concurrency=concurrency,
            **kwargs,
        )

    @classmethod
    async def get_wells(
        cls,
        path: IHSPath,
        params: Dict = None,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        timeout: Optional[int] = None,
        concurrency: int = 50,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        optcount = sum([api14s is not None, api10s is not None])
        if optcount < 1:
            raise ValueError("One of ['api14s', 'api10s'] must be specified")
        if optcount > 1:
            raise ValueError("Only one of ['api14s', 'api10s'] can be specified")

        if api14s is not None:
            ids = api14s
            param_name = "api14"
        elif api10s is not None:
            ids = api10s
            param_name = "api10"

        return await cls._get(
            ids=ids,
            path=path,
            param_name=param_name,
            params=params,
            timeout=timeout,
            concurrency=concurrency,
            **kwargs,
        )

    @classmethod
    async def get_ids_by_area(cls, path: IHSPath, area: str, **kwargs) -> List[str]:
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
            coros.append(cls.get_ids_by_area(path=path, area=area))

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
        prod_ids = ["14207C017575", "14207C020251"]
        prod_ids = [
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

        well_ids = [
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

        await IHSClient.get_production(
            entities=prod_ids, path=IHSPath.prod_h, params={"related": False}
        )

        await IHSClient.get_areas(path=IHSPath.well_h_ids)
        await IHSClient.get_ids_by_area(path=IHSPath.well_h_ids, area="tx-upton")

        await IHSClient.get_wells(api14s=well_ids, path=IHSPath.well_h_ids)
