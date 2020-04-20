import asyncio
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
        entity12s: Union[str, List[str]] = None,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        timeout: Optional[int] = None,
        concurrency: int = None,
        related: bool = True,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """Fetch production records from the internal IHS service.

        Returns:
            list -- list of monthly production records
        """
        optcount = sum(
            [
                entities is not None,
                api14s is not None,
                api10s is not None,
                entity12s is not None,
            ]
        )
        if optcount < 1:
            raise ValueError(
                "One of ['entities', 'api14s', 'api10s', 'entitiy12s'] must be specified"
            )
        if optcount > 1:
            raise ValueError(
                "Only one of ['entities', 'api14s', 'api10s', 'entitiy12s'] can be specified"
            )

        if entities is not None:
            ids = entities
            param_name = "id"
        elif api14s is not None:
            ids = api14s
            param_name = "api14"
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
            params={"related": related, **(params or {})},
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
    async def get_sample(
        cls,
        path: IHSPath,
        n: int = None,
        frac: float = None,
        area: str = None,
        timeout: Optional[int] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:

        if path not in [
            IHSPath.well_h_sample,
            IHSPath.well_v_sample,
            IHSPath.prod_h_sample,
            IHSPath.prod_v_sample,
        ]:
            raise ValueError(f"'path' must be path to a sampling endpoint")

        params: Dict = {}

        if n is not None:
            params["n"] = n
        elif frac is not None:
            if frac > 0 and frac <= 1:
                params["frac"] = frac
            raise ValueError(
                f"invaid fraction supplied ({frac}): must be 0 < frac <= 1"
            )
        else:
            raise ValueError(f"One of 'n' or 'frac' must be specified")

        if area:
            params["area"] = area

        async with cls(**kwargs) as client:
            response = await client.get(path.value, params=params, timeout=timeout)

            json: Dict = response.json()  # type: ignore
            data: List[Dict[str, Any]] = []
            if "data" in json.keys():
                data += json["data"]

            return data

    @classmethod
    async def get_ids_by_area(cls, path: IHSPath, area: str, **kwargs) -> List[str]:
        async with cls(**kwargs) as client:
            response = await client.get(f"{path.value}/{area}")
            response.raise_for_status()
            return response.json()["data"][0].get("ids", [])

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

    loggers.config()

    async def async_wrapper():
        entity12s = ["14207C017575", "14207C020251"]
        entity12s
        entities = [
            "14207C0155111H",
            "14207C0155258418H",
            "14207C0155258421H",
            "14207C01552617H",
        ]
        entities

        #         well_ids = [
        #             "42461409160000",
        #             "42383406370000",
        #             "42461412100000",
        #             "42461412090000",
        #             "42461411750000",
        #             "42461411740000",
        #             "42461411730000",
        #             "42461411720000",
        #             "42461411600000",
        #             "42461411280000",
        #             "42461411270000",
        #             "42461411260000",
        #             "42383406650000",
        #             "42383406640000",
        #             "42383406400000",
        #             "42383406390000",
        #             "42383406380000",
        #             "42461412110000",
        #             "42383402790000",
        #         ]

        # await IHSClient.get_production(
        #     entity12s=entity12s, path=IHSPath.prod_h, params={"related": False}
        # )

        await IHSClient.get_sample(IHSPath.well_v_sample, n=10)
        await IHSClient.get_sample(IHSPath.prod_h_sample, n=5)
        #         # await IHSClient.get_areas(path=IHSPath.well_h_ids)
        #         # await IHSClient.get_ids_by_area(path=IHSPath.well_h_ids, area="tx-upton")

        wells = await IHSClient.get_wells(
            path=IHSPath.well_h, api14s=["42329389060000"]
        )
        wells


#         geoms = await IHSClient.get_wells(api14s=well_ids, path=IHSPath.well_h_geoms)

#         util.jsontools.to_json(wells[0:2], "tests/fixtures/ihs_wells.json")
#         util.jsontools.to_json(geoms[0:2], "tests/fixtures/ihs_well_shapes.json")
