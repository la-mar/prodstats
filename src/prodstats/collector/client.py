from __future__ import annotations

import copy
import inspect
import logging
from typing import Dict, Optional, Union

import httpx
import orjson
from async_generator import async_generator, yield_

import util
from schemas.credentials import HTTPAuth

logger = logging.getLogger(__name__)


class AsyncClient(httpx.AsyncClient):
    """ Extend the httpx.AsyncClient to encapsulate additional behavior needed
        for bulk sourcing data from external systems """

    _credentials = None

    def __init__(
        self,
        base_url: Optional[Union[httpx.URL, str]] = None,
        headers: Optional[Union[httpx.Headers, Dict]] = None,
        params: Optional[Dict] = None,
        credentials: Optional[Union[HTTPAuth, Dict, str]] = None,
        auth_url: Optional[Union[str, httpx.URL]] = None,
        **kwargs,
    ):

        super().__init__(
            base_url=httpx.URL(base_url or "http://127.0.0.1"),
            headers=headers,
            params=params,
            http2=True,
            **kwargs,
        )
        self.credentials = credentials
        self.auth_url: Optional[httpx.URL] = httpx.URL(
            auth_url
        ) if auth_url is not None else None

    def __repr__(self):
        return f"<Requestor: {self.base_url} headers={len(self.headers)} params={len(self.params)}>"

    @async_generator
    async def iter_links(
        self, url_or_path: Union[httpx.URL, str], ref: str = "next", **kwargs
    ):
        """Get a generator to iterate the link headers under the specified relation,
        if present.

        Keyword Arguments:
            ref {str} -- reference name of the link to use (default: "next")
            kwargs {dict} -- all unnamed kwargs are passed on to build the get request

        Returns:
            Generator[httpx.Response] -- generator yielding a response for each
                request made to a paged resource
        """

        while True:
            response = await self.get(url_or_path, **kwargs)
            await yield_(response)
            url_or_path = response.links.get(ref, {}).get("url")  # type: ignore
            if not url_or_path:
                break

    @async_generator
    async def iter_pages(
        self,
        url_or_path: Union[httpx.URL, str],
        data_key: str,
        page_key: str = "Page",
        total_count_key: str = "TotalCount",
        total_pages_key: str = "TotalPages",
        next_url_key: str = None,
        **kwargs,
    ):
        """Get a generator to iterate a paged endpoint using paging information gathered
        from the json response body.

        The search priority when collecting the paging information is as follows:
            if next_url_key is found -> process with urls
            if total_count is found -> calculate total pages and iter until complete
            if total_pages is found -> iter until page > total pages
            else -> iter until empty response is received

        Arguments:
            url_or_path {Union[httpx.URL, str]} -- absolute or relative url

        Keyword Arguments:
            page_key {str} -- key containing the current page number
                (default: {"Page"})
            total_count_key {str} -- key containing the total record count
                (default: {"TotalCount"})
            total_pages_key {str} -- key containing the total number of pages
                (default: {"TotalPages"})
            data_key {str} -- key containing the requested data (not metadata)
                (default: {None})
            next_url_key {str} -- key containing the absolute or relative url of the next page
                (default: {None})

        Returns:
            Generator[httpx.Response] -- generator yielding a response for each
                request made to a paged resource
        """
        params = copy.deepcopy(self.params)
        passed_params = kwargs.pop("params", None)
        if passed_params:
            params.update(passed_params)

        total_count: Optional[int] = None
        remaining_count: Optional[int] = None
        has_remaining_count = False

        total_pages: Optional[int] = None
        remaining_pages: Optional[int] = None
        has_remaining_pages = False

        while True:
            # logger.warning(f"getting {url_or_path}, params={params}")
            response = await self.get(url_or_path, params=params, **kwargs)
            if response.content:
                response.json = lambda: orjson.loads(response.content)  # type: ignore
                data = response.json()
            else:
                data = {}

            if isinstance(data, dict):
                page = data.get(page_key)
                if page:
                    params[page_key] = int(page) + 1  # type: ignore

                total_count = data.get(total_count_key)
                total_pages = data.get(total_pages_key)

                next = data.get(next_url_key)
                has_next_url = False

                # if data_key:
                record_count = len(data.get(data_key, {}))
                # else:
                #     record_count = len(data)
                # has_records = record_count > 0

                if total_count:
                    if not remaining_count:
                        remaining_count = total_count - record_count
                    else:
                        remaining_count = remaining_count - record_count

                    if remaining_count is not None:
                        has_remaining_count = remaining_count > 0

                elif total_pages:
                    if not remaining_pages:
                        remaining_pages = total_pages - 1
                    else:
                        remaining_pages = remaining_pages - 1

                    if remaining_pages is not None:
                        has_remaining_pages = remaining_pages > 0

                elif next is not None:
                    url_or_path = next  # client will handle absolute or relative
                    has_next_url = True

            logger.warning(
                f"{response.url} - {record_count=}, {total_count=}, {total_pages=}, {next=}, {remaining_count=}, {remaining_pages=}"  # noqa
            )
            await yield_(response)

            # logger.warning(
            #     f"{has_remaining_count=}, {has_remaining_pages=}, {has_next_url=}, {has_records=}"
            # )
            if not any([has_remaining_count, has_remaining_pages, has_next_url]):
                break

            # if not has_records:
            #     break

    @property
    def credentials(self):
        """ Get the credentials used for authentication """
        return self._credentials

    @credentials.setter
    def credentials(self, credentials: Union[HTTPAuth, Dict, str]):
        """ Set the auth credentials using a pydantic credential model or the dotted
            import path to such a model """

        if credentials:
            if isinstance(credentials, str):
                credentials = util.locate(credentials)

            if inspect.isclass(credentials):
                credentials = credentials()

            if not isinstance(credentials, dict):

                if isinstance(credentials, HTTPAuth):
                    credentials = credentials.dict(reveal=True)

            self._credentials = credentials

    async def post(self, *args, **kwargs) -> httpx.Response:
        response = await super().post(*args, **kwargs)
        if response.content:
            response.json = lambda: orjson.loads(response.content)  # type: ignore
        return response


if __name__ == "__main__":
    pass
    import config as conf
    import asyncio
    import random

    async def async_wrapper():

        deo_ids = [
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

        client = AsyncClient(base_url=conf.IHS_BASE_URL)

        resp_ids = await client.get("well/h/ids/tx-upton")
        resp_ids.json()

        # params = {"api14": ",".join(random.sample(deo_ids, 3))}

        async def get(client):
            return await client.post(
                "well/h",
                params={"api14": ",".join(random.sample(deo_ids, 3))},
                timeout=None,
            )

        result = None
        async with AsyncClient(base_url=conf.IHS_BASE_URL) as client:
            coros = [
                client.get(
                    "well/h",
                    params={"api14": ",".join(random.sample(deo_ids, 3))},
                    timeout=None,
                )
                for x in range(0, 5)
            ]
            result = await asyncio.gather(*coros)
            print([len(x.json()) for x in result])

        data = [x.json() for x in result]
        [x["data"]["ip"] for x in data]
        data[0]["data"][0]
