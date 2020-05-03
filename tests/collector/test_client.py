import logging
import time
from multiprocessing import Process

import httpx
import pytest
import uvicorn
from fastapi import Depends, FastAPI
from starlette.responses import Response

import db
from api.helpers import Pagination
from collector import AsyncClient
from db.models import ProdStat as Model
from schemas.credentials import BasicAuth
from tests.utils import MockAsyncDispatch, get_open_port, rand_str

logger = logging.getLogger(__name__)


pytestmark = pytest.mark.asyncio


base_url = httpx.URL("http://127.0.0.1")


@pytest.fixture(scope="module")
def port():
    yield get_open_port()


@pytest.fixture
async def seed_model(bind):
    for x in range(0, 15):
        await Model.create(api10=rand_str(length=10), name=rand_str(length=20))


app = FastAPI()


@app.on_event("startup")
async def startup():
    await db.startup()


@app.on_event("shutdown")
async def shutdown():
    await db.shutdown()


@app.get("/no-content", status_code=204)
async def return_no_content():
    return None


@app.get("/paged/body/urls")
async def pager_urls(pagination: Pagination = Depends(Pagination), id: int = None):
    response = await pagination.paginate(Model)
    return response


@app.get("/paged/body/counts")
async def pager_counts(page: int, pagesize: int):
    data = [x.to_dict() for x in await Model.query.gino.all()]
    total_count = len(data)
    data = data[
        (page - 1) * pagesize : page * pagesize
    ]  # will break if pagesize doesnt divide evenly into total count
    response = {}
    response["page"] = page
    response["pagesize"] = pagesize
    response["count"] = total_count
    response["data"] = data  # type: ignore

    return response


@app.get("/paged/body/total_pages")
async def pager_total_pages(page: int, pagesize: int):
    data = [x.to_dict() for x in await Model.query.gino.all()]
    # total_count = len(data)
    data = data[
        (page - 1) * pagesize : page * pagesize
    ]  # will break if pagesize doesnt divide evenly into total count
    response = {}
    response["page"] = page
    response["pagesize"] = pagesize
    response["total_pages"] = 5
    response["data"] = data  # type: ignore

    return response


@app.get("/paged/header/links/")
async def pager_links(
    response: Response, pagination: Pagination = Depends(Pagination), id: int = None,
):
    data, headers = await pagination.paginate_links(Model)

    response.headers["x-total-count"] = str(headers["x-total-count"])
    response.headers["link"] = ",".join([x for x in headers["link"] if x is not None])
    return data


@pytest.fixture(scope="module")
def server():

    host = "127.0.0.1"
    port = get_open_port()
    process = Process(
        target=uvicorn.run,
        # args=(app,),
        args=("tests.collector.test_client:app",),
        kwargs={"host": host, "port": port, "log_level": "warning"},
        daemon=True,
    )
    process.start()
    time.sleep(3)

    yield httpx.URL(f"http://{host}:{port}")
    process.kill()


# async def test_server(server, seed_model):
#     with httpx.Client(base_url="http://127.0.0.1:8080") as client:
#         r = client.get("/paged/body/urls")
#         logger.warning(r.json())


# @pytest.mark.asyncio
# async def test_server(server):
#     async with httpx.AsyncClient(base_url="http://0.0.0.0:8080") as client:
#         r = await client.get("/pager")
#         print(r.json())


@pytest.fixture
def response_body():
    return [{f"{x}": x} for x in range(0, 10)]


@pytest.fixture
def requestor(response_body):
    mock_response = MockAsyncDispatch(response_body)
    yield AsyncClient(base_url=base_url, dispatch=mock_response)


class TestAsyncClient:
    async def test_requestor_repr(self, requestor):
        repr(requestor)

    async def test_requestor_get(self, requestor):
        response = await requestor.get("/users/")
        logger.warning(response.url)
        assert len(response.json()) == 10

    async def test_requestor_headers(self, requestor):
        key = rand_str(length=25)
        requestor.headers["X-API-KEY"] = key
        assert requestor.headers["x-api-key"] == key

    # TODO: figure out how to call test client from httpx/requestor client

    async def test_creds_from_schema_class(self):
        requestor = AsyncClient(credentials=BasicAuth)
        assert requestor.credentials == {"username": None, "password": None}

    async def test_creds_from_schema_instance(self):

        requestor = AsyncClient(credentials=BasicAuth())
        assert requestor.credentials == {"username": None, "password": None}

    async def test_creds_from_str(self):
        requestor = AsyncClient(credentials="schemas.credentials.BasicAuth")
        assert requestor.credentials == {"username": None, "password": None}

    async def test_creds_from_dict(self):
        requestor = AsyncClient(credentials={"username": None, "password": None})
        assert requestor.credentials == {"username": None, "password": None}


class TestPageIterators:
    async def test_iter_links(self, server, seed_model):
        requestor = AsyncClient(base_url=server)
        response_gen = requestor.iter_links("/paged/header/links?limit=3")

        responses = []
        async for r in response_gen:
            responses.append(r)

        actual_count = sum([len(r.json()) for r in responses])
        expected_count = int(responses[0].headers.get("x-total-count"))

        assert actual_count == expected_count

    # TODO: same problem
    async def test_iter_from_body_counts(self, server, seed_model):
        requestor = AsyncClient(base_url=server)
        responses = []
        async for r in requestor.iter_pages(
            "/paged/body/counts?page=1&pagesize=3",
            total_count_key="count",
            data_key="data",
        ):
            responses.append(r)

        actual_count = sum([len(r.json()["data"]) for r in responses])
        expected_count = 15

        assert actual_count == expected_count

    async def test_iter_from_body_urls(self, server, seed_model):
        requestor = AsyncClient(base_url=server)
        responses = []
        async for r in requestor.iter_pages(
            "/paged/body/urls?limit=3",
            data_key="data",
            next_url_key="next",
            total_count_key=None,
            page_key=None,
            total_pages_key=None,
        ):
            responses.append(r)

        actual_count = sum([len(r.json()["data"]) for r in responses])
        expected_count = int(responses[0].json().get("count"))

        assert actual_count == expected_count

    async def test_iter_from_body_total_pages(self, server, seed_model):
        requestor = AsyncClient(base_url=server)
        responses = []
        async for r in requestor.iter_pages(
            "/paged/body/total_pages?page=1&pagesize=3",
            total_pages_key="total_pages",
            data_key="data",
            page_key="page",
        ):
            responses.append(r)

        actual_count = sum([len(r.json()["data"]) for r in responses])
        expected_count = 15

        assert actual_count == expected_count

    async def test_iter_pages_with_extra_params(self, server, seed_model):
        requestor = AsyncClient(base_url=server)
        responses = []
        async for r in requestor.iter_pages(
            "/paged/body/counts",
            total_count_key="count",
            data_key="data",
            page_key="page",
            params={"pagesize": 3, "page": 1},
        ):
            responses.append(r)

        actual_count = sum([len(r.json()["data"]) for r in responses])
        expected_count = int(responses[0].json().get("count"))

        assert actual_count == expected_count

    async def test_iter_pages_no_content(self, server, seed_model):
        requestor = AsyncClient(base_url=server)
        responses = []
        async for r in requestor.iter_pages(
            "/no-content",
            total_count_key="count",
            data_key="data",
            params={"pagesize": 3},
        ):
            responses.append(r)
            # logger.warning(r.content)

        assert len(responses) == 1
        assert responses[0].status_code == 204
        assert responses[0].content == b""
