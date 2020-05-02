import logging

import pytest
import starlette.status as codes

from schemas.task import TaskOut

logger = logging.getLogger(__name__)  # noqa

pytestmark = pytest.mark.asyncio


@pytest.fixture(scope="session")
def task_records(json_fixture):
    yield json_fixture("task_list.json")


class TestTaskEndpoint:
    path: str = "/api/v1/tasks/"

    async def test_list_tasks(self, client):

        response = await client.get(self.path)
        assert response.status_code == codes.HTTP_200_OK
        data = response.json()
        assert isinstance(data, list)

        expected_keys = set(TaskOut.__fields__.keys())
        for task in data:
            assert isinstance(task, dict)
            assert set(task.keys()) == expected_keys

    # async def test_get_prodstat(self, client):
    #     id = 20
    #     response = await client.get(f"{self.path}/{id}")
    #     assert response.status_code == codes.HTTP_200_OK
    #     data = response.json()
    #     assert data["id"] == 20


# from api.v1 import api_router

# dir(api_router)

# routes = api_router.routes
# route = routes[1]
# dir(route)
# route.path
# route.name
# route.methods
