# import logging

# import pandas as pd
# import pytest
# import starlette.status as codes

# from db.models import ProdStat as Model

# from tests.utils import rand_str


# logger = logging.getLogger(__name__)

# pytestmark = pytest.mark.asyncio


# @pytest.fixture(scope="session")
# def prodstat_records(json_fixture):
#     yield json_fixture("prodstats.json")


# @pytest.fixture
# def prodstat_df(prodstat_records):
#     yield pd.DataFrame(prodstat_records).set_index(["api10", "prod_date"])


# @pytest.fixture(autouse=True)
# async def seed_prodstats(bind, prodstat_records):
#     await Model.bulk_insert(prodstat_records)


# class TestPlaceEndpoint:
#     path: str = "/api/v1/prodstats"

#     async def test_create_prodstat(self, client):
#         prodstat_name = "test"
#         response = await client.post(self.path, json=[{"name": prodstat_name}])
#         assert response.status_code == codes.HTTP_202_ACCEPTED

#     async def test_list_prodstats(self, client):
#         expected_record_count = 25
#         response = await client.get(self.path)
#         assert response.status_code == codes.HTTP_200_OK
#         data = response.json()
#         assert len(data) == expected_record_count
#         assert response.links["next"] is not None

#     async def test_get_prodstat(self, client):
#         id = 20
#         response = await client.get(f"{self.path}/{id}")
#         assert response.status_code == codes.HTTP_200_OK
#         data = response.json()
#         assert data["id"] == 20

#     async def test_update_exising_prodstat(self, client):
#         id = 10
#         value = rand_str(length=8)
#         response = await client.put(f"{self.path}/{id}", json={"state": value})
#         assert response.status_code == codes.HTTP_200_OK
#         data = response.json()
#         assert data["id"] == id
#         assert data["state"] == value

#     async def test_update_prodstat_not_found(self, client):
#         id = 99999
#         value = rand_str(length=8)
#         response = await client.put(f"{self.path}/{id}", json={"state": value})
#         assert response.status_code == codes.HTTP_404_NOT_FOUND

#     async def test_delete_existing_prodstat(self, client):
#         id = 20
#         response = await client.delete(f"{self.path}/{id}")
#         assert response.status_code == codes.HTTP_200_OK
#         data = response.json()
#         assert data["id"] == id

#     async def test_delete_prodstat_not_found(self, client):
#         id = 99999
#         response = await client.delete(f"{self.path}/{id}")
#         assert response.status_code == codes.HTTP_404_NOT_FOUND
#         data = response.json()
#         assert data["detail"] == "prodstat not found"
