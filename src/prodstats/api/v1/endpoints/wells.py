# import asyncio
# import logging
# from typing import List

# import starlette.status as codes
# from fastapi import APIRouter, Depends, HTTPException
# from starlette.responses import UJSONResponse

# from api.helpers import Pagination
# from db.models import GeocodingQueue
# from db.models import Place as ORMPlace
# from schemas import Place, PlaceCreateIn, PlaceUpdateIn

# logger = logging.getLogger(__name__)

# router = APIRouter()


# @router.post("/", status_code=codes.HTTP_202_ACCEPTED)
# async def create_places(places: List[PlaceCreateIn]):
#     """ Newly created places undergo a normalization process prior to creation
#         that takes between 1 and 5 minutes to complete. As a result, newly created
#         addresses are not immediately available.
#     """

#     # NOTE: if in the future we want to submit geocoding tasks in real time, we would
#     # add a call to schedule the job (to be executed immediately) on a celery worker.
#     await asyncio.gather(
#         *[GeocodingQueue.create(name=u.name, json=u.dict()) for u in places]
#     )


# @router.get("/", response_model=List[Place])
# async def list_places(
#     response: UJSONResponse, pagination: Pagination = Depends(Pagination)
# ):
#     """
#     Description Here
#     """
#     data, headers = await pagination.paginate_links(ORMPlace, serializer=Place)
#     response = pagination.set_headers(response, headers)
#     return data


# @router.get("/{id}", response_model=Place)
# async def retrieve_place(id: int):
#     """
#     Description Here
#     """
#     place: Place = await ORMPlace.query.where(ORMPlace.id == id).gino.one_or_none()
#     return place


# @router.put("/{id}", response_model=Place, status_code=codes.HTTP_200_OK)
# async def update_place(body: PlaceUpdateIn, id: int):
#     """
#     Description Here
#     """
#     place: Place = await ORMPlace.query.where(ORMPlace.id == id).gino.one_or_none()
#     if not place:
#         raise HTTPException(
#             status_code=codes.HTTP_404_NOT_FOUND, detail="place not found"
#         )

#     await place.update(**body.dict(exclude_unset=True)).apply()
#     return place


# @router.delete("/{id}", response_model=Place)
# async def delete_place(id: int):
#     place: ORMPlace = await ORMPlace.query.where(ORMPlace.id == id).gino.one_or_none()
#     if not place:
#         raise HTTPException(
#             status_code=codes.HTTP_404_NOT_FOUND, detail="place not found"
#         )

#     await place.delete()
#     return place
