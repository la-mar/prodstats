import asyncio
import logging
from typing import List

import starlette.status as codes
from fastapi import APIRouter, Depends, HTTPException
from starlette.responses import UJSONResponse

from api.helpers import Pagination
from db.models import ProdStat as ORMProdStat
from schemas import ProdStat, ProdStatCreateIn, ProdStatUpdateIn

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/", status_code=codes.HTTP_202_ACCEPTED)
async def create_ProdStats(items: List[ProdStatCreateIn]):
    """ Newly created prodstats undergo a normalization process prior to creation
        that takes between 1 and 5 minutes to complete. As a result, newly created
        addresses are not immediately available.
    """

    await asyncio.gather(
        *[ORMProdStat.create(api10=u.name, json=u.dict()) for u in items]
    )


@router.get("/", response_model=List[ProdStat])
async def list_prodstats(
    response: UJSONResponse, pagination: Pagination = Depends(Pagination)
):
    """
    Description Here
    """
    data, headers = await pagination.paginate_links(ORMProdStat, serializer=ProdStat)
    response = pagination.set_headers(response, headers)
    return data


@router.get("/{api10}", response_model=ProdStat)
async def retrieve_ProdStat(api10: str):
    """
    Description Here
    """
    result: ORMProdStat = await ORMProdStat.query.where(
        ORMProdStat.api10 == api10
    ).gino.one_or_none()
    return result


@router.put("/{api10}", response_model=ProdStat, status_code=codes.HTTP_200_OK)
async def update_ProdStat(body: ProdStatUpdateIn, api10: str):
    """
    Description Here
    """
    result: ORMProdStat = await ORMProdStat.query.where(
        ORMProdStat.api10 == api10
    ).gino.one_or_none()
    if not result:
        raise HTTPException(
            status_code=codes.HTTP_404_NOT_FOUND, detail="ProdStat not found"
        )

    await ORMProdStat.update(**body.dict(exclude_unset=True)).apply()
    return result  # TODO: return the updated result instead of the input


@router.delete("/{api10}", response_model=ProdStat)
async def delete_ProdStat(api10: str):
    result: ORMProdStat = await ORMProdStat.query.where(
        ORMProdStat.api10 == api10
    ).gino.one_or_none()
    if not result:
        raise HTTPException(
            status_code=codes.HTTP_404_NOT_FOUND, detail="ProdStat not found"
        )

    await result.delete()
    return result
