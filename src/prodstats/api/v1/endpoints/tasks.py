import logging
from typing import List, Set, Union

from fastapi import APIRouter

from cq import celery_app
from schemas.task import TaskOut

logger = logging.getLogger(__name__)

router = APIRouter()


# @router.post("/", status_code=codes.HTTP_202_ACCEPTED)
# async def create_tasks(items: List[ProdStatCreateIn]):
#     """
#     Description Here
#     """
#     await asyncio.gather(
#         *[ORMProdStat.create(api10=u.name, json=u.dict()) for u in items]
#     )


@router.get("/", response_model=Union[List[TaskOut], List[str]])
async def list_tasks(fields: Set[str] = None):
    """ Get a list of executable tasks """
    tasks = celery_app.describe_tasks()
    tasks = [TaskOut(**task).dict(include=fields) for task in tasks]
    return tasks


# @router.post("/{task}", status_code=codes.)


# @router.get("/{api10}", response_model=ProdStat)
# async def retrieve_task(api10: str):
#     """
#     Description Here
#     """
#     result: ORMProdStat = await ORMProdStat.query.where(
#         ORMProdStat.api10 == api10
#     ).gino.one_or_none()
#     return result


# @router.put("/{api10}", response_model=ProdStat, status_code=codes.HTTP_200_OK)
# async def update_task(body: ProdStatUpdateIn, api10: str):
#     """
#     Description Here
#     """
#     result: ORMProdStat = await ORMProdStat.query.where(
#         ORMProdStat.api10 == api10
#     ).gino.one_or_none()
#     if not result:
#         raise HTTPException(
#             status_code=codes.HTTP_404_NOT_FOUND, detail="ProdStat not found"
#         )

#     await ORMProdStat.update(**body.dict(exclude_unset=True)).apply()
#     return result  # TODO: return the updated result instead of the input


# @router.delete("/{api10}", response_model=ProdStat)
# async def delete_task(api10: str):
#     result: ORMProdStat = await ORMProdStat.query.where(
#         ORMProdStat.api10 == api10
#     ).gino.one_or_none()
#     if not result:
#         raise HTTPException(
#             status_code=codes.HTTP_404_NOT_FOUND, detail="ProdStat not found"
#         )

#     await result.delete()
#     return result


if __name__ == "__main__":
    from redbeat import schedulers
    import pytz
    from datetime import datetime
    from celery.utils.time import humanize_seconds

    # from cq import celery_app
    import pandas as pd

    redis = schedulers.get_redis(celery_app)
    conf = schedulers.RedBeatConfig(celery_app)
    keys = redis.zrange(conf.schedule_key, 0, -1)

    utcnow = pytz.utc.localize(datetime.utcnow())

    entries = [
        schedulers.RedBeatSchedulerEntry.from_key(key, app=celery_app) for key in keys
    ]
    # pprint(entries)
    entry_data = []
    for entry in entries:
        e = entry.__dict__
        e["due_at"] = entry.due_at
        e["due_in"] = humanize_seconds(
            (entry.due_at - utcnow).total_seconds(), prefix="in "
        )
        entry_data.append(e)

    df = pd.DataFrame(entry_data)

    # df = df.drop(columns=["app", "task", "args"])
    print(df)
