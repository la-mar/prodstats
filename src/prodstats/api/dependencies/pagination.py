import logging
from typing import Dict, List, Optional, Tuple, Union

from fastapi import Query
from pydantic import BaseModel as PydanticModel
from sqlalchemy.sql.elements import TextClause
from starlette.requests import Request
from starlette.responses import Response

from db import db
from db.models import Model

logger = logging.getLogger(__name__)

LINK_TEMPLATE = '<{url}>; rel="{rel}"'


class Pagination:
    """ Paging dependency for endpoints"""

    default_offset = 0
    default_limit = 25
    max_offset = None
    max_limit = 1000

    def __init__(
        self,
        request: Request,
        offset: int = Query(default=default_offset, ge=0, le=max_offset),
        limit: int = Query(default=default_limit, ge=-1, le=max_limit),
        filter: str = Query(default=None),
        # multiple: bool = Query(default=False),
        sort: str = Query(default=""),
        desc: bool = Query(default=True),
    ):
        self.request = request
        self.offset = offset
        self.limit = limit
        self.filter = filter
        self.sort = sort
        self.desc = desc
        self.sort_direction = "desc" if desc else "asc"
        self.model: Model = None

    async def count(self, filter: Optional[Union[str, TextClause]] = None) -> int:
        q = db.select([db.func.count([x for x in self.model.pk][0])])
        filter = filter if filter is not None else self.filter
        if filter is not None:
            if not isinstance(filter, TextClause):
                filter = db.text(filter)
            q = q.where(filter)
        return await q.gino.scalar() or 0

    def get_next_url(self, count: int) -> Optional[str]:
        if self.offset + self.limit >= count or self.limit <= 0:
            return None
        return str(
            self.request.url.include_query_params(
                limit=self.limit, offset=self.offset + self.limit
            )
        )

    def get_previous_url(self) -> Optional[str]:
        if self.offset <= 0:
            return None

        if self.offset - self.limit <= 0:
            return str(self.request.url.remove_query_params(keys=["offset"]))

        return str(
            self.request.url.include_query_params(
                limit=self.limit, offset=self.offset - self.limit
            )
        )

    async def get(
        self,
        filter: Optional[Union[str, TextClause]] = None,
        serializer: Optional[PydanticModel] = None,
    ) -> list:
        """ Build and execute the paged sql query, returning the results as a list of Pydantic
            model instances (if serializer is specified) or dicts (if serializer is NOT specified)
        """
        q = self.model.query
        filter = filter if filter is not None else self.filter
        if filter is not None:
            if not isinstance(filter, TextClause):
                filter = db.text(filter)
            q = q.where(filter)
        if self.limit > 0:
            q = q.limit(self.limit)
        if self.sort:
            q = q.order_by(db.text(f"{self.sort} {self.sort_direction}"))
        result = await q.offset(self.offset).gino.all()

        if serializer:
            return [serializer.from_orm(x) for x in result]
        else:
            return result

    async def paginate(
        self,
        model: Model,
        serializer: Optional[PydanticModel] = None,
        filter: Optional[str] = None,
    ) -> dict:
        self.model = model
        count = await self.count(filter)
        return {
            "count": count,
            "next": self.get_next_url(count),
            "prev": self.get_previous_url(),
            "data": await self.get(filter, serializer=serializer),
        }

    async def paginate_links(
        self,
        model: Model,
        serializer: Optional[PydanticModel] = None,
        filter: Optional[str] = None,
    ) -> Tuple[List[Union[Dict, PydanticModel]], Dict[str, Union[int, List]]]:
        p = await self.paginate(model=model, serializer=serializer, filter=filter)
        headers = {
            "x-total-count": p["count"],
            "link": [],
        }

        if p["prev"] is not None:
            headers["link"].append(LINK_TEMPLATE.format(url=p["prev"], rel="prev"))

        if p["next"] is not None:
            headers["link"].append(LINK_TEMPLATE.format(url=p["next"], rel="next"))

        return p["data"], headers

    def set_headers(self, response: Response, headers: Dict):
        response.headers["x-total-count"] = str(headers["x-total-count"])
        response.headers["link"] = ",".join(
            [x for x in headers["link"] if x is not None]
        )
        return response


# if __name__ == "__main__":
#     from db import db, DATABASE_CONFIG
#     from db.models import User
#     from schemas import User as UserSchema

#     r = Request(
#         {
#             "type": "http",
#             "path": "/api/v1/user",
#             "query_string": "",
#             "headers": {},
#             "method": "GET",
#             "server": "localhost:8000".split(":"),
#             "scheme": "http",
#         }
#     )
#     # r.url

#     # q = Query(default=0, ge=0, le=100)
#     # # dir(q).default
#     # self = Pagination(r, offset=0, limit=10, sort="created_at", desc=True, query="")

#     async def asyncwrapper():
#         await db.set_bind(DATABASE_CONFIG.url)
#         for x in range(1000, 1100):
#             await User.create(id=x)

#         result = await Pagination(
#             r, offset=0, limit=10, sort="created_at", desc=True, filter=""
#         ).paginate(User, serializer=UserSchema)

#         data, headers = await Pagination(
#             r,
#             offset=0,
#             limit=10,
#             sort="created_at",
#             desc=True,
#             filter="api10 = 'test'",
#         ).paginate_links(User, serializer=UserSchema)
#         # result["result"] = [x.to_dict() for x in result["result"]]
#         links = headers["link"]
#         str("".join(links))

#         len(result["data"])
#         await db.func.count(*User.pk).gino.scalar()
#         await db.func.count(*User.pk).gino.scalar()
#         await db.select([db.func.count(User.id)]).where(
#             db.text("id < 1010")
#         ).gino.scalar()
