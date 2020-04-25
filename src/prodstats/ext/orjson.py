""" Starlette compatable response and middleware using orjson serializer """

from typing import Any, Callable

import orjson
from starlette.middleware.base import BaseHTTPMiddleware


async def parse_request_body(self) -> Any:  # nocover
    if not hasattr(self, "_json"):
        body = await self.body()
        self._json = orjson.loads(body)
    return self._json


def orjson_dumps(v: Any, *, default: Callable = None) -> str:
    return orjson.dumps(v, default=default, option=orjson.OPT_NAIVE_UTC).decode()


class ORJSONMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.json = parse_request_body
        return response
