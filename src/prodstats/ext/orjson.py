""" Starlette compatable response and middleware using orjson serializer """

from typing import Any, Callable

import orjson
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response


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


# Specify multiple options, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_NAIVE_UTC
valid_args = {
    "default",
    "option",
}


class ORJSONResponse(Response):

    media_type = "application/json"

    def __init__(self, *args, **kwargs):
        self.render_args = {arg: v for arg, v in kwargs.items() if arg in valid_args}
        base_args = {arg: v for arg, v in kwargs.items() if arg not in valid_args}
        super(ORJSONResponse, self).__init__(*args, **base_args)

    def render(self, content: Any) -> bytes:
        return orjson.dumps(content, **self.render_args)
