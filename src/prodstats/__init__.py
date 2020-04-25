import logging

from fastapi import FastAPI
from fastapi.responses import ORJSONResponse

import config as conf
import db

logger = logging.getLogger(__name__)


app: FastAPI = FastAPI(
    title=conf.project,
    version=conf.version,
    openapi_url="/api/v1/openapi.json",
    docs_url="/swagger",
    redoc_url="/docs",
    default_response_class=ORJSONResponse,
    debug=conf.DEBUG,
)


@app.on_event("startup")
async def startup():  # nocover (implicitly tested with test client)
    """ Event hook to run on web process startup """

    await db.startup()


@app.on_event("shutdown")
async def shutdown():  # nocover (implicitly tested with test client)
    """ Event hook to run on web process shutdown """

    await db.shutdown()
