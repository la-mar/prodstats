import loggers
import util
from cq.tasks import sync_area_manifest
from db import db

loggers.config(level=10)

util.aio.async_to_sync(db.startup())

sync_area_manifest.apply()
