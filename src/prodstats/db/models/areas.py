from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd
import pytz

import config as conf
from const import HoleDirection
from db.models.bases import Base, db

logger = logging.getLogger(__name__)

__all__ = ["Area"]

SIX_HOURS = 6
ONE_DAY = 24
TWO_DAYS = 48
THREE_DAYS = 72
FOUR_DAYS = 96
FIVE_DAYS = 120
SIX_DAYS = 144
ONE_WEEK = 168


class Area(Base):
    __tablename__ = "areas"

    id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
    area = db.Column(db.String(25), unique=True, nullable=False)
    h_last_run_at = db.Column(db.DateTime(timezone=True))
    v_last_run_at = db.Column(db.DateTime(timezone=True))
    providers = db.Column(db.JSONB(), nullable=False, server_default="[]")
    # entity_type = db.Column(
    #     db.ChoiceType(EntityType, impl=db.String()), primary_key=True
    # )
    # hole_direction = db.Column(
    #     db.ChoiceType(HoleDirection, impl=db.String()), primary_key=True
    # )

    @classmethod
    def _is_ready(cls, last_run_at: Optional[datetime], cooldown_hours: int):
        if last_run_at:
            utcnow = datetime.now().astimezone(pytz.utc)
            threshold = utcnow - timedelta(hours=cooldown_hours)
            return last_run_at < threshold
        else:
            return True

    @classmethod
    async def next_available(
        cls, hole_dir: HoleDirection
    ) -> Tuple[Area, str, bool, int]:
        """ Get the properties describing the next available execution time of
         the given hole direction and entity type """

        if hole_dir == HoleDirection.H:
            cooldown = conf.PRODSTATS_H_COOLDOWN
        elif hole_dir == HoleDirection.V:
            cooldown = conf.PRODSTATS_V_COOLDOWN
        else:
            cooldown = ONE_WEEK

        attr = f"{hole_dir.value.lower()}_last_run_at"

        area_obj = await cls.query.order_by(  # get stalest area for given hole_dir
            getattr(cls, attr).asc().nullsfirst()
        ).gino.first()

        is_ready = cls._is_ready(getattr(area_obj, attr), cooldown)
        logger.info(
            f"({cls.__name__}[{hole_dir}]) next available: {area_obj.area} {is_ready=}"  # noqa
        )
        return area_obj, attr, is_ready, cooldown

    @classmethod
    async def df(cls) -> pd.DataFrame:
        records = await cls.query.gino.all()
        return pd.DataFrame(
            [x.to_dict() for x in records], columns=cls.c.names
        ).set_index("area")


if __name__ == "__main__":

    async def wrapper():
        from db import db
        from db.models import Area
        from const import HoleDirection

        cls = Area

        await db.startup()

        hole_dir = HoleDirection.H

        await cls.next_available(hole_dir)
