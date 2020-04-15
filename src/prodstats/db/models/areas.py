from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd
import pytz

from const import HoleDirection
from db.models.bases import Base, db

logger = logging.getLogger(__name__)

__all__ = ["Area"]

SIX_HOURS = 6
ONE_DAY = 24
TWO_DAYS = 48
THREE_DAYS = 72
ONE_WEEK = 168


class Area(Base):
    __tablename__ = "areas"

    area = db.Column(db.String(25), primary_key=True)
    # entity_type = db.Column(
    #     db.ChoiceType(EntityType, impl=db.String()), primary_key=True
    # )
    hole_direction = db.Column(
        db.ChoiceType(HoleDirection, impl=db.String()), primary_key=True
    )
    last_run_at = db.Column(db.DateTime(timezone=True))

    @classmethod
    def _is_ready(cls, last_run_at: Optional[datetime], cooldown_hours: int):
        if last_run_at:
            utcnow = datetime.now().astimezone(pytz.utc)
            threshold = utcnow - timedelta(hours=cooldown_hours)
            return last_run_at < threshold
        else:
            return True

    @classmethod
    async def next_available(cls, hole_dir: HoleDirection) -> Tuple[Area, bool]:
        """ Get the properties describing the next available execution time of
         the given hole direction and entity type """

        if hole_dir == HoleDirection.H:
            cooldown = ONE_DAY
        elif hole_dir == HoleDirection.V:
            cooldown = THREE_DAYS
        else:
            cooldown = ONE_WEEK

        area_obj = (  # get stalest area
            await cls.query.where(
                (cls.hole_direction == hole_dir)  # & (cls.entity_type == entity_type)
            )
            .order_by(cls.last_run_at.desc())
            .gino.first()
        )

        is_ready = cls._is_ready(area_obj.last_run_at, cooldown)
        logger.debug(
            f"({cls.__name__}) next available: {area_obj.area}[{area_obj.hole_direction.name}] {is_ready=}"  # noqa
        )
        return area_obj, is_ready

    @classmethod
    async def df(cls) -> pd.DataFrame:
        records = await cls.query.gino.all()
        return pd.DataFrame([x.to_dict() for x in records]).set_index(
            ["area", "hole_direction"]
        )


if __name__ == "__main__":

    async def wrapper():
        from db import db
        from db.models import Area

        await db.startup()

        cls = Area
        hole_dir = HoleDirection.H

        await cls.next_available(hole_dir)
