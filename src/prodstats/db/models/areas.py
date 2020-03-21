from const import EntityType, HoleDirection, IHSPath
from db.models.bases import Base, db

__all__ = ["Area"]


class Area(Base):
    __tablename__ = "areas"

    area = db.Column(db.String(25), primary_key=True)
    type = db.Column(db.ChoiceType(EntityType, impl=db.String()), primary_key=True)
    hole_direction = db.Column(
        db.ChoiceType(HoleDirection, impl=db.String()), primary_key=True
    )
    path = db.Column(db.ChoiceType(IHSPath, impl=db.String()))
    last_sync = db.Column(db.DateTime(timezone=True))
