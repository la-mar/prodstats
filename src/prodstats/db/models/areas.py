from db.models.bases import Base, db

__all__ = ["Area"]


class Area(Base):
    __tablename__ = "areas"

    path = db.Column(db.String(25), primary_key=True)
    area = db.Column(db.String(25), primary_key=True)
    last_sync = db.Column(db.DateTime(timezone=True))
