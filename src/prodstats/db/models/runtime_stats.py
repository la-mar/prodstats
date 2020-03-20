from db.models.bases import Base, db

__all__ = ["RuntimeStat"]


class RuntimeStat(Base):
    __tablename__ = "runtime_stats"

    id = db.Column(db.BigInteger(), primary_key=True)
    hole_direction = db.Column(db.String(25))
    data_type = db.Column(db.String(25))
    operation = db.Column(db.String(50))
    name = db.Column(db.String(50))
    time = db.Column(db.Numeric(19, 2))
    count = db.Column(db.Integer())
