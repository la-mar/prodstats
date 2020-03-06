from db.models.bases import Base, db

__all__ = ["TestModel"]


class TestModel(Base):
    __tablename__ = "test_model"

    id = db.Column(db.Integer(), index=True, primary_key=True)
    name = db.Column(db.String())
