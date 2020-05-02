from db.models.bases import Base, db

__all__ = [
    "KnownEntity",
]


class KnownEntity(Base):
    __tablename__ = "known_entities"

    entity_id = db.Column(db.String(100), primary_key=True)  # ex. 42461409160000
    entity_type = db.Column(db.String(100), primary_key=True)  # ex. api14
    ihs_last_seen_at = db.Column(db.DateTime(timezone=True))
    enverus_last_seen_at = db.Column(db.DateTime(timezone=True))
    fracfocus_last_seen_at = db.Column(db.DateTime(timezone=True))
    ix_known_entities_all = db.Index(  # to avoid heap access
        "ix_known_entities_all",
        "entity_type",
        "entity_id",
        "ihs_last_seen_at",
        "enverus_last_seen_at",
        "fracfocus_last_seen_at",
    )
