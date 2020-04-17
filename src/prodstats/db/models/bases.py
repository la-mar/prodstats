from __future__ import annotations

from typing import Any, Dict, List, Union

from geoalchemy2 import Geometry
from gino.dialects.asyncpg import JSONB
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.schema import Constraint
from sqlalchemy_utils import ChoiceType, EmailType, URLType

import util.jsontools
from db import db
from db.mixins import DataFrameMixin
from util.deco import classproperty
from util.dt import utcnow

db.JSONB, db.UUID, db.EmailType, db.URLType, db.ChoiceType, db.Geometry = (
    JSONB,
    UUID,
    EmailType,
    URLType,
    ChoiceType,
    Geometry,
)


class Base(db.Model, DataFrameMixin):
    """ Data model base class """

    __abstract__ = True
    _columns: ColumnProxy = None  # type: ignore
    _agg: AggregateProxy = None  # type: ignore
    _pk: PrimaryKeyProxy = None  # type: ignore

    created_at = db.Column(
        db.DateTime(timezone=True), default=utcnow, server_default=db.func.now(),
    )
    updated_at = db.Column(
        db.DateTime(timezone=True),
        default=utcnow,
        onupdate=utcnow,
        server_default=db.func.now(),
    )

    def __repr__(self):
        return util.jsontools.make_repr(self)

    @classproperty
    def c(cls) -> ColumnProxy:
        return cls.columns

    @classproperty
    def columns(cls) -> ColumnProxy:
        if not cls._columns:
            cls._columns = ColumnProxy(cls)
        return cls._columns

    @classproperty
    def agg(cls) -> AggregateProxy:
        if not cls._agg:
            cls._agg = AggregateProxy(cls)
        return cls._agg

    @classproperty
    def pk(cls) -> PrimaryKeyProxy:
        if not cls._pk:
            cls._pk = PrimaryKeyProxy(cls)
        return cls._pk

    @classproperty
    def __model_name__(cls) -> str:
        return f"{cls.__module__}.{cls.__name__}"

    @classproperty
    def constraints(cls) -> Dict[str, Constraint]:
        return {x.name: x for x in list(cls.__table__.constraints)}


class ColumnProxy:
    """ Proxy object for a data model's columns """

    def __init__(self, model: Base):
        self.model = model

    def __iter__(self):
        for col in self.columns:
            yield col

    def __repr__(self):
        return util.jsontools.make_repr(self.names)

    # TODO: support indexing

    @property
    def columns(self) -> List[Column]:
        return list(self.model.__table__.c)

    @property
    def names(self) -> List[str]:
        return [x.name for x in self.columns]

    @property
    def pytypes(self) -> Dict[str, Any]:
        dtypes = {}
        for col in self.columns:
            dtypes[col.name] = col.type.python_type

        return dtypes

    @property
    def dtypes(self) -> Dict[str, Any]:
        dtypes = {}
        for col in self.columns:
            dtypes[col.name] = col.type

        return dtypes


class PrimaryKeyProxy(ColumnProxy):
    """ Proxy object for a data model's primary key attributes """

    @property
    def columns(self) -> List[Column]:
        return list(self.model.__table__.primary_key.columns)

    @property
    async def values(self) -> Union[List[Any]]:
        values = await self.model.select(*self.names).gino.all()
        if len(values) > 0:
            if len(values[0]) == 1:
                values = [v[0] for v in values]
        return values


class AggregateProxy:
    """ Proxy object for invoking aggregate queries against a model's underlying data """

    def __init__(self, model: Base):
        self.model: Base = model

    def __repr__(self):
        return f"AggregateProxy: {self.model.__module__}"

    async def count(self) -> int:
        return await db.func.count([x for x in self.model.pk][0]).gino.scalar()
