from schemas.bases import CustomBaseModel, ORMBase

__all__ = [
    "ProdStat",
    "ProdStatCreateIn",
    "ProdStatCreateOut",
    "ProdStatUpdateIn",
    "ProdStatUpdateOut",
]


class ProdStatBase(CustomBaseModel):
    class Config:
        allow_population_by_field_name = True
        extra = "allow"


class ProdStat(ORMBase, ProdStatBase):
    pass


class ProdStatCreateIn(ProdStatBase):
    pass


class ProdStatCreateOut(ProdStatBase):
    pass


class ProdStatUpdateIn(ProdStatBase):
    pass


class ProdStatUpdateOut(ProdStatBase):
    pass
