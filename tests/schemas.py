from pydantic import BaseModel


class TestBase(BaseModel):
    class Config:
        allow_population_by_field_name = True
        extra = "allow"
        orm_mode = True

    id: int


class Test(TestBase):
    pass


class TestCreateIn(TestBase):
    pass


class TestCreateOut(TestBase):
    pass


class TestUpdateIn(TestBase):
    pass


class TestUpdateOut(TestBase):
    pass
