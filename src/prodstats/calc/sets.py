from typing import Any, Dict, Tuple

import pandas as pd

import db.models


class SetItem:

    __slots__ = ("name", "model", "df")

    def __init__(self, name: str, model: db.models.Model, df: pd.DataFrame):
        self.name: str = name
        self.model: db.models.Model = model
        self.df: pd.DataFrame = df

    def __repr__(self):
        return f"SetItem: name={self.name} model={self.model} df={self.df.shape[0]}"

    def __iter__(self):
        for x in self.__slots__:
            yield getattr(self, x)


class DataSet:
    models: Dict[str, db.models.Model] = None  # type: ignore

    __data_slots__: Tuple = ("frame1", "frame2")

    def __init__(self, models: Dict[str, db.models.Model]):
        self.models: Dict[str, db.models.Model] = models

    def __repr__(self):
        s = " ".join([f"{k}={v}" for k, v in self.describe().items()])
        return s

    def __iter__(self):
        for x in self.__data_slots__:
            yield getattr(self, x, None)

    def describe(self) -> Dict[str, int]:
        result: Dict[str, Any] = {}
        for x in self.__data_slots__:

            value = getattr(self, x, pd.DataFrame())
            if value is not None:
                result[x] = value.shape[0]
            else:
                result[x] = 0

        return result

    def items(self):
        for x in self.__data_slots__:
            yield SetItem(
                name=x,
                model=self.models.get(x, None),
                df=getattr(self, x, pd.DataFrame()),
            )


class ProdSet(DataSet):

    __data_slots__: Tuple = ("header", "monthly", "stats")

    def __init__(
        self,
        header: pd.DataFrame = None,
        monthly: pd.DataFrame = None,
        stats: pd.DataFrame = None,
    ):

        super().__init__(
            models={
                "header": db.models.ProdHeader,
                "monthly": db.models.ProdMonthly,
                "stats": db.models.ProdStat,
            }
        )
        self.header = header
        self.monthly = monthly
        self.stats = stats


class WellSet:
    pass
