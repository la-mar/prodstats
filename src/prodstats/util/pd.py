import logging
from typing import List, Union

import pandas as pd

import util
from util.types import PandasObject


def validate_required_columns(required: List[str], columns: List[str]):
    missing = []
    for x in required:
        if x not in columns:
            missing.append(x)

    if len(missing) > 0:
        raise KeyError(f"Missing columns: {missing}")


logger = logging.getLogger(__name__)


@pd.api.extensions.register_dataframe_accessor("util")
class PDUtil:
    def __init__(self, obj: PandasObject):
        self._obj: PandasObject = obj

    def column_as_set(self, column_name: str) -> set:
        df = self._obj

        elements = set()
        if column_name in {*df.columns, *df.index.names} and df.shape[0] > 0:
            elements = {*df.reset_index()[column_name].values.tolist()}
        return elements

    def column_stats(self, columns: Union[str, List[str]]) -> pd.DataFrame:
        " "
        df = self._obj
        columns = util.ensure_list(columns)
        df_grouped = df.loc[:, columns].groupby(level=0)
        df_agg = (
            df_grouped.agg(["min", "max", "mean"])
            .unstack()
            .rename({"mean": "avg"})
            .reset_index()
            .rename(
                columns={
                    "level_0": "property_name",
                    "level_1": "aggregate_type",
                    0: "value",
                }
            )
        )

        df_agg = df_agg.append(
            df_grouped.quantile([0.25, 0.5, 0.75])
            .rename({0.25: "p25", 0.50: "p50", 0.75: "p75"})
            .reset_index()
            .rename(columns={"level_1": "aggregate_type"})
            .melt(id_vars=["api14", "aggregate_type"], var_name="property_name")
        )

        df_agg["name"] = df_agg.property_name + "_" + df_agg.aggregate_type
        return df_agg.set_index(
            ["api14", "property_name", "aggregate_type"]
        ).sort_index()
