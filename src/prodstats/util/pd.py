from typing import List

import pandas as pd


def validate_required_columns(required: List[str], columns: List[str]):
    missing = []
    for x in required:
        if x not in columns:
            missing.append(x)

    if len(missing) > 0:
        raise KeyError(f"Missing columns: {missing}")


def column_as_set(df: pd.DataFrame, column_name: str) -> set:
    elements = set()
    if column_name in {*df.columns, *df.index.names} and df.shape[0] > 0:
        elements = {*df.reset_index()[column_name].values.tolist()}
    return elements
