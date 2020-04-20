from typing import List, NewType, Union

import pandas as pd

StringArray = NewType("StringArray", List[str])
PandasObject = Union[pd.DataFrame, pd.Series]


def to_bool(value):
    valid = {
        "true": True,
        "t": True,
        "1": True,
        "yes": True,
        "no": False,
        "false": False,
        "f": False,
        "0": False,
    }

    if value is None:
        return False

    if isinstance(value, bool):
        return value

    if not isinstance(value, str):
        raise ValueError("invalid literal for boolean. Not a string.")

    lower_value = value.lower()
    if lower_value in valid:
        return valid[lower_value]
    else:
        raise ValueError('invalid literal for boolean: "%s"' % value)
