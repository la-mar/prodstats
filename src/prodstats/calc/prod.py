from typing import List

import pandas as pd

PEAK_NORM_LIMIT: int = 12


@pd.api.extensions.register_dataframe_accessor("ps")
class ProdStatAccessor:
    def __init__(self, pandas_obj):
        self._validate(pandas_obj)
        self._obj = pandas_obj

    @staticmethod
    def _validate(obj):
        # verify there is a column latitude and a column longitude
        required: List[str] = ["oil", "gas", "water"]
        missing: List[str] = [x for x in obj.columns if obj.columns not in required]
        if len(missing) > 0:
            raise AttributeError("Must have colums: 'oil' and 'longitude'.")
