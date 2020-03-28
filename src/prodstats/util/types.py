from typing import List, NewType, Union

import pandas as pd

StringArray = NewType("StringArray", List[str])
PandasObject = Union[pd.DataFrame, pd.Series]
