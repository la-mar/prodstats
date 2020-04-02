# from datetime import date, datetime
# from typing import Any, Dict, List, Optional

# import pandas as pd
# import pytz
# from pydantic import Field, validator

# from schemas.bases import CustomBaseModel

# __all__ = ["FracFocus"]


# class FracFocusBase(CustomBaseModel):
#     class Config:
#         allow_population_by_field_name = True


# class FracFocusJob(FracFocusBase):
#     api10: str
#     fluid: Optional[int] = None
#     fluid_uom: Optional[str] = None
#     proppant: Optional[int] = None
#     proppant_uom: Optional[str] = None
#     provider: str
#     provider_last_update_at: datetime
