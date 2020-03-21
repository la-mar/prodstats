""" Constants """
from util.enums import Enum

MCF_TO_BBL_FACTOR: int = 6


# ---Enums-------------------------------------------------------------------- #


class HoleDirection(str, Enum):
    h = "h"
    v = "v"


class EntityType(str, Enum):
    well = "well"
    prod = "prod"


class IHSPath(str, Enum):
    prod_h = "prod/h"
    prod_v = "prod/v"
    well_h = "well/h"
    well_v = "well/v"

    prod_h_ids = "prod/h/ids"
    prod_v_ids = "prod/v/ids"
    well_h_ids = "well/h/ids"
    well_v_ids = "well/v/ids"
