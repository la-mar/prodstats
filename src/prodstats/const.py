""" Constants """
from util.enums import Enum

MCF_TO_BBL_FACTOR: int = 6

LATERAL_DIP_THRESHOLD: int = 85

# ---Enums-------------------------------------------------------------------- #


class ProdStatRange(str, Enum):
    FIRST = "first"
    LAST = "last"
    PEAKNORM = "peaknorm"
    ALL = "all"


class HoleDirection(str, Enum):
    H = "H"
    V = "V"


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

    prod_h_geoms = "prod/h/geoms"
    prod_v_geoms = "prod/v/geoms"
    well_h_geoms = "well/h/geoms"
    well_v_geoms = "well/v/geoms"


class FracFocusPath(str, Enum):
    api10 = "api10"
    api14 = "api14"
