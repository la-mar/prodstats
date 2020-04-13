""" Constants """
from util.enums import Enum

MCF_TO_BBL_FACTOR: int = 6
LATERAL_DIP_THRESHOLD: int = 85
PEAK_NORM_LIMIT: int = 6

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
    WELL = "well"
    PROD = "prod"


class Provider(str, Enum):
    IHS = "ihs"
    ENVERUS = "enverus"
    FRACFOCUS = "fracfocus"


class IHSPath(str, Enum):
    prod_h = "prod/h"
    prod_h_ids = "prod/h/ids"
    prod_h_geoms = "prod/h/geoms"
    prod_h_sample = "prod/h/sample"
    prod_h_headers = "prod/h/headers"

    well_h = "well/h"
    well_h_ids = "well/h/ids"
    well_h_geoms = "well/h/geoms"
    well_h_sample = "well/h/sample"
    well_h_headers = "well/h/headers"

    prod_v = "prod/v"
    prod_v_ids = "prod/v/ids"
    prod_v_geoms = "prod/v/geoms"
    prod_v_sample = "prod/v/sample"
    prod_v_headers = "prod/v/headers"

    well_v = "well/v"
    well_v_ids = "well/v/ids"
    well_v_geoms = "well/v/geoms"
    well_v_sample = "well/v/sample"
    well_v_headers = "well/v/headers"


class FracFocusPath(str, Enum):
    api10 = "api10"
    api14 = "api14"
