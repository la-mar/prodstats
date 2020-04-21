import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Union

logger = logging.getLogger(__name__)

# try:
#     import shapely.geometry as geom
# except ImportError as e:
#     logger.warning(f"failed to load shapely -- {e}", stack_info=True)
#     geom = None


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return int(obj.total_seconds())
        else:
            return super().default(obj)


class ObjectEncoder(json.JSONEncoder):
    """Class to convert an object into json"""

    def default(self, obj: Any):
        """Convert `obj` to json"""

        if isinstance(obj, (int, float, str, list, dict, tuple, bool)):
            # return super().default(obj)
            return obj
        elif hasattr(obj, "to_dict"):
            return self.default(obj.to_dict())
        elif hasattr(obj, "dict"):
            return self.default(obj.dict())
        elif isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, Path):
            return str(obj)
        elif hasattr(obj, "__geo_interface__"):
            return self.default(obj.__geo_interface__)
        else:
            # generic fallback
            cls = type(obj)
            result = {
                "__custom__": True,
                "__module__": cls.__module__,
                "__name__": cls.__name__,
            }
            return result


class UniversalEncoder(DateTimeEncoder, ObjectEncoder):
    pass


def to_string(data: Union[List, Dict], pretty: bool = True) -> str:
    indent = 4 if pretty else 0
    return json.dumps(data, indent=indent, cls=UniversalEncoder)


def dumps(data: Union[List, Dict], pretty: bool = True) -> str:
    """ placeholder: alias for jsontools.to_string """
    return to_string(data, pretty)


def to_json(d: dict, path: Union[Path, str], cls=DateTimeEncoder):
    with open(path, "w") as f:
        json.dump(d, f, cls=cls, indent=4)


def load_json(path: Union[Path, str]):
    with open(path, "r") as f:
        return json.load(f)


def make_repr(data: Union[List, Dict], pretty: bool = True) -> str:
    """wraps to_string to encapsulate repr specific edge cases """
    return to_string(data=data, pretty=pretty)


if __name__ == "__main__":
    from util.dt import utcnow
    from pandas import Timestamp
    from shapely.geometry import asShape

    pt = {"type": "Point", "coordinates": [-102.15990376316262, 31.882545563762434]}
    line = {
        "type": "LineString",
        "coordinates": [
            [-102.1658373327804, 31.90677101457917],
            [-102.1658377151659, 31.906770789271725],
            [-102.16583857765673, 31.906770392266168],
            [-102.1658401362329, 31.906769325679146],
            [-102.16584303140229, 31.906765711084283],
        ],
    }

    obj = {
        "dt": datetime.now(),
        "utcnow": utcnow(),
        "ts": Timestamp.utcnow(),
        "pt": asShape(pt),
        "line": asShape(line),
    }
    json.dumps(obj, cls=UniversalEncoder)
