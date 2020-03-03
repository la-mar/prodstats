import json
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, timedelta):
            return int(obj.total_seconds())
        else:
            return super().default(obj)


class ObjectEncoder(json.JSONEncoder):
    """Class to convert an object into JSON."""

    def default(self, obj: Any):
        """Convert `obj` to JSON."""

        if isinstance(obj, (int, float, str, list, dict, tuple)):
            return obj
        else:
            if hasattr(obj, "to_dict"):
                return self.default(obj.to_dict())
            if hasattr(obj, "dict"):
                return self.default(obj.dict())
            elif isinstance(obj, set):
                return list(obj)
            elif isinstance(obj, Path):
                return str(obj)
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


def to_string(data: Dict[Any, Any], pretty: bool = True) -> str:
    indent = 4 if pretty else 0
    return json.dumps(data, indent=indent, cls=UniversalEncoder)


def make_repr(data: Dict[Any, Any], pretty: bool = True) -> str:
    """wraps to_string to encapsulate repr specific edge cases """
    return to_string(data=data, pretty=pretty)
