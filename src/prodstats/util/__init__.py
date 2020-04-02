import math
import urllib.parse
from pathlib import Path
from typing import Any, Callable, Dict, List, Union

import yaml

from util.iterables import chunks  # noqa
from util.jsontools import DateTimeEncoder, ObjectEncoder  # noqa
from util.locate import locate  # noqa
from util.strings import StringProcessor  # noqa


def ensure_list(value: Any) -> List[Any]:
    """ Ensure the passed value is a list-like object. """

    if isinstance(value, set):
        value = list(value)
    if not issubclass(type(value), list):
        return [value]
    return value


def reduce(values: List) -> Union[List[Any], Any]:
    """ Reduce a list to a scalar if length == 1 """
    while isinstance(values, list) and len(values) == 1:
        values = values[0]
    return values


def hf_size(size_bytes: Union[str, int]) -> str:
    """Human friendly string representation of a size in bytes.

    Source: https://stackoverflow.com/questions/5194057/better-way-to-convert-file-sizes-in-python

    Arguments:
        size_bytes {Union[str, int]} -- size of object in number of bytes

    Returns:
        str -- string representation of object size. Ex: 299553704 -> "285.68 MB"
    """  # noqa
    if size_bytes == 0:
        return "0B"

    suffixes = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")

    if isinstance(size_bytes, str):
        size_bytes = int(size_bytes)

    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {suffixes[i]}"


def hf_number(i: float, round: int = 0) -> str:
    sign = "" if i >= 0 else "-"
    i = float(abs(i))
    M = 1000000.0
    K = 1000.0
    div = 1.0
    divname = ""

    if i >= M:
        div = M
        divname = "M"
    elif i >= K:
        div = K
        divname = "K"
    return f"{sign}{i/div:.{round}f}{divname}"


def apply_transformation(
    data: dict, convert: Callable, keys: bool = False, values: bool = True
) -> Dict:
    """ Recursively apply an arbitrary function to a dictionary's keys, values, or both """
    if isinstance(data, (str, int, float)):
        if values:
            return convert(data)
        else:
            return data
    if isinstance(data, dict):
        new = data.__class__()
        for k, v in data.items():
            if keys:
                new[convert(k)] = apply_transformation(v, convert, keys, values)
            else:
                new[k] = apply_transformation(v, convert, keys, values)
    elif isinstance(data, (list, set, tuple)):
        new = data.__class__(
            apply_transformation(v, convert, keys, values) for v in data
        )
    else:
        return data
    return new


def safe_load_yaml(path: Union[Path, str]):
    try:
        with open(path) as f:
            return yaml.safe_load(f)
    except FileNotFoundError as fe:
        print(f"Failed to load configuration: {fe}")


def urljoin(base: str = "", path: str = "") -> str:
    base = base or ""
    path = path or ""
    if not base.endswith("/"):
        base = base + "/"
    if path.startswith("/"):
        path = path[1:]
    return urllib.parse.urljoin(base, path)
