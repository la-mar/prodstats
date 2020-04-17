import math
from typing import Union


def size_bytes(size: Union[str, int]) -> str:
    """Human friendly string representation of a size in bytes.

    Source: https://stackoverflow.com/questions/5194057/better-way-to-convert-file-sizes-in-python

    Arguments:
        size_bytes {Union[str, int]} -- size of object in number of bytes

    Returns:
        str -- string representation of object size. Ex: 299553704 -> "285.68 MB"
    """  # noqa
    if size == 0:
        return "0B"

    suffixes = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")

    if isinstance(size, str):
        size = int(size)

    i = int(math.floor(math.log(size, 1024)))
    p = math.pow(1024, i)
    s = round(size / p, 2)
    return f"{s} {suffixes[i]}"


def short_number(i: float, round: int = 0) -> str:
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
