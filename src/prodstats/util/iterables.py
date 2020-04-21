import functools
import hashlib
import itertools
from collections import OrderedDict
from typing import Any, Callable, Dict, Generator, Iterable, List, Tuple, Union


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def make_hash(data: Union[List, OrderedDict]) -> str:
    return hashlib.md5(str(data).encode()).hexdigest()


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


def chunks(iterable: Iterable, n: int = 1000, cls=list) -> Generator:
    """ Process an infinitely nested interable in chunks of size n (default=1000) """
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield cls(itertools.chain((first_el,), chunk_it))


def query(path: str, data: dict, sep: str = "."):
    """ Query any combination of nested lists/dicts, returning the last valid value
        encountered in the query chain.

        Example:
            data = {"a": {"b": {"c": 1}}}

            query(path="a.b.c", data=data) => 1

            query(path="a.b", data=data) => {"c": 1}

         """
    elements = path.split(sep)
    for e in elements:
        if issubclass(type(data), list) and len(data) > 0:
            try:
                data = data[int(e)]  # TODO: this needs to be smarter
            except ValueError:
                # handle cases where a list of items occurs where a mapping should be
                data = data[-1]
        elif issubclass(type(data), dict):
            data = data.get(e, {})

    return data if data != {} else None


def query_factory(data: str, sep: str = "."):
    return functools.partial(query, data=data, sep=sep)


def filter_by_prefix(
    d: Dict, prefix: str, tolower: bool = True, strip: bool = True
) -> Dict:
    """ Return all items with keys that begin with the given prefix.

        Example: prefix = "collector"

            Returns:
                {
                    "base_url": "example.com/api",
                    "path": "path/to/data",
                    "endpoints": {...}
                }
    """

    if tolower:
        cast = str.lower
    else:
        cast = str.upper

    if not prefix.endswith("_"):
        prefix = prefix + "_"
    prefix = cast(prefix)

    result: Dict = {}

    for key, value in d.items():
        key = cast(key)
        if key.startswith(prefix):
            if strip:
                key = key.replace(prefix, "")
            result[key] = value

    return result


def distinct_by_key(data: List[Dict], key: str) -> List:
    """ Return distinct records from a list of dictionaries, as determined by the
        provided key. """

    result = {}
    for x in data:
        k = x.get(key)
        if k:
            result[k] = x
    return list(result.values())


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


def flatten(
    d: Union[List, Dict], path=None
) -> Generator[Tuple[List[str], Any], None, None]:
    """
    flatten a nested collection of lists/dicts or arbitrary depth, returning a tuple
    of path elements and the value at the end of the path.

    Example:
    >>> x = {
            "paper_id": "6875684534q5ywthr65234t5yqwegrs",
            "metadata": {
                "title": "Salivary Gland Responses to Sparkling Water",
                "authors": [
                    {
                        "first": "Topo",
                        "middle": [],
                        "last": "Chico",
                        "suffix": "",
                        "affiliation": {},
                        "email": "",
                    },
                    {
                        "first": "San",
                        "middle": [],
                        "last": "Pellegrino",
                        "suffix": "",
                        "affiliation": {
                            "laboratory": "",
                            "institution": "Generic Bottling Co.",
                            "location": {
                                "addrLine": "Nowhere",
                                "region": "CA",
                                "country": "United States",
                            },
                        },
                        "email": "",
                    },
                ],
            },
            "abstract": [],
            }

    >>> flatten(x)

        [(['paper_id'], '6875684534q5ywthr65234t5yqwegrs'),
        (['metadata', 'title'], 'Salivary Gland Responses to Sparkling Water'),
        (['metadata', 'authors', 'first'], 'Topo'),
        (['metadata', 'authors', 'last'], 'Chico'),
        (['metadata', 'authors', 'suffix'], ''),
        (['metadata', 'authors', 'affiliation'], {}),
        (['metadata', 'authors', 'email'], ''),
        (['metadata', 'authors'],
        {'first': 'Topo',
        'middle': [],
        'last': 'Chico',
        'suffix': '',
        'affiliation': {},
        'email': ''}),
        (['metadata', 'authors', 'first'], 'San'),
        (['metadata', 'authors', 'last'], 'Pellegrino')]

    """

    if not path:
        path = []
    if isinstance(d, dict):
        for x in d.keys():
            local_path = path[:]
            local_path.append(x)
            for b in flatten(d[x], local_path):
                yield b
    if isinstance(d, list):
        for idx, x in enumerate(d):
            local_path = path[:]
            if isinstance(x, (str, int)):
                local_path.append(f"{idx}.{x}")
            for b in flatten(d[idx], local_path):
                yield b
    else:
        yield path, d
