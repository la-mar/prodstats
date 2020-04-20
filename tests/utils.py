import json
import random
import string
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

from httpx import Response
from httpx.dispatch.base import AsyncDispatcher, SyncDispatcher

import util
from collector import FracFocusClient, IHSClient
from const import HoleDirection, IHSPath
from db.models import Model
from util.enums import Enum
from util.jsontools import to_json


class MockBaseDispatch:
    def __init__(
        self, body=b"", status_code=200, headers=None, cookie=None, assert_func=None,
    ):
        self._original_body = body
        if headers is None:
            headers = {}
        if isinstance(body, (dict, list)):
            body = json.dumps(body, cls=util.jsontools.UniversalEncoder).encode()
            headers["Content-Type"] = "application/json"
        else:
            if isinstance(body, str):
                body = body.encode()
            headers["Content-Type"] = "application/x-www-form-urlencoded"

        self.body = body
        self.status_code = status_code
        self.headers = headers
        self.cookie = cookie
        self.assert_func = assert_func


class MockDispatch(MockBaseDispatch, SyncDispatcher):
    def send(self, request, verify=None, cert=None, timeout=None):
        if self.assert_func:
            self.assert_func(request)

        return Response(
            self.status_code, content=self.body, headers=self.headers, request=request,
        )


class MockAsyncDispatch(MockBaseDispatch, AsyncDispatcher):
    async def send(self, request, verify=None, cert=None, timeout=None):
        if self.assert_func:
            self.assert_func(request)

        response = Response(
            self.status_code, content=self.body, headers=self.headers, request=request,
        )
        if self.cookie:
            response.cookies.set(**self.cookie)
        return response


def get_open_port():
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("", 0))
    s.listen(1)
    port = s.getsockname()[1]
    s.close()
    return port


def rand(length: int = None, min: int = None, max: int = None, choices=None):
    n = 8

    if length:
        n = length
    else:
        if not length and min and max:
            n = random.choice(range(min, max + 1))

    return "".join(random.choice(choices) for _ in range(n))


def rand_str(
    length: int = None, min: int = None, max: int = None, choices=string.ascii_letters
):
    return rand(length, min, max, choices)


def rand_int(
    length: int = None, min: int = None, max: int = None, choices=string.digits
):
    return int(rand(length, min, max, choices))


def rand_float(min: float = 0, max: float = 100, step: float = 0.01):
    return random.uniform(min, max)


def rand_lon(min: float = -98.7, max: float = -95, step: float = 0.0000000001):
    return rand_float(min, max, step)


def rand_lat(min: float = 31.9, max: float = 33.6, step: float = 0.0000000001):
    return rand_float(min, max, step)


def rand_email(min: int, max: int):
    return f"{rand_str(min=3, max=25)}@gmail.com"


def rand_datetime(min_year=1900, max_year=datetime.now().year):
    start = datetime(min_year, 1, 1, 00, 00, 00)
    years = max_year - min_year + 1
    end = start + timedelta(days=365 * years)
    return start + (end - start) * random.random()


def rand_bool():
    return random.choice([True, False])


def rand_enum(e: Enum):
    return random.choice(e.values())


def generator_map():
    return {
        datetime: rand_datetime(),
        int: rand_int(min=1, max=9),
        str: rand_str(min=4, max=10),
        float: rand_float(),
        bool: rand_bool(),
        bytes: rand_str(min=300, max=10000).encode(),
        dict: {rand_str(length=4): rand_str(length=4) for x in range(0, 5)},
        list: [rand_str(length=4) for x in range(0, 5)],
    }


def random_model_data(model: Model, n: int = 10) -> List[Dict]:
    items = []
    pytypes = model.columns.pytypes
    dtypes = model.columns.dtypes
    for i in range(0, n):
        item = {}
        for col_name, col_type in pytypes.items():
            dtype = dtypes[col_name]
            dtype_name = dtype.__class__.__name__
            value = None
            if dtype_name == "ChoiceType":  # handle enums
                value = random.choice(dtype.choices.values())
            elif dtype_name == "EmailType":  # handle enums
                value = rand_email(min=5, max=20)
            elif col_name == "lon":
                value = rand_lon()
            elif col_name == "lat":
                value = rand_lat()
            else:
                value = generator_map()[col_type]

            if col_name not in ["id"]:
                item[col_name] = value
        items.append(item)

    return items


async def seed_model(model: Model, n: int = 10):
    items = random_model_data(model=model, n=n)
    # from util.jsontools import to_string

    # print(to_string(items))
    await model.bulk_insert(items)
    print(f"Created {len(items)} {model.__tablename__}")


# hole_dir = HoleDirection.H
# n=10


async def refresh_fixtures(hole_dir: HoleDirection, n: int = 10):

    fixture_path = Path("tests/fixtures")
    hd = hole_dir.value.lower()

    if hole_dir == HoleDirection.H:
        wellpath = IHSPath.well_h_sample
        prodpath = IHSPath.prod_h
        geompath = IHSPath.well_h_geoms

    elif HoleDirection.V:
        wellpath = IHSPath.well_v_sample
        prodpath = IHSPath.prod_v
        geompath = IHSPath.well_v_geoms
    else:
        raise ValueError(f"invalid hole direction")

    wells = await IHSClient.get_sample(wellpath, n=n, area="tx-midland")
    api14s = [x["api14"] for x in wells]

    to_json(wells, fixture_path / f"wells_{hd}.json")

    prod = await IHSClient.get_production(prodpath, api14s=api14s)
    to_json(
        prod, fixture_path / f"prod_{hd}.json",
    )

    geoms = await IHSClient.get_wells(geompath, api14s=api14s)
    to_json(
        geoms, fixture_path / f"geoms_{hd}.json",
    )

    fracs = await FracFocusClient.get_jobs(api14s=api14s)
    to_json(
        fracs, fixture_path / f"fracs_{hd}.json",
    )


if __name__ == "__main__":
    util.aio.async_to_sync(refresh_fixtures(HoleDirection.H, n=25))
    util.aio.async_to_sync(refresh_fixtures(HoleDirection.V, n=10))
