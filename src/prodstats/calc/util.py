import asyncio
from typing import List, Union

import pandas as pd

from const import HoleDirection, IHSPath


def print_status_assignment_detail(
    hole_dir: Union[HoleDirection, str], api14s: List[str]
):
    hole_dir = HoleDirection(hole_dir)

    async def coro():

        wells, *other = await pd.DataFrame.wells.from_multiple(
            hole_dir=hole_dir, api14s=api14s
        )

        # TODO: Move to Router
        if hole_dir == HoleDirection.H:
            prodpath = IHSPath.prod_h_headers
        elif hole_dir == HoleDirection.V:
            prodpath = IHSPath.prod_v_header
        else:
            raise ValueError(f"cant determine IHSPath from hole_dir ({hole_dir})")

        prod_headers = await wells.wells.last_prod_date(
            path=prodpath, prefer_local=False
        )
        wells = wells.join(prod_headers)
        indicators = wells.wells.assign_status(detail=True)
        print(f"\n{indicators.T}\n")
        labels = wells.wells.assign_status(detail=True, as_labels=True)
        print(f"\n{labels.T}\n")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(coro())


if __name__ == "__main__":
    api14s = [
        "42461409160000",
        "42383406370000",
        "42461412100000",
        "42461412090000",
        "42461411750000",
        "42461411740000",
        "42461411730000",
        "42461411720000",
        "42461411600000",
        "42461411280000",
        "42461411270000",
        "42461411260000",
        "42383406650000",
        "42383406640000",
        "42383406400000",
        "42383406390000",
        "42383406380000",
        "42461412110000",
        "42383402790000",
    ]

    print_status_assignment_detail(hole_dir="H", api14s=api14s)
