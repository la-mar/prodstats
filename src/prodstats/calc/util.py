import asyncio
import itertools
from typing import List, Optional, Tuple, Union

import pandas as pd

import util
from const import HoleDirection, IHSPath, ProdStatRange

__all__ = [
    "prodstat_option_matrix",
    "status_assignment_detail",
    "PRODSTAT_DEFAULT_OPTIONS",
    "PRODSTAT_DEFAULT_RATIO_OPTIONS",
]


def prodstat_option_matrix(
    ranges: Union[ProdStatRange, List[ProdStatRange]],
    months: Optional[Union[int, List[int]]],
    include_zeroes: Union[bool, List[bool]] = [True, False],
) -> List[Tuple[ProdStatRange, int, bool]]:
    return list(
        itertools.product(
            util.ensure_list(ranges),
            util.ensure_list(months),
            util.ensure_list(include_zeroes),
        )
    )


def status_assignment_detail(
    hole_dir: Union[HoleDirection, str], api14s: List[str]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    hole_dir = HoleDirection(hole_dir)

    async def coro() -> Tuple[pd.DataFrame, pd.DataFrame]:

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
        return indicators, labels

    loop = asyncio.get_event_loop()
    return loop.run_until_complete(coro())


PRODSTAT_DEFAULT_OPTIONS: frozenset = frozenset(  # TODO: define this in config somehow?
    itertools.chain(
        prodstat_option_matrix(
            ProdStatRange.FIRST, months=[1, 3, 6, 12, 18, 24, 30, 36, 42, 48],
        ),
        prodstat_option_matrix(ProdStatRange.LAST, months=[1, 3, 6, 12],),
        prodstat_option_matrix(ProdStatRange.PEAKNORM, months=[1, 3, 6],),
        prodstat_option_matrix(ProdStatRange.ALL, months=None),
    )
)

PRODSTAT_DEFAULT_RATIO_OPTIONS: frozenset = frozenset(
    itertools.chain(
        prodstat_option_matrix(ProdStatRange.FIRST, months=[1, 3, 6, 12, 18, 24]),
        prodstat_option_matrix(ProdStatRange.LAST, months=[1, 3, 6]),
        prodstat_option_matrix(ProdStatRange.PEAKNORM, months=[1, 3, 6]),
        prodstat_option_matrix(ProdStatRange.ALL, months=None),
    )
)


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

    status_assignment_detail(hole_dir="H", api14s=api14s)
