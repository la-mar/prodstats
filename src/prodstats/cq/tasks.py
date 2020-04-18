import asyncio
import itertools
from datetime import timedelta
from typing import Coroutine, Dict, List, Union

import numpy as np
import pandas as pd
from celery.utils.log import get_task_logger
from celery.utils.time import humanize_seconds

import calc.prod  # noqa
import config as conf
import cq.signals  # noqa
import db.models as models
import ext.metrics as metrics
import util
from collector import IHSClient
from const import HoleDirection, IHSPath, Provider
from cq.worker import celery_app
from executors import BaseExecutor, GeomExecutor, ProdExecutor, WellExecutor  # noqa

logger = get_task_logger(__name__)


RETRY_BASE_DELAY = 15

# TODO: add retries
# TODO: tenacity?
# TODO: asynchronously fracture failed batches
# TODO: circuit breakers?

# TODO: add task meta


def log_transform(
    x: float, vs: float = 1, hs: float = 1, ht: float = 0, vt: float = 0, mod: float = 1
) -> float:
    """ Default parameters yield the untransformed natural log curve.

        f(x) = (vs * ln(hs * (x - ht)) + vt) + (x/mod)

        vs = vertical stretch or compress
        hs = horizontal stretch or compress
        ht = horizontal translation
        vt = vertical translation
        mod = modulate growth of curve W.R.T x

     """

    # import pandas as pd
    # import numpy as np
    # import math

    # df = pd.DataFrame(data={"ct": range(0, 200)})
    # df["log"] = df.ct.add(1).apply(np.log10)
    # df["a"] = df.ct.apply(lambda x: 1 * np.log(1 * (x + 0)) + 1)
    # df["b"] = df.ct.apply(lambda x: 50 * np.log(0.25 * (x + 4)) + 0)
    # df["c"] = df.ct.apply(lambda x: 25 * np.log(0.5 * (x + 2)) + 5)
    # df["d"] = df.ct.apply(lambda x: 25 * np.log(1 * (x + 1)) + 5)
    # df = df.replace([-np.inf, np.inf], np.nan)
    # df = df.fillna(0)
    # sample = df.sample(n=25).fillna(0).astype(int).sort_index()
    # # ax = sns.lineplot(data=df)
    # # ax.set_yscale('log')
    # ax.set_xlim(0, 150)
    # ax.set_ylim(0, 150)

    # multiplier = multiplier or conf.TASK_SPREAD_MULTIPLIER

    return np.round((vs * np.log(hs * (x + ht)) + vt) + (x / mod), 2)


def spread_countdown(x: float, vs: float = None, hs: float = None) -> float:
    return log_transform(x=x, vs=vs or 25, hs=hs or 0.25, ht=4, vt=0, mod=4)


def run_executors(
    hole_dir: HoleDirection,
    api14s: List[str] = None,
    api10s: List[str] = None,
    executors: List[BaseExecutor] = None,
    batch_size: int = None,
    log_vs: float = None,
    log_hs: float = None,
):
    executors = executors or [WellExecutor, GeomExecutor, ProdExecutor]
    batch_size = batch_size or conf.TASK_BATCH_SIZE

    if api14s is not None:
        id_name = "api14s"
        ids = api14s
    elif api10s is not None:
        id_name = "api10s"
        ids = api10s
    else:
        raise ValueError(f"One of [api14s, api10s] must be specified")

    # TODO: move chunking to run_executor?
    for idx, chunk in enumerate(util.chunks(ids, n=batch_size)):
        for executor in executors:
            kwargs = {
                "hole_dir": hole_dir,
                "executor_name": executor.__name__,
                id_name: chunk,
            }
            countdown = spread_countdown(idx, vs=log_vs, hs=log_hs)
            logger.info(
                f"submitting task: hole_dir={hole_dir.value} executor={executor.__name__} {id_name}={len(chunk)} countdown={countdown}"  # noqa
            )

            run_executor.apply_async(
                args=[], kwargs=kwargs, countdown=countdown,
            )


@celery_app.task
def post_heartbeat():
    """ Send heartbeat to metrics backend"""
    return metrics.post_heartbeat()


@celery_app.task
def run_executor(hole_dir: HoleDirection, executor_name: str, **kwargs):
    # logger.warning(f"running {executor_name=} {hole_dir=} {kwargs=}")
    executor = globals()[executor_name]
    count, dataset = executor(hole_dir).run(**kwargs)


@celery_app.task
def run_next_available(hole_dir: Union[HoleDirection, str], force: bool = False):
    """ Run next available area """

    # TODO: set task meta
    hole_dir = HoleDirection(hole_dir)

    async def coro():
        # await db.startup()
        # hole_dir = HoleDirection.H

        # TODO: move to Router
        if hole_dir == HoleDirection.H:
            ids_path = IHSPath.well_h_ids
        else:
            ids_path = IHSPath.well_v_ids

        area_obj, attr, is_ready, cooldown_hours = await models.Area.next_available(
            hole_dir
        )
        utcnow = util.utcnow()
        prev_run = getattr(area_obj, attr)

        if is_ready or force:
            api14s: List[str] = await IHSClient.get_ids_by_area(
                path=ids_path, area=area_obj.area
            )  # pull from IDMaster once implmented
            api14s = api14s[:10]
            run_executors(hole_dir=hole_dir, api14s=api14s)

            await area_obj.update(**{attr: utcnow}).apply()

            prev_run = (
                prev_run.strftime(util.dt.formats.no_seconds) if prev_run else None
            )
            utcnow = utcnow.strftime(util.dt.formats.no_seconds)
            logger.warning(
                f"({models.Area.__name__}[{hole_dir}]) updated {area_obj.area}.{attr}: {prev_run} -> {utcnow}"  # noqa
            )
        else:
            next_run_in_seconds = (
                (prev_run + timedelta(hours=cooldown_hours)) - utcnow
            ).total_seconds()
            logger.warning(
                f"({models.Area.__name__}[{hole_dir}]) Skipping {area_obj.area} next available for run in {humanize_seconds(next_run_in_seconds)}"  # noqa
            )  # noqa

    return util.aio.async_to_sync(coro())


@celery_app.task
def sync_area_manifest():  # FIXME: change to use Counties endpoint (and add Counties endpoint to IHS service :/) # noqa
    """ Ensure the local list of areas is up to date """

    loop = asyncio.get_event_loop()

    async def wrapper(path: IHSPath, hole_dir: HoleDirection) -> List[Dict]:

        records: List[Dict] = []
        areas = await IHSClient.get_areas(path=path, name_only=False)
        records = [
            {"area": area["name"], "providers": [Provider.IHS]} for area in areas
        ]
        return records

    coros: List[Coroutine] = []
    for args in [
        (IHSPath.well_h_ids, HoleDirection.H),
        (IHSPath.well_v_ids, HoleDirection.V),
        (IHSPath.prod_h_ids, HoleDirection.H),
        (IHSPath.prod_v_ids, HoleDirection.V),
    ]:
        coros.append(wrapper(*args))

    results = loop.run_until_complete(asyncio.gather(*coros))
    inbound_df = pd.DataFrame(list(itertools.chain(*results))).set_index("area")
    inbound_areas = inbound_df.groupby(level=0).first().sort_index()

    existing_areas = util.aio.async_to_sync(models.Area.df()).sort_index()

    # get unique area names that dont already exist
    for_insert = inbound_areas[~inbound_areas.isin(existing_areas)].dropna()

    # for_insert["h_last_run_at"] = util.utcnow()

    if for_insert.shape[0] > 0:
        coro = models.Area.bulk_upsert(
            for_insert,
            update_on_conflict=True,
            reset_index=True,
            conflict_constraint=models.Area.constraints["uq_areas_area"],
        )

        affected = loop.run_until_complete(coro)

        logger.info(
            f"({models.Area.__name__}) synchronized manifest: added {affected} areas"
        )
    else:
        logger.info(f"({models.Area.__name__}) synchronized manifest: no updates")


@celery_app.task
def log():
    """Print some log messages"""
    logger.warning("heartbeat")


@celery_app.task
def smoke_test():
    """ Verify an arbitrary Celery task can run """
    return "verified"


@celery_app.task
def run_test_apilist():

    api10s = list(
        set(
            [
                "4232938906",
                "4232939088",
                "4232939552",
                "4232939730",
                "4232939821",
                "4232939919",
                "4232939930",
                "4232940253",
                "4232940254",
                "4232940257",
                "4232940266",
                "4232940267",
                "4232940268",
                "4232940269",
                "4232940270",
                "4232940271",
                "4232940317",
                "4232940463",
                "4232940465",
                "4232940466",
                "4232940467",
                "4232940567",
                "4232940569",
                "4232940570",
                "4232940572",
                "4232940573",
                "4232940624",
                "4232940625",
                "4232940699",
                "4232940700",
                "4232940745",
                "4232940746",
                "4232940747",
                "4232940803",
                "4232940807",
                "4232940808",
                "4232940809",
                "4232940815",
                "4232940816",
                "4232940899",
                "4232940901",
                "4232940902",
                "4232940904",
                "4232940906",
                "4232940932",
                "4232940936",
                "4232940941",
                "4232940990",
                "4232941003",
                "4232941050",
                "4232941068",
                "4232941070",
                "4232941071",
                "4232941072",
                "4232941073",
                "4232941077",
                "4232941103",
                "4232941139",
                "4232941140",
                "4232941141",
                "4232941158",
                "4232941161",
                "4232941198",
                "4232941232",
                "4232941234",
                "4232941236",
                "4232941238",
                "4232941245",
                "4232941272",
                "4232941273",
                "4232941274",
                "4232941312",
                "4232941398",
                "4232941413",
                "4232941415",
                "4232941417",
                "4232941420",
                "4232941422",
                "4232941434",
                "4232941435",
                "4232941436",
                "4232941501",
                "4232941517",
                "4232941530",
                "4232941535",
                "4232941540",
                "4232941545",
                "4232941547",
                "4232941550",
                "4232941558",
                "4232941559",
                "4232941560",
                "4232941596",
                "4232941609",
                "4232941611",
                "4232941644",
                "4232941708",
                "4232941709",
                "4232941710",
                "4232941775",
                "4232941847",
                "4232941890",
                "4232941892",
                "4232941918",
                "4232941919",
                "4232941920",
                "4232941921",
                "4232941922",
                "4232941923",
                "4232941924",
                "4232941928",
                "4232941930",
                "4232941939",
                "4232941940",
                "4232941957",
                "4232942009",
                "4232942078",
                "4232942105",
                "4232942120",
                "4232942136",
                "4232942140",
                "4232942143",
                "4232942152",
                "4232942169",
                "4232942170",
                "4232942171",
                "4232942172",
                "4232942175",
                "4232942202",
                "4232942205",
                "4232942207",
                "4232942210",
                "4232942213",
                "4232942245",
                "4232942266",
                "4232942299",
                "4232942302",
                "4232942304",
                "4232942370",
                "4232942378",
                "4232942379",
                "4232942423",
                "4232942424",
                "4232942436",
                "4232942437",
                "4232942463",
                "4232942509",
                "4232942514",
                "4232942515",
                "4232942516",
                "4232942586",
                "4232942587",
                "4232942589",
                "4232942598",
                "4232942650",
                "4232942666",
                "4232942683",
                "4232942719",
                "4232942728",
                "4232942729",
                "4232942730",
                "4232942755",
                "4232942758",
                "4232942783",
                "4232942788",
                "4232942791",
                "4232942805",
                "4232942807",
                "4232942819",
                "4232942827",
                "4232942884",
                "4232942887",
                "4232942889",
                "4232942929",
                "4232942931",
                "4232943011",
                "4232943058",
                "4232943061",
                "4232943064",
                "4232943067",
                "4232943069",
                "4232943070",
                "4232943082",
                "4232943087",
                "4232943092",
                "4238340357",
                "4246139434",
                "4246139461",
                "4246139588",
                "4246139665",
                "4246139671",
                "4246139720",
                "4246139754",
                "4246139770",
                "4246139771",
                "4246139796",
                "4246139841",
                "4246139851",
                "4246139885",
                "4246139924",
                "4246140005",
                "4246140013",
                "4246140014",
                "4246140015",
                "4246140016",
                "4246140017",
                "4246140018",
                "4246140069",
                "4246140106",
                "4246140108",
                "4246140114",
                "4246140122",
                "4246140130",
                "4246140132",
                "4246140146",
                "4246140151",
                "4246140152",
                "4246140158",
                "4246140159",
                "4246140165",
                "4246140166",
                "4246140168",
                "4246140169",
                "4246140170",
                "4246140173",
                "4246140174",
                "4246140175",
                "4246140190",
                "4246140193",
                "4246140194",
                "4246140195",
                "4246140197",
                "4246140201",
                "4246140204",
                "4246140205",
                "4246140206",
                "4246140208",
                "4246140209",
                "4246140213",
                "4246140217",
                "4246140222",
                "4246140223",
                "4246140230",
                "4246140237",
                "4246140238",
                "4246140243",
                "4246140244",
                "4246140245",
                "4246140246",
                "4246140247",
                "4246140250",
                "4246140253",
                "4246140256",
                "4246140258",
                "4246140272",
                "4246140291",
                "4246140292",
                "4246140301",
                "4246140308",
                "4246140336",
                "4246140343",
                "4246140345",
                "4246140349",
                "4246140358",
                "4246140362",
                "4246140364",
                "4246140377",
                "4246140378",
                "4246140379",
                "4246140381",
                "4246140388",
                "4246140397",
                "4246140400",
                "4246140402",
                "4246140407",
                "4246140409",
                "4246140410",
                "4246140415",
                "4246140417",
                "4246140441",
                "4246140443",
                "4246140444",
                "4246140445",
                "4246140449",
                "4246140460",
                "4246140467",
                "4246140468",
                "4246140469",
                "4246140470",
                "4246140472",
                "4246140473",
                "4246140491",
                "4246140492",
                "4246140493",
                "4246140494",
                "4246140495",
                "4246140504",
                "4246140505",
                "4246140522",
                "4246140531",
                "4246140532",
                "4246140533",
                "4246140534",
                "4246140535",
                "4246140537",
                "4246140538",
                "4246140543",
                "4246140544",
                "4246140545",
                "4246140550",
                "4246140551",
                "4246140552",
                "4246140553",
                "4246140554",
                "4246140555",
                "4246140556",
                "4246140562",
                "4246140563",
                "4246140576",
                "4246140577",
                "4246140578",
                "4246140579",
                "4246140611",
                "4246140612",
                "4246140613",
                "4246140632",
                "4246140633",
                "4246140634",
                "4246140635",
                "4246140647",
                "4246140650",
                "4246140656",
                "4246140666",
                "4246140667",
                "4246140668",
                "4246140669",
                "4246140692",
                "4246140710",
                "4246140719",
                "4246140720",
                "4246140733",
                "4246140734",
                "4246140741",
                "4246140742",
                "4246140743",
                "4246140749",
                "4246140750",
                "4246140751",
                "4246140752",
                "4246140773",
                "4246140774",
                "4246140775",
                "4246140778",
                "4246140780",
                "4246140781",
                "4246140782",
                "4246140814",
                "4246140815",
                "4246140816",
                "4246140819",
                "4246140820",
                "4246140822",
                "4246140823",
                "4246140832",
                "4246140837",
                "4246140838",
                "4246140842",
                "4246140856",
                "4246140874",
                "4246140877",
                "4246140878",
                "4246140879",
                "4246140880",
                "4246140881",
                "4246140882",
                "4246140884",
                "4246140885",
                "4246140886",
                "4246140887",
                "4246140905",
                "4246140914",
                "4246140915",
                "4246140917",
                "4246140922",
                "4246140927",
                "4246140938",
                "4246140939",
                "4246140943",
                "4246140944",
                "4246140945",
                "4246140946",
                "4246140947",
                "4246140948",
                "4246140950",
                "4246140951",
                "4246140956",
                "4246140958",
                "4246140959",
                "4246140960",
                "4246140961",
                "4246140964",
                "4246140966",
                "4246140971",
                "4246141003",
                "4246141012",
                "4246141036",
                "4246141054",
                "4246141055",
                "4246141077",
                "4232942174",
            ]
        )
    )

    run_executors(HoleDirection.H, api10s=api10s, log_vs=200, batch_size=10)


@celery_app.task
def run_driftwood():

    executors = [WellExecutor, GeomExecutor, ProdExecutor]

    deo_api14h = [
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

    run_executors(HoleDirection.H, deo_api14h, executors=executors)

    deo_api14v = [
        "42461326620001",
        "42461326620000",
        "42461328130000",
        "42461343960001",
        "42461352410000",
        "42383362120000",
        "42383362080000",
        "42383362090000",
        "42383374140000",
        "42383374130000",
        "42383362060000",
    ]

    run_executors(HoleDirection.V, deo_api14v, executors=executors)


if __name__ == "__main__":
    from db import db
    from db.models import ProdMonthly, ProdStat
    import loggers
    import cq.tasks

    loggers.config()
    hole_dir = HoleDirection.H
    util.aio.async_to_sync(db.startup())
    # cq.tasks.sync_area_manifest.apply_async()

    from util.jsontools import load_json

    api10s = load_json("/Users/friedrichb/Downloads/apilist.json")
    api10s = list(set(api10s))

    api10s_db = util.aio.async_to_sync(
        ProdStat.query.distinct(ProdStat.api10).gino.load(ProdStat.api10).all()
    )
    api10s_db = list(set(api10s_db))
    missing = list({x for x in api10s if x not in api10s_db})
    print(f"input={len(api10s)} missing={len(missing)} in_db={len(api10s_db)}")

    api10s_db = util.aio.async_to_sync(
        ProdMonthly.query.distinct(ProdMonthly.api10)
        .where(ProdMonthly.oil_norm_3k is None)
        .gino.load(ProdMonthly.api10)
        .all()
    )

    len(api10s_db)

    cq.tasks.run_executors(
        hole_dir=hole_dir,
        api10s=api10s,
        executors=[ProdExecutor],
        batch_size=10,
        log_vs=200,
        # log_hs=0.5,
    )

    cq.tasks.run_executors(
        hole_dir=hole_dir, api10s=api10s, executors=[WellExecutor], batch_size=10,
    )

    # import pandas as pd
    # import numpy as np
    # import math

    # df = pd.DataFrame(data={"ct": range(0, 200)})
    # df["log"] = df.ct.add(1).apply(np.log10)
    # df["a"] = df.ct.apply(lambda x: 1 * np.log(1 * (x + 0)) + 1)
    # df["b"] = df.ct.apply(lambda x: 50 * np.log(0.25 * (x + 4)) + 0)
    # df["c"] = df.ct.apply(lambda x: 25 * np.log(0.5 * (x + 2)) + 5)
    # df["d"] = df.ct.apply(lambda x: 25 * np.log(1 * (x + 1)) + 5)
    # df = df.replace([-np.inf, np.inf], np.nan)
    # df = df.fillna(0)
    # sample = df.sample(n=25).fillna(0).astype(int).sort_index()
    # # ax = sns.lineplot(data=df)
    # # ax.set_yscale('log')
    # ax.set_xlim(0, 150)
    # ax.set_ylim(0, 150)

    # f(x) = a * ln(b * (x - c)) + d
    #
    # a = vertical stretch or compress
    # b = horizontal stretch or compress
    # c = translate horizontally
    # d = translate vertically
