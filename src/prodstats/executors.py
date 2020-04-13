import asyncio
import logging
import uuid
from timeit import default_timer as timer
from typing import Coroutine, Dict, List, Optional, Tuple, Union

import pandas as pd

import calc.geom  # noqa
import calc.prod  # noqa
import calc.well  # noqa
import db.models as models
import exc
import util
from calc.sets import ProdSet, WellGeometrySet, WellSet
from const import HoleDirection, IHSPath

logger = logging.getLogger(__name__)

# TODO: There is likely a good deal of shared functionality that can be
#       migrated up into the base executor


class BaseExecutor:
    __exec_name__: str = "base"

    def __init__(self, hole_dir: Union[HoleDirection, str]):

        self.hole_dir: HoleDirection = HoleDirection(hole_dir)
        self.metrics: pd.DataFrame = pd.DataFrame(
            columns=["executor", "operation", "name", "time", "count"]
        )

    def __repr__(self):
        return f"{self.__class__.__name__}[{self.hole_dir.value}]"

    def add_metric(self, operation: str, name: str, time: float, count: int):
        idx_max = self.metrics.index.max()
        idx_max = idx_max + 1 if not pd.isna(idx_max) else 0
        self.metrics = self.metrics.append(
            pd.Series(
                {
                    "executor": self.__exec_name__,
                    "operation": operation,
                    "name": name,
                    "time": time,
                    "count": count,
                },
                name=idx_max,
            )
        )

    async def _persist(
        self, name: str, model: models.Model, df: pd.DataFrame, **kwargs
    ) -> int:
        ts = timer()

        count = await model.bulk_upsert(df, **kwargs)
        exc_time = round(timer() - ts, 2)
        logger.info(f"({self}) persisted {df.shape[0]} {name} records ({exc_time}s)")
        self.add_metric(
            operation="persist", name=name, time=exc_time, count=df.shape[0],
        )
        return count


class ProdExecutor(BaseExecutor):
    __exec_name__: str = "prod"

    def __init__(
        self,
        hole_dir: Union[HoleDirection, str],
        header_kwargs: Dict = None,
        monthly_kwargs: Dict = None,
        stats_kwargs: Dict = None,
        download_kwargs: Dict = None,
        process_kwargs: Dict = None,
    ):
        super().__init__(hole_dir)
        self.model_kwargs = {
            "header": {**(header_kwargs or {})},
            "monthly": {**(monthly_kwargs or {})},
            "stats": {"batch_size": 1000, **(stats_kwargs or {})},
        }
        self.download_kwargs = download_kwargs or {}
        self.process_kwargs = process_kwargs or {}

    async def download(
        self,
        entities: Union[str, List[str]] = None,
        entity12s: Union[str, List[str]] = None,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        create_index: bool = True,
        **kwargs,
    ) -> pd.DataFrame:
        kwargs = {**self.download_kwargs, **kwargs}
        try:
            ts = timer()

            # TODO:  Move to router
            if self.hole_dir == HoleDirection.H:
                path = IHSPath.prod_h
            elif self.hole_dir == HoleDirection.V:
                path = IHSPath.prod_v
            else:
                raise ValueError("Cant determine request path")

            df = await pd.DataFrame.prodstats.from_ihs(
                path=path,
                entities=entities,
                entity12s=entity12s,
                api14s=api14s,
                api10s=api10s,
                create_index=create_index,
                **kwargs,
            )
            exc_time = round(timer() - ts, 2)
            logger.info(f"({self}) downloaded {df.shape[0]} records ({exc_time}s)")
            self.add_metric(
                operation="download", name="*", time=exc_time, count=df.shape[0],
            )
            return df
        except Exception as e:
            logger.error(
                f"({self}) error during download -- {e}",
                extra={
                    "entities": entities,
                    "entity12s": entity12s,
                    "api14s": api14s,
                    "api10s": api10s,
                },
            )
            raise e

    async def process(self, df: pd.DataFrame, **kwargs) -> ProdSet:
        kwargs = {**self.process_kwargs, **kwargs}
        try:
            ts = timer()

            ps = df.prodstats.calc(**kwargs)
            exc_time = round(timer() - ts, 2)

            total_count = sum([x.shape[0] for x in list(ps) if x is not None])
            for name, model, df in ps.items():
                t = round(df.shape[0] * (exc_time / (total_count or 1)), 2)
                logger.info(f"processed {df.shape[0]} {name} records ({t}s)")
                self.add_metric(
                    operation="process", name=name, time=t, count=df.shape[0],
                )
            return ps
        except Exception as e:
            api10s = df.util.column_as_set("api10")
            logger.error(
                f"({self}) error during persisting: {len(api10s)} records in batch -- {e}",
                extra={"api10s": api10s},
            )
            raise e

    async def persist(
        self,
        ps: ProdSet,
        header_kwargs: Dict = None,
        monthly_kwargs: Dict = None,
        stats_kwargs: Dict = None,
    ) -> int:

        try:
            coros: List[Coroutine] = []
            for name, model, df in ps.items():
                kwargs = {}
                if name == "header" and header_kwargs:
                    kwargs = header_kwargs
                elif name == "monthly" and monthly_kwargs:
                    kwargs = monthly_kwargs
                elif name == "stats" and stats_kwargs:
                    kwargs = stats_kwargs

                coros.append(
                    self._persist(
                        name, model, df, **{**self.model_kwargs[name], **kwargs}
                    )
                )
            return sum(await asyncio.gather(*coros))

        except Exception as e:
            api10s = ps.header.util.column_as_set("api10")
            logger.error(
                f"({self}) error during persisting: {len(api10s)} records in batch -- {e}",
                extra={"api10s": api10s},
            )
            raise e

    async def _run(
        self,
        entities: Union[str, List[str]] = None,
        entity12s: Union[str, List[str]] = None,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        return_data: bool = False,
    ) -> Tuple[int, Optional[ProdSet]]:

        exec_id = uuid.uuid4().hex
        logger.warning(f"({self}) starting execution {exec_id=}")
        try:
            df = await self.download(
                entities=entities, api10s=api10s, entity12s=entity12s
            )
            ps = await self.process(df)
            ct = await self.persist(ps)
            logger.warning(f"({self}) completed execution {exec_id=}")
            return ct, ps if return_data else None
        except Exception as e:
            logger.error(
                f"({self}) run failed: {exec_id=} -- {e}",
                extra={
                    "entities": entities,
                    "entity12s": entity12s,
                    "api14s": api14s,
                    "api10s": api10s,
                },
            )
            raise e

    async def run(
        self,
        entities: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        entity12s: Union[str, List[str]] = None,
        batch_size: int = None,  # max number of ids to process together
        return_data: bool = False,
    ) -> Tuple[List[Tuple[int, Optional[ProdSet]]], List[BaseException]]:

        param_count = sum(
            [entities is not None, api10s is not None, entity12s is not None]
        )

        if param_count > 1:
            raise ValueError(
                f"Only one of [area_name, api10s, entity12s, entities] can be specified"
            )

        elif param_count < 1:
            raise ValueError(
                f"One of [area_name, api10s, entity12s, entities] must be specified"
            )

        batch_size = batch_size or 100

        if entities:
            ids = entities
            id_type = "entities"
        elif api10s:
            ids = api10s
            id_type = "api10s"
        elif entity12s:
            ids = entity12s
            id_type = "entity12s"
        else:
            raise ValueError(f"Could not determine id type")

        # generate coros for all chunks
        coros: List[Coroutine] = []
        for chunk in util.chunks(ids, n=batch_size):
            coros.append(self._run(**{id_type: chunk, "return_data": return_data}))

        logger.info(
            f"({self}) compressed {len(ids)} {id_type} into {len(coros)} coroutines"
        )

        return exc.split_errors(await asyncio.gather(*coros, return_exceptions=True))

    def run_sync(
        self,
        entities: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        entity12s: Union[str, List[str]] = None,
        batch_size: int = None,  # max number of ids to process together
        return_data: bool = False,
    ) -> Tuple[List[Tuple[int, Optional[ProdSet]]], List[BaseException]]:
        loop = asyncio.get_event_loop()
        coro = self.run(
            entities=entities,
            api10s=api10s,
            entity12s=entity12s,
            batch_size=batch_size,
            return_data=return_data,
        )
        return loop.run_until_complete(coro)


class GeomExecutor(BaseExecutor):
    __exec_name__: str = "geom"

    def __init__(
        self,
        hole_dir: Union[HoleDirection, str],
        locations_kwargs: Dict = None,
        surveys_kwargs: Dict = None,
        points_kwargs: Dict = None,
        download_kwargs: Dict = None,
        process_kwargs: Dict = None,
    ):
        super().__init__(hole_dir)
        self.model_kwargs = {
            "locations": {**(locations_kwargs or {})},
            "surveys": {**(surveys_kwargs or {})},
            "points": {"batch_size": 1000, **(points_kwargs or {})},
        }
        self.download_kwargs = download_kwargs or {}
        self.process_kwargs = process_kwargs or {}

    async def download(
        self,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        create_index: bool = True,
        **kwargs,
    ) -> WellGeometrySet:
        kwargs = {**self.download_kwargs, **kwargs}
        try:
            ts = timer()

            # TODO:  Move to router
            if self.hole_dir == HoleDirection.H:
                path = IHSPath.well_h_geoms
            elif self.hole_dir == HoleDirection.V:
                path = IHSPath.well_v_geoms
            else:
                raise ValueError("Cant determine request path")

            geoms: WellGeometrySet = await pd.DataFrame.shapes.from_ihs(
                path=path,
                api14s=api14s,
                api10s=api10s,
                create_index=create_index,
                **kwargs,
            )
            exc_time = round(timer() - ts, 2)
            logger.info(
                f"({self}) downloaded {geoms.locations.shape[0]} geometries ({exc_time}s)"  # noqa
            )
            self.add_metric(
                operation="download",
                name="*",
                time=exc_time,
                count=geoms.locations.shape[0],
            )
            return geoms
        except Exception as e:
            count = len(util.ensure_list(api10s or api14s))
            logger.error(
                f"({self}) error during download: {count} records in batch -- {e}",
                extra={"api10s": api10s, "api14s": api14s},
            )
            raise e

    async def process(self, geoms: WellGeometrySet, **kwargs) -> WellGeometrySet:
        kwargs = {**self.process_kwargs, **kwargs}
        try:
            ts = timer()
            locations, surveys, points = geoms
            points = points.shapes.index_survey_points()
            kops = points.shapes.find_kop()
            points = points.join(kops)

            # surveys
            laterals = points[points.is_in_lateral].shapes.as_line(label="lateral_only")
            sticks = points.shapes.as_stick()
            bent_sticks = points.shapes.as_bent_stick()
            surveys = surveys.join(laterals).join(sticks).join(bent_sticks)

            geomset = WellGeometrySet(
                locations=locations, surveys=surveys, points=points
            )
            exc_time = round(timer() - ts, 2)

            total_count = sum([x.shape[0] for x in list(geomset) if x is not None])
            for name, model, df in geomset.items():
                t = round(df.shape[0] * (exc_time / (total_count or 1)), 2)
                logger.info(f"processed {df.shape[0]} {name} records ({t}s)")
                self.add_metric(
                    operation="process", name=name, time=t, count=df.shape[0],
                )
            return geomset
        except Exception as e:
            api14s = locations.util.column_as_set("api14")
            logger.error(
                f"({self}) error during processing: {len(api14s)} records in batch -- {e}",
                extra={"api14s": api14s},
            )
            raise e

    async def persist(
        self,
        geoms: WellGeometrySet,
        locations_kwargs: Dict = None,
        surveys_kwargs: Dict = None,
        points_kwargs: Dict = None,
    ) -> int:

        try:
            coros: List[Coroutine] = []
            geoms = geoms.shapes_as_wkb()
            for name, model, df in geoms.items():
                kwargs = {}
                if name == "locations" and locations_kwargs:
                    kwargs = locations_kwargs
                elif name == "surveys" and surveys_kwargs:
                    kwargs = surveys_kwargs
                elif name == "points" and points_kwargs:
                    kwargs = points_kwargs

                coros.append(
                    self._persist(
                        name, model, df, **{**self.model_kwargs[name], **kwargs}
                    )
                )
            return sum(await asyncio.gather(*coros))

        except Exception as e:
            api14s = geoms.locations.util.column_as_set("api14")
            logger.error(
                f"({self}) error during persistance: {len(api14s)} records in batch -- {e}",
                extra={"api14s": api14s},
            )
            raise e

    async def _run(
        self,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        return_data: bool = False,
    ) -> Tuple[int, Optional[WellGeometrySet]]:

        exec_id = uuid.uuid4().hex
        logger.warning(f"({self}) starting execution {exec_id=}")

        try:
            df = await self.download(api14s=api14s, api10s=api10s)
            ps = await self.process(df)
            ct = await self.persist(ps)
            logger.warning(f"({self}) completed execution {exec_id=}")
            return ct, ps if return_data else None

        except Exception as e:
            logger.error(
                f"({self}) run failed: {exec_id=} -- {e}",
                extra={"api14s": api14s, "api10s": api10s},
            )
            raise e

    async def run(
        self,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        batch_size: int = None,  # max number of ids to process together
        return_data: bool = False,
    ) -> Tuple[List[Tuple[int, Optional[WellGeometrySet]]], List[BaseException]]:

        param_count = sum([api14s is not None, api10s is not None])

        if param_count > 1:
            raise ValueError(f"Only one of [api14s, api10s] can be specified")

        elif param_count < 1:
            raise ValueError(f"One of [api14s, api10s] must be specified")

        batch_size = batch_size or 100

        if api14s:
            ids = api14s
            id_type = "api14s"
        elif api10s:
            ids = api10s
            id_type = "api10s"
        else:
            raise ValueError(f"Could not determine id type")

        # generate coros for all chunks
        coros: List[Coroutine] = []
        for chunk in util.chunks(ids, n=batch_size):
            coros.append(self._run(**{id_type: chunk, "return_data": return_data}))

        logger.info(f"created {len(coros)} coroutines from {len(ids)} {id_type}s")

        return exc.split_errors(await asyncio.gather(*coros, return_exceptions=True))

    def run_sync(
        self,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        batch_size: int = None,  # max number of ids to process together
        return_data: bool = False,
    ) -> Tuple[List[Tuple[int, Optional[WellGeometrySet]]], List[BaseException]]:
        loop = asyncio.get_event_loop()
        coro = self.run(
            api14s=api14s, api10s=api10s, batch_size=batch_size, return_data=return_data
        )
        return loop.run_until_complete(coro)


class WellExecutor(BaseExecutor):
    __exec_name__: str = "well"

    # TODO: Add sample option
    def __init__(
        self,
        hole_dir: Union[HoleDirection, str],
        wells_kwargs: Dict = None,
        depths_kwargs: Dict = None,
        fracs_kwargs: Dict = None,
        ips_kwargs: Dict = None,
        stats_kwargs: Dict = None,
        links_kwargs: Dict = None,
        download_kwargs: Dict = None,
        process_kwargs: Dict = None,
    ):
        super().__init__(hole_dir)

        self.model_kwargs = {
            "wells": {**(wells_kwargs or {})},
            "depths": {**(depths_kwargs or {})},
            "fracs": {**(fracs_kwargs or {})},
            "ips": {**(ips_kwargs or {})},
            "stats": {**(stats_kwargs or {})},
            "links": {**(links_kwargs or {})},
        }
        self.download_kwargs = download_kwargs or {}
        self.process_kwargs = process_kwargs or {}

    async def download(
        self,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        create_index: bool = True,
        **kwargs,
    ) -> WellSet:

        kwargs = {**self.download_kwargs, **kwargs}
        try:
            ts = timer()

            # TODO: Add sample option
            wellset = await pd.DataFrame.wells.from_multiple(
                hole_dir=self.hole_dir,
                api14s=api14s,
                api10s=api10s,
                create_index=create_index,
                **kwargs,
            )

            exc_time = round(timer() - ts, 2)
            logger.info(
                f"({self}) downloaded {wellset.wells.shape[0]} wells ({exc_time}s)"
            )
            self.add_metric(
                operation="download",
                name="*",
                time=exc_time,
                count=wellset.wells.shape[0],
            )
            return wellset
        except Exception as e:
            count = len(util.ensure_list(api10s or api14s))
            logger.error(
                f"({self}) error during download: {count} records in batch -- {e}",
                extra={"api10s": api10s, "api14s": api14s},
            )
            raise e

    async def process(
        self,
        wellset: WellSet,
        geoms: WellGeometrySet = None,
        prod_headers: pd.DataFrame = None,
        **kwargs,
    ) -> WellSet:
        kwargs = {**self.process_kwargs, **kwargs}
        try:
            ts = timer()
            wells, depths, fracs, ips, *other = wellset

            # ? source geoms and prod_headers, if not passed
            api14s = wells.util.column_as_set("api14")

            # TODO:  Move to router
            if geoms is None:
                if self.hole_dir == HoleDirection.H:
                    gpath = IHSPath.well_h_geoms
                elif self.hole_dir == HoleDirection.V:
                    gpath = IHSPath.well_v_geoms
                else:
                    raise ValueError("Cant determine geometry request path")

                logger.debug(f"({self}) fetching fresh geometries")
                geoms = await pd.DataFrame.shapes.from_ihs(gpath, api14s=api14s)

            if prod_headers is None:
                prod_headers = await wells.wells.last_prod_date(prefer_local=False)

            # ? begin process

            # depths
            depth_stats = geoms.points.shapes.depth_stats()
            depths_melted = (
                depths.dropna(how="all")
                .reset_index()
                .melt(id_vars=["api14"], var_name="property_name")
            )
            depths_melted["aggregate_type"] = None
            depths_melted["name"] = depths_melted.property_name
            depths_melted = depths_melted.set_index(
                ["api14", "property_name", "aggregate_type"]
            )

            depth_stats = depth_stats.append(depths_melted)
            depths = depth_stats.dropna(subset=["value"])

            # wells
            md_tvd: pd.DataFrame = depths[depths.name.isin(["md", "tvd"])].reset_index(
                level=[1, 2], drop=True
            ).pivot(columns="name")
            md_tvd.columns = md_tvd.columns.droplevel(0)

            wells = wells.join(md_tvd)
            wells["lateral_length"] = geoms.points.shapes.lateral_length()
            wells["provider_status"] = wells.status.str.upper()

            # well status
            wells["status"] = wells.join(prod_headers).wells.assign_status()
            wells["is_producing"] = wells.wells.is_producing()

            lateral_lengths = wells.wells.merge_lateral_lengths()
            fracs = fracs.join(lateral_lengths)
            fracs = fracs.wells.process_fracs()

            wellset = WellSet(
                wells=wells, depths=depths, fracs=fracs, ips=ips, stats=None, links=None
            )

            exc_time = round(timer() - ts, 2)

            total_count = sum([x.shape[0] for x in list(wellset) if x is not None])
            for name, model, df in wellset.items():
                if df is not None and not df.empty:
                    t = round(df.shape[0] * (exc_time / (total_count or 1)), 2)
                    logger.info(
                        f"({self}) {name}: processed {df.shape[0]} records ({t}s)"
                    )
                    self.add_metric(
                        operation="process", name=name, time=t, count=df.shape[0],
                    )
                else:
                    logger.debug(f"({self}) {name}: no records to process")

            return wellset
        except Exception as e:
            api14s = wells.util.column_as_set("api14")
            logger.error(
                f"({self}) error during processing: {len(api14s)} records in batch -- {e}",
                extra={"api14s": api14s},
            )
            raise e

    async def persist(
        self,
        wellset: WellSet,
        wells_kwargs: Dict = None,
        depths_kwargs: Dict = None,
        fracs_kwargs: Dict = None,
        ips_kwargs: Dict = None,
        stats_kwargs: Dict = None,
        links_kwargs: Dict = None,
    ) -> int:

        try:
            coros: List[Coroutine] = []
            for name, model, df in wellset.items():
                if df is not None and not df.empty:
                    kwargs = {}
                    if name == "wells" and wells_kwargs:
                        kwargs = wells_kwargs
                    elif name == "depths" and depths_kwargs:
                        kwargs = depths_kwargs
                    elif name == "fracs" and fracs_kwargs:
                        kwargs = fracs_kwargs
                    elif name == "ips" and ips_kwargs:
                        kwargs = ips_kwargs
                    elif name == "stats" and stats_kwargs:
                        kwargs = stats_kwargs
                    elif name == "links" and links_kwargs:
                        kwargs = links_kwargs
                    logger.info(
                        f"({self}) {name}: scheduling peristance to {model.__name__}"
                    )
                    coros.append(
                        self._persist(
                            name, model, df, **{**self.model_kwargs[name], **kwargs}
                        )
                    )
                else:
                    logger.debug(f"({self}) {name}: no records to persist")

            return sum(await asyncio.gather(*coros))

        except Exception as e:
            api14s = wellset.wells.util.column_as_set("api14")
            logger.error(
                f"({self}) error during persistance: {len(api14s)} records in batch -- {e}",
                extra={"api14s": api14s},
            )
            raise e

    async def _run(
        self,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        return_data: bool = False,
    ) -> Tuple[int, Optional[WellSet]]:

        exec_id = uuid.uuid4().hex
        logger.warning(f"({self}) starting execution {exec_id=}")

        try:
            df = await self.download(api14s=api14s, api10s=api10s)
            ps = await self.process(df)
            ct = await self.persist(ps)
            logger.warning(f"({self}) completed execution {exec_id=}")
            return ct, ps if return_data else None

        except Exception as e:
            logger.error(
                f"({self}) run failed: {exec_id=} -- {e}",
                extra={"api14s": api14s, "api10s": api10s},
            )
            raise e

    async def run(
        self,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        batch_size: int = None,  # max number of ids to process together
        return_data: bool = False,
    ) -> Tuple[List[Tuple[int, Optional[WellSet]]], List[BaseException]]:

        param_count = sum([api14s is not None, api10s is not None])

        if param_count > 1:
            raise ValueError(f"Only one of [api14s, api10s] can be specified")

        elif param_count < 1:
            raise ValueError(f"One of [api14s, api10s] must be specified")

        batch_size = batch_size or 100

        if api14s:
            ids = api14s
            id_type = "api14s"
        elif api10s:
            ids = api10s
            id_type = "api10s"
        else:
            raise ValueError(f"Could not determine id type")

        # generate coros for all chunks
        coros: List[Coroutine] = []
        for chunk in util.chunks(ids, n=batch_size):
            coros.append(self._run(**{id_type: chunk, "return_data": return_data}))

        logger.info(f"created {len(coros)} coroutines from {len(ids)} {id_type}s")

        return exc.split_errors(await asyncio.gather(*coros, return_exceptions=True))

    def run_sync(
        self,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        batch_size: int = None,  # max number of ids to process together
        return_data: bool = False,
    ) -> Tuple[
        List[Tuple[int, Optional[WellSet]]], List[BaseException]
    ]:  # TODO: confusing return type
        loop = asyncio.get_event_loop()
        coro = self.run(
            api14s=api14s, api10s=api10s, batch_size=batch_size, return_data=return_data
        )
        return loop.run_until_complete(coro)


if __name__ == "__main__":
    import loggers
    import calc.prod  # noqa
    from db import db  # noqa

    loggers.config(level=10, formatter="layman")

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
    api10s = [x[:10] for x in api14s]
    ids = ["14207C0155111H", "14207C0155258418H"]
    ids = ["14207C0202511H", "14207C0205231H"]
    entity12s = {x[:12] for x in ids}
    hole_direction = HoleDirection.H

    async def async_wrapper():
        if not db.is_bound():
            await db.startup()

        async def test_prod():
            pexec = ProdExecutor(hole_direction)
            prodset = ProdSet()
            # df = await pexec.download(entity12s=entity12s)
            df = await pexec.download(api10s=api10s, params={"related": True})
            prodset = await pexec.process(df)
            await pexec.persist(prodset)

            # results, errors = pexec.run_sync(
            #     entity12s=entity12s, batch_size=20, return_data=True
            # )

            # affected, prodset = results[0]

        async def test_geoms():
            gexec = GeomExecutor()
            results, errors = gexec.run_sync(api14s=api14s)
            # geoms = await gexec.download(api14s=api14s)
            # geoms = await gexec.process(geoms=geoms)
            # await gexec.persist(geoms=geoms)

        async def test_wells():
            wexec = WellExecutor(hole_direction)
            # results, errors = wexec.run_sync(api14s=api14s)
            wellset = await wexec.download(api14s=api14s)
            wellset_processed = await wexec.process(wellset=wellset)
            await wexec.persist(wellset=wellset_processed)

        async def test_wells_supplemental():
            wexec = WellExecutor(hole_direction)
            wellset = await wexec.download(api14s=api14s)
            geoms = await GeomExecutor(hole_direction).download(api14s=api14s)
            prod = await ProdExecutor(hole_direction).download(api14s=api14s)
            prod_headers = (
                prod.reset_index()
                .groupby("api14")
                .prod_date.max()
                .rename("last_prod_date")
                .to_frame()
            )
            wellset_processed = await wexec.process(
                wellset=wellset, geoms=geoms, prod_headers=prod_headers
            )
            await wexec.persist(wellset=wellset_processed)
