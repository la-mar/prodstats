import asyncio
import logging
import uuid
from timeit import default_timer as timer
from typing import Coroutine, Dict, List, Optional, Tuple, Union

import pandas as pd

import calc  # noqa
import db.models as models
import util
from calc.sets import DataSet, ProdSet, WellGeometrySet, WellSet
from const import HoleDirection, IHSPath, ProdStatRange
from db import db

logger = logging.getLogger(__name__)


# TODO: there is a better way to pass kwargs into run() and arun()


class BaseExecutor:
    __exec_name__: str = "base"

    def __init__(
        self,
        hole_dir: Union[HoleDirection, str],
        download_kwargs: Dict = None,
        process_kwargs: Dict = None,
        persist_kwargs: Dict = None,
    ):

        self.hole_dir: HoleDirection = HoleDirection(hole_dir)
        self.download_kwargs = download_kwargs or {}
        self.process_kwargs = process_kwargs or {}
        self.persist_kwargs = persist_kwargs or {}

        self.metrics: pd.DataFrame = pd.DataFrame(
            columns=[
                "executor",
                "hole_direction",
                "operation",
                "name",
                "seconds",
                "count",
            ]
        )

    def __repr__(self):
        return f"{self.__class__.__name__}[{self.hole_dir.value}]"

    def raise_execution_error(
        self, operation: str, record_count: int, e: Exception, extra: Dict = None
    ):
        logger.error(
            f"({self}) error during {operation}ing: {record_count} records in batch -- {e.__class__.__name__}: {e}",  # noqa
            extra=extra,
        )
        raise e

    def add_metric(self, operation: str, name: str, seconds: float, count: int):
        logger.info(
            f"({self}) {operation}ed {count}{f' {name}' if name != '*' else ''} records ({seconds}s)",  # noqa
            extra={"duration": seconds},
        )

        idx_max = self.metrics.index.max()
        idx_max = idx_max + 1 if not pd.isna(idx_max) else 0
        self.metrics = self.metrics.append(
            pd.Series(
                {
                    "executor": self.__exec_name__,
                    "hole_direction": self.hole_dir,
                    "operation": operation,
                    "name": name,
                    "seconds": seconds,
                    "count": count,
                },
                name=idx_max,
            )
        )

    async def download(self, **kwargs,) -> DataSet:
        raise NotImplementedError

    async def process(self, dataset: DataSet, **kwargs) -> DataSet:
        raise NotImplementedError

    async def _persist(
        self, name: str, model: models.Model, df: Optional[pd.DataFrame], **kwargs
    ) -> int:

        count = 0
        if df is not None and not df.empty:
            logger.debug(
                f"({self}) scheduling persistance of {df.shape[0]} {name} records)"
            )

            ts = timer()
            count = await model.bulk_upsert(df, **kwargs)
            exc_time = round(timer() - ts, 2)
            self.add_metric(
                operation="persist", name=name, seconds=exc_time, count=df.shape[0],
            )
        else:
            logger.info(f"({self}) nothing to persist")
        return count

    async def persist(self, dataset: DataSet, **kwargs) -> int:
        raise NotImplementedError

    # TODO: download_kwargs, process_kwargs, persist_kwargs
    async def arun(self, **kwargs) -> Tuple[int, Optional[DataSet]]:

        return_data = kwargs.pop("return_data", True)
        persist = kwargs.pop("persist", True)

        exec_id = uuid.uuid4().hex

        try:
            ts = timer()
            logger.info(f"({self}) execution started {exec_id=}")
            ds: DataSet = await self.download(**kwargs)
            ds_proc = await self.process(ds)
            if persist:
                ct = await self.persist(ds_proc)
            else:
                logger.info(f"({self}) skipping persistance {exec_id=}")
                ct = 0
            exc_time = round(timer() - ts, 2)
            logger.info(
                f"({self}) execution completed {exec_id=}",
                extra={"duration": exc_time, **kwargs},
            )
            return ct, ds_proc if return_data else None

        except Exception as e:
            exc_time = round(timer() - ts, 2)
            logger.error(
                f"({self}) execution failed: {exec_id=} -- {e} ||||| {kwargs=}",
                extra={"duration": exc_time, **kwargs},
            )
            raise e

    def run(self, **kwargs) -> Tuple[int, Optional[DataSet]]:
        loop = asyncio.get_event_loop()
        coro = self.arun(**kwargs)
        return loop.run_until_complete(coro)


class ProdExecutor(BaseExecutor):
    __exec_name__: str = "production"

    def __init__(
        self,
        hole_dir: Union[HoleDirection, str],
        header_kwargs: Dict = None,
        monthly_kwargs: Dict = None,
        stats_kwargs: Dict = None,
        **kwargs,
    ):
        super().__init__(hole_dir, **kwargs)
        self.model_kwargs = {
            "header": {**(header_kwargs or {})},
            "monthly": {**(monthly_kwargs or {})},
            "stats": {"batch_size": 1000, **(stats_kwargs or {})},
        }

    async def download(
        self,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        entities: Union[str, List[str]] = None,
        entity12s: Union[str, List[str]] = None,
        **kwargs,
    ) -> DataSet:
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

            prodset: ProdSet = await pd.DataFrame.prodstats.from_ihs(
                path=path,
                api14s=api14s,
                api10s=api10s,
                entities=entities,
                entity12s=entity12s,
                **kwargs,
            )
            exc_time = round(timer() - ts, 2)

            self.add_metric(
                operation="download",
                name="*",
                seconds=exc_time,
                count=prodset.header.shape[0],
            )

            return prodset

        except Exception as e:
            self.raise_execution_error(
                operation="download",
                record_count=len(
                    util.ensure_list(entities or entity12s or api14s or api10s or [])
                ),
                e=e,
                extra={
                    "entities": entities,
                    "entity12s": entity12s,
                    "api14s": api14s,
                    "api10s": api10s,
                },
            )
            raise e

    def _process_monthly(
        self, prodset: ProdSet, prod_columns: List[str] = ["oil", "gas", "water", "boe"]
    ) -> ProdSet:
        """ Calculate the monthly production for the given ProdSet. A new ProdSet object
            is returned. The input ProdSet is not mutated.

        Arguments:
            prodset {ProdSet} -- must contain at least a monthly DataFrame

        Keyword Arguments:
            prod_columns {List[str]} -- list of column names to be considered as monthly
                production values (default: ["oil", "gas", "water", "boe"])

        Returns:
            ProdSet
        """
        # prodset = ProdSet(*prodset)  # copy
        monthly: pd.DataFrame = prodset.monthly

        if monthly is not None and not monthly.empty:
            # TODO: timeit
            logger.debug(f"(Production) calculating monthly production...")
            monthly["boe"] = monthly.prodstats.boe()
            monthly["oil_percent"] = monthly.prodstats.oil_percent()
            monthly["prod_days"] = monthly.prodstats.prod_days()
            monthly["peak_norm_month"] = monthly.prodstats.peak_norm_month()
            monthly["peak_norm_days"] = monthly.prodstats.peak_norm_days()

            # * avg daily prod by month
            monthly = monthly.join(monthly.prodstats.daily_avg_by_month(prod_columns))

            # * normalize to various lateral lengths
            perfll = monthly.perfll
            per1k = monthly[prod_columns].prodstats.norm_to_ll(1000, perfll)
            per3k = monthly[prod_columns].prodstats.norm_to_ll(3000, perfll)
            per5k = monthly[prod_columns].prodstats.norm_to_ll(5000, perfll)
            per7k = monthly[prod_columns].prodstats.norm_to_ll(
                7500, perfll, suffix="7500"
            )
            per10k = monthly[prod_columns].prodstats.norm_to_ll(10000, perfll)

            monthly = pd.concat([monthly, per1k, per3k, per5k, per7k, per10k], axis=1)
            # monthly = monthly.drop(columns=["perfll"])

            prodset.monthly = monthly
        else:
            logger.info(f"(Production) no monthly production to calculate")

        return prodset

    def _process_headers(self, prodset: ProdSet) -> ProdSet:
        # prodset = ProdSet(*prodset)  # copy
        header: pd.DataFrame = prodset.header
        monthly: pd.DataFrame = prodset.monthly

        has_headers = header is not None and not header.empty
        has_monthly = monthly is not None and not monthly.empty

        if has_headers and has_monthly:
            # TODO: timeit
            logger.debug(f"(Production) enriching production headers")
            header = header.join(monthly.prodstats.peak30())
            header = header.join(monthly.prodstats.prod_dates_by_well())
            pdp = monthly.prodstats.pdp_by_well(
                range_name=ProdStatRange.LAST,
                months=3,
                dollars_per_bbl=30000,
                factor=0.75,
            )
            header = header.join(pdp)

            prodset.header = header
        else:
            logger.info(
                f"(Production) no production headers to process {has_headers=} {has_monthly=}"
            )

        return prodset

    def _process_prodstats(
        self,
        monthly: pd.DataFrame,
        agg_type: str = "sum",
        norm_value: int = None,
        norm_suffix: str = None,
        prod_columns: List[str] = ["oil", "gas", "water", "boe"],
        option_sets: List[Tuple[ProdStatRange, int, bool]] = None,
    ) -> pd.DataFrame:

        # * oil/gas/water/boe
        if option_sets is None:
            option_sets = calc.PRODSTAT_DEFAULT_OPTIONS
            logger.debug(
                f"({self}) prodstats: using default option_sets -- {option_sets=}"
            )
        else:
            logger.debug(
                f"({self}) prodstats: using passed option_sets -- {option_sets=}"
            )

        logger.debug(
            f"({self}) prodstats: calculating {agg_type=} {norm_value=} option_sets={len(option_sets)}"  # noqa
        )

        prodstat_dfs: List[pd.DataFrame] = []
        if monthly is not None and not monthly.empty:
            for range_name, months, include_zeroes in option_sets:
                prodstat_dfs.append(
                    monthly.prodstats.calc_prodstat(
                        range_name=range_name,
                        columns=prod_columns,
                        months=months,
                        agg_type=agg_type,
                        include_zeroes=include_zeroes,
                        norm_value=norm_value,
                        norm_suffix=norm_suffix,
                    )
                )

            return pd.concat(prodstat_dfs, axis=0)
        else:
            return pd.DataFrame(
                columns=[
                    "start_month",
                    "end_month",
                    "start_date",
                    "end_date",
                    "value",
                    "includes_zeroes",
                    "ll_norm_value",
                    "is_ll_norm",
                    "is_peak_norm",
                    "aggregate_type",
                    "property_name",
                    "comments",
                    "api10",
                    "name",
                ]
            ).set_index(["api10", "name"])

    def _process_prodstat_ratios(
        self,
        monthly: pd.DataFrame,
        prod_columns: List[str] = ["oil", "gas", "water", "boe"],
        option_sets: List[Tuple[ProdStatRange, int, bool]] = None,
    ):

        if option_sets is None:
            option_sets = calc.PRODSTAT_DEFAULT_RATIO_OPTIONS
            logger.debug(
                f"({self}) prodstats: using default ratio option_sets -- {option_sets=}"
            )
        else:
            logger.debug(
                f"({self}) prodstats: using passed ratio option_sets -- {option_sets=}"
            )

        logger.debug(f"(Production) calculating prodstat ratios")

        # * gor/oil_percent/avg_daily
        gor_dfs: List[pd.DataFrame] = []
        oil_percent_dfs: List[pd.DataFrame] = []
        avgdaily_dfs: List[pd.DataFrame] = []
        for range_name, months, include_zeroes in option_sets:
            kwargs = {
                "range_name": range_name,
                "months": months,
                "include_zeroes": include_zeroes,
            }
            gor_dfs.append(monthly.prodstats.gor_by_well(**kwargs))
            oil_percent_dfs.append(monthly.prodstats.oil_percent_by_well(**kwargs))
            for col in prod_columns:
                avgdaily_dfs.append(
                    monthly.prodstats.avg_daily_by_well(numerator=col, **kwargs)
                )

        return pd.concat(gor_dfs + oil_percent_dfs + avgdaily_dfs, axis=0)

    async def process(
        self,
        dataset: DataSet,
        prod_columns: List[str] = ["oil", "gas", "water", "boe"],
        prodstat_opts: List[Tuple[ProdStatRange, int, bool]] = None,
        ratio_opts: List[Tuple[ProdStatRange, int, bool]] = None,
        **kwargs,
    ) -> ProdSet:
        kwargs = {**self.process_kwargs, **kwargs}
        prodstat_opts = prodstat_opts or kwargs.pop("prodstat_opts", None)
        ratio_opts = ratio_opts or kwargs.pop("ratio_opts", None)

        ts = timer()

        try:

            dataset = self._process_monthly(dataset)
            dataset = self._process_headers(dataset)
            monthly = dataset.monthly

            prodstats = pd.concat(
                [
                    self._process_prodstats(
                        monthly, prod_columns=prod_columns, option_sets=prodstat_opts,
                    ),
                    self._process_prodstats(
                        monthly,
                        norm_value=1000,
                        prod_columns=prod_columns,
                        option_sets=prodstat_opts,
                    ),
                    self._process_prodstat_ratios(
                        monthly, prod_columns=prod_columns, option_sets=ratio_opts,
                    ),
                ],
                axis=0,
            )

            dataset.stats = prodstats

            if "perfll" in dataset.monthly.columns:
                dataset.monthly = dataset.monthly.drop(columns=["perfll"])

            exc_time = round(timer() - ts, 2)

            total_count = sum([x.shape[0] for x in list(dataset) if x is not None])
            for name, model, df in dataset.items():
                if df is not None and not df.empty:
                    seconds = round(df.shape[0] * (exc_time / (total_count or 1)), 2)
                    self.add_metric(
                        operation="process",
                        name=name,
                        seconds=seconds,
                        count=df.shape[0],
                    )

            logger.debug(f"(Production) processing finished")
            return dataset

        except Exception as e:
            api10s = dataset.header.util.column_as_set("api10")
            self.raise_execution_error(
                operation="process",
                record_count=len(api10s),
                e=e,
                extra={"api10s": api10s},
            )
            raise e

    async def persist(
        self,
        dataset: ProdSet,
        header_kwargs: Dict = None,
        monthly_kwargs: Dict = None,
        stats_kwargs: Dict = None,
        **kwargs,
    ) -> int:

        try:
            coros: List[Coroutine] = []
            for name, model, df in dataset.items():
                if name == "header" and header_kwargs:
                    kwargs.update(header_kwargs)
                elif name == "monthly" and monthly_kwargs:
                    kwargs.update(monthly_kwargs)
                elif name == "stats" and stats_kwargs:
                    kwargs.update(stats_kwargs)

                coros.append(
                    self._persist(
                        name, model, df, **{**self.model_kwargs[name], **kwargs}
                    )
                )

            return sum(await asyncio.gather(*coros))

        except Exception as e:
            api10s = dataset.header.util.column_as_set("api10")
            self.raise_execution_error(
                operation="persist",
                record_count=len(api10s),
                e=e,
                extra={"api10s": api10s},
            )
            raise e

    async def arun(
        self,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        return_data: bool = False,
        entities: Union[str, List[str]] = None,
        entity12s: Union[str, List[str]] = None,
        **kwargs,
    ) -> Tuple[int, Optional[DataSet]]:

        param_count = sum(
            [
                entities is not None,
                api14s is not None,
                api10s is not None,
                entity12s is not None,
            ]
        )

        if param_count > 1:
            raise ValueError(
                f"Only one of [area_name, api10s, api14s, entity12s, entities] can be specified"
            )

        elif param_count < 1:
            raise ValueError(
                f"One of [area_name, api10s, api14s, entity12s, entities] must be specified"
            )

        return await super().arun(
            api14s=api14s,
            api10s=api10s,
            return_data=return_data,
            entities=entities,
            entity12s=entity12s,
            **kwargs,
        )

    def run(
        self,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        entities: Union[str, List[str]] = None,
        entity12s: Union[str, List[str]] = None,
        return_data: bool = False,
        **kwargs,
    ) -> Tuple[int, Optional[DataSet]]:

        return super().run(
            entities=entities,
            api14s=api14s,
            api10s=api10s,
            entity12s=entity12s,
            return_data=return_data,
        )


class GeomExecutor(BaseExecutor):
    __exec_name__: str = "geometry"

    def __init__(
        self,
        hole_dir: Union[HoleDirection, str],
        locations_kwargs: Dict = None,
        surveys_kwargs: Dict = None,
        points_kwargs: Dict = None,
        **kwargs,
    ):
        super().__init__(hole_dir, **kwargs)
        self.model_kwargs = {
            "locations": {**(locations_kwargs or {})},
            "surveys": {**(surveys_kwargs or {})},
            "points": {"batch_size": 1000, **(points_kwargs or {})},
        }

    async def download(
        self,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
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
                path=path, api14s=api14s, api10s=api10s, **kwargs,
            )
            exc_time = round(timer() - ts, 2)
            self.add_metric(
                operation="download",
                name="*",
                seconds=exc_time,
                count=geoms.locations.shape[0],
            )

            return geoms

        except Exception as e:
            count = len(util.ensure_list(api10s or api14s))
            self.raise_execution_error(
                operation="download",
                record_count=count,
                e=e,
                extra={"api10s": api10s, "api14s": api14s},
            )

    async def process(self, dataset: WellGeometrySet, **kwargs) -> WellGeometrySet:
        kwargs = {**self.process_kwargs, **kwargs}
        try:
            ts = timer()
            locations, surveys, points = dataset

            if self.hole_dir == HoleDirection.H:  # TODO: Move to router
                if (
                    surveys is not None
                    and points is not None
                    and not surveys.empty
                    and not points.empty
                ):
                    points = points.shapes.index_survey_points()
                    kops = points.shapes.find_kop()
                    points = points.join(kops)

                    # surveys
                    laterals = points[points.is_in_lateral].shapes.as_line(
                        label="lateral_only"
                    )
                    sticks = points.shapes.as_stick()
                    bent_sticks = points.shapes.as_bent_stick()
                    surveys = surveys.join(laterals).join(sticks).join(bent_sticks)
                else:
                    api14s = locations.util.column_as_set("api14")
                    logger.warning(
                        f"({self}) skipped processing of {len(api14s)} surveys)",
                        extra={"api14s": api14s},
                    )

            if locations is not None and not locations.empty:
                locations["lon"] = locations.geom.apply(lambda pt: pt.x if pt else None)
                locations["lat"] = locations.geom.apply(lambda pt: pt.y if pt else None)

            geomset = WellGeometrySet(
                locations=locations, surveys=surveys, points=points
            )
            exc_time = round(timer() - ts, 2)
            total_count = sum([x.shape[0] for x in list(geomset) if x is not None])

            for name, model, df in geomset.items():
                if df is not None and not df.empty:
                    seconds = round(df.shape[0] * (exc_time / (total_count or 1)), 2)
                    self.add_metric(
                        operation="process",
                        name=name,
                        seconds=seconds,
                        count=df.shape[0],
                    )

            return geomset

        except Exception as e:
            api14s = dataset.locations.util.column_as_set("api14")
            self.raise_execution_error(
                operation="process",
                record_count=len(api14s),
                e=e,
                extra={"api14s": api14s},
            )

    async def persist(
        self,
        dataset: WellGeometrySet,
        locations_kwargs: Dict = None,
        surveys_kwargs: Dict = None,
        points_kwargs: Dict = None,
        **kwargs,
    ) -> int:

        try:
            dataset = dataset.shapes_as_wkb()

            coros: List[Coroutine] = []
            for name, model, df in dataset.items():
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

            result: int = sum(await asyncio.gather(*coros))

        except Exception as e:
            api14s = dataset.locations.util.column_as_set("api14")
            self.raise_execution_error(
                operation="persist",
                record_count=len(api14s),
                e=e,
                extra={"api14s": api14s},
            )

        return result

    async def arun(
        self,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        return_data: bool = False,
        **kwargs,
    ) -> Tuple[int, Optional[WellGeometrySet]]:

        param_count = sum([api14s is not None, api10s is not None])

        if param_count > 1:
            raise ValueError(f"Only one of [api14s, api10s] can be specified")

        elif param_count < 1:
            raise ValueError(f"One of [api14s, api10s] must be specified")

        return await super().arun(
            api14s=api14s, api10s=api10s, return_data=return_data, **kwargs
        )

    def run(
        self,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        return_data: bool = False,
        **kwargs,
    ) -> Tuple[int, Optional[WellGeometrySet]]:

        return super().run(api14s=api14s, api10s=api10s, return_data=return_data,)


class WellExecutor(BaseExecutor):
    __exec_name__: str = "well"

    def __init__(
        self,
        hole_dir: Union[HoleDirection, str],
        wells_kwargs: Dict = None,
        depths_kwargs: Dict = None,
        fracs_kwargs: Dict = None,
        ips_kwargs: Dict = None,
        stats_kwargs: Dict = None,
        links_kwargs: Dict = None,
        **kwargs,
    ):
        super().__init__(hole_dir, **kwargs)

        self.model_kwargs = {
            "wells": {**(wells_kwargs or {})},
            "depths": {**(depths_kwargs or {})},
            "fracs": {**(fracs_kwargs or {})},
            "ips": {**(ips_kwargs or {})},
            "stats": {**(stats_kwargs or {})},
            "links": {**(links_kwargs or {})},
        }

    async def download(
        self,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        **kwargs,
    ) -> WellSet:

        kwargs = {**self.download_kwargs, **kwargs}
        try:
            ts = timer()

            # TODO: Add sample option
            wellset = await pd.DataFrame.wells.from_multiple(
                hole_dir=self.hole_dir, api14s=api14s, api10s=api10s, **kwargs,
            )

            exc_time = round(timer() - ts, 2)
            self.add_metric(
                operation="download",
                name="*",
                seconds=exc_time,
                count=wellset.wells.shape[0],
            )

            return wellset

        except Exception as e:
            count = len(util.ensure_list(api10s or api14s))
            self.raise_execution_error(
                operation="download",
                record_count=count,
                e=e,
                extra={"api10s": api10s, "api14s": api14s},
            )

    async def process(
        self,
        dataset: WellSet,
        use_local_geoms: bool = False,
        use_local_prod: bool = False,
        geoms: WellGeometrySet = None,
        prod_headers: pd.DataFrame = None,
        **kwargs,
    ) -> WellSet:
        kwargs = {
            **self.process_kwargs,
            **kwargs,
        }  # FIXME: where should this be used now?
        try:
            ts = timer()
            wells, depths, fracs, ips, *other = dataset

            # ? source geoms and prod_headers, if not passed
            api14s = wells.util.column_as_set("api14")

            if self.hole_dir == HoleDirection.H:  # TODO:  Move to router
                gpath = IHSPath.well_h_geoms
                prodpath = IHSPath.prod_h_headers
                if geoms is None:
                    logger.debug(f"({self}) fetching fresh geometries")
                    geoms = await pd.DataFrame.shapes.from_ihs(
                        gpath, api14s=api14s, dispatch=kwargs.get("geoms_dispatch"),
                    )
                    logger.warning(f"geoms: {geoms}")

            elif self.hole_dir == HoleDirection.V:  # TODO:  Move to router
                prodpath = IHSPath.prod_v_headers

            if prod_headers is None:
                logger.debug(f"({self}) fetching fresh production headers")
                prod_headers = await wells.wells.last_prod_date(
                    path=prodpath,
                    prefer_local=use_local_prod,
                    dispatch=kwargs.get("prod_headers_dispatch"),
                )

            #  * process depths
            if self.hole_dir == HoleDirection.H:
                if depths is not None:
                    depth_stats: pd.DataFrame = depths.wells.melt_depths()

                if geoms and geoms.points is not None and not geoms.points.empty:
                    depth_stats = depth_stats.append(geoms.points.shapes.depth_stats())
                    wells["lateral_length"] = geoms.points.shapes.lateral_length()

                md_tvd: pd.DataFrame = depth_stats[
                    depth_stats.name.isin(["md", "tvd"])
                ].reset_index(level=[1, 2], drop=True).pivot(columns="name")
                md_tvd.columns = md_tvd.columns.droplevel(0)

                # combine md and tvd columns from geoms and header, preferring header
                md_tvd = depths.loc[:, ["md", "tvd"]].combine_first(md_tvd)
                wells = wells.join(md_tvd)
                depths = depth_stats.dropna(subset=["value"])

            elif self.hole_dir == HoleDirection.V:
                if depths is not None and not depths.empty:
                    # copy md to tvd where tvd is missing
                    depths.tvd = depths.tvd.combine_first(depths.md)
                    md_tvd = depths.loc[:, ["md", "tvd"]]
                    depth_stats = depths.wells.melt_depths()
                    wells = wells.join(md_tvd)
                    depths = depth_stats.dropna(subset=["value"])

            # * norm ip prod values
            if ips is not None and not ips.empty:
                ip_norm_cols = ["oil", "gas", "water", "perfll"]
                ip_norms = ips.loc[:, ip_norm_cols].prodstats.norm_to_ll(10000)
                ips = ips.join(ip_norms)

            # * determine well status
            wells["provider_status"] = wells.status.str.upper()
            if wells is not None and not wells.empty:
                wells["status"] = wells.join(prod_headers).wells.assign_status()
                wells["is_producing"] = wells.wells.is_producing()

            # * process fracs
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
                    seconds = round(df.shape[0] * (exc_time / (total_count or 1)), 2)
                    self.add_metric(
                        operation="process",
                        name=name,
                        seconds=seconds,
                        count=df.shape[0],
                    )

            return wellset

        except Exception as e:
            api14s = wells.util.column_as_set("api14")
            self.raise_execution_error(
                operation="process",
                record_count=len(api14s),
                e=e,
                extra={"api14s": api14s},
            )

    async def persist(
        self,
        dataset: WellSet,
        wells_kwargs: Dict = None,
        depths_kwargs: Dict = None,
        fracs_kwargs: Dict = None,
        ips_kwargs: Dict = None,
        stats_kwargs: Dict = None,
        links_kwargs: Dict = None,
        **kwargs,
    ) -> int:

        try:
            coros: List[Coroutine] = []
            for name, model, df in dataset.items():
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

            result: int = sum(await asyncio.gather(*coros))

        except Exception as e:
            api14s = dataset.wells.util.column_as_set("api14")
            self.raise_execution_error(
                operation="persist",
                record_count=len(api14s),
                e=e,
                extra={"api14s": api14s},
            )

        return result

    async def arun(
        self,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        return_data: bool = False,
        **kwargs,
    ) -> Tuple[int, Optional[WellSet]]:

        param_count = sum([api14s is not None, api10s is not None])

        if param_count > 1:
            raise ValueError(f"Only one of [api14s, api10s] can be specified")

        elif param_count < 1:
            raise ValueError(f"One of [api14s, api10s] must be specified")

        return await super().arun(
            api14s=api14s, api10s=api10s, return_data=return_data, **kwargs
        )

    def run(
        self,
        api14s: Union[str, List[str]] = None,
        api10s: Union[str, List[str]] = None,
        return_data: bool = False,
        **kwargs,
    ) -> Tuple[int, Optional[WellSet]]:

        return super().run(
            api14s=api14s, api10s=api10s, return_data=return_data, **kwargs
        )


if __name__ == "__main__":
    import loggers
    import calc.prod  # noqa
    from db import db  # noqa
    from collector import IHSClient

    # import itertools
    # import multiprocessing as mp

    loggers.config(level=10, formatter="funcname")

    ranges = ProdStatRange.PEAKNORM
    months = [3, 6]
    include_zeroes = [True, False]
    calc.prodstat_option_matrix(
        ranges=ranges, months=months, include_zeroes=include_zeroes
    )

    # def get_id_sets(area: str) -> Tuple[List[str], List[str]]:
    #     loop = asyncio.get_event_loop()
    #     coroh = IHSClient.get_ids_by_area(path=IHSPath.well_h_ids, area=area)
    #     corov = IHSClient.get_ids_by_area(path=IHSPath.well_v_ids, area=area)
    #     return loop.run_until_complete(asyncio.gather(coroh, corov))

    # area = "tx-upton"
    # sample_size = 25

    # api14h, api14v = get_id_sets(area)
    # h_sample: List[str] = random.choices(api14h, k=sample_size)
    # v_sample: List[str] = random.choices(api14v, k=sample_size)

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

    ids = ["14207C0155111H", "14207C0155258418H"]
    ids = ["14207C0202511H", "14207C0205231H"]
    entity12s = {x[:12] for x in ids}
    hole_direction = HoleDirection.H
    # from db import db
    # import util

    # ! TEST PROD EXECUTOR

    util.aio.async_to_sync(db.startup())

    async def async_wrapper():
        if not db.is_bound():
            await db.startup()

        async def test_well_horizontal_tiny_batch():
            if not db.is_bound():
                await db.startup()
            wexec = WellExecutor(HoleDirection.H)

            wellsets = []
            for api14 in deo_api14h:
                try:
                    # api14 = "42461411280000"
                    wellset = await wexec.download(api14s=[api14])
                    wellset_processed = await wexec.process(wellset)
                    await wexec.persist(wellset_processed)
                except Exception:
                    wellsets.append(wellset_processed)

        async def test_well_vertical_tiny_batch():
            if not db.is_bound():
                await db.startup()
            wexec = WellExecutor(HoleDirection.V)

            wellsets = []
            for api14 in deo_api14v:
                try:
                    # api14 = "42461411280000"
                    wellset = await wexec.download(api14s=[api14])
                    wellset_processed = await wexec.process(wellset)
                    await wexec.persist(wellset_processed)
                except Exception:
                    wellsets.append(wellset_processed)

        async def test_geom_tiny_batch():
            if not db.is_bound():
                await db.startup()
            pexec = GeomExecutor(HoleDirection.H)

            for api14 in deo_api14h:
                try:
                    ds: WellGeometrySet = await pexec.download(api14s=[api14])
                    ps: WellGeometrySet = await pexec.process(ds)
                    await pexec.persist(ps)
                except Exception:
                    pass

        async def test_driftwood_horizontals():
            if not db.is_bound():
                await db.startup()
            wexec = WellExecutor(HoleDirection.H)
            wellset = await wexec.download(api14s=deo_api14h)
            wellset_processed = await wexec.process(wellset)
            await wexec.persist(wellset_processed)

        async def test_driftwood_production():
            if not db.is_bound():
                await db.startup()
            pexec = ProdExecutor(HoleDirection.H)
            ds: DataSet = await pexec.download(api14s=deo_api14h)
            ps: ProdSet = await pexec.process(ds)
            await pexec.persist(ps)

        async def test_driftwood_geoms():
            if not db.is_bound():
                await db.startup()
            pexec = GeomExecutor(HoleDirection.H)
            ds: WellGeometrySet = await pexec.download(api14s=deo_api14h)
            # ds: WellGeometrySet = await pexec.download(api14s=["42461412110000"])
            ps: WellGeometrySet = await pexec.process(ds)
            await pexec.persist(ps)

        async def test_well_vertical():
            if not db.is_bound():
                await db.startup()
            wexec = WellExecutor(HoleDirection.V)
            # results, errors = wexec.run_sync(api14s=api14s)
            api14s = await IHSClient.get_ids_by_area(
                path=IHSPath.well_v_ids, area="tx-upton"
            )
            api14s = api14s[:25]
            wellset = await wexec.download(api14s=api14s)
            wellset_processed = await wexec.process(wellset)
            await wexec.persist(wellset_processed)

        async def test_production_vertical():
            if not db.is_bound():
                await db.startup()

            api14s = await IHSClient.get_ids_by_area(
                path=IHSPath.well_v_ids, area="tx-upton"
            )
            api14s = api14s[:5]

            pexec = ProdExecutor(HoleDirection.V)
            entities = await IHSClient.get_ids_by_area(
                path=IHSPath.prod_v_ids, area="tx-upton"
            )
            entities = entities[:10]
            ds: DataSet = await pexec.download(api14s=api14s)
            ps: ProdSet = await pexec.process(ds)
            await pexec.persist(ps)

        async def test_well_horizontals():
            if not db.is_bound():
                await db.startup()
            wexec = WellExecutor(HoleDirection.H)
            # results, errors = wexec.run_sync(api14s=api14s)
            api14s = await IHSClient.get_ids_by_area(
                path=IHSPath.well_h_ids, area="tx-upton"
            )
            api14s = api14s[:100]
            ws = await wexec.download(api14s=api14s)
            # ws = await wexec.download(api10s=api10s)
            wsproc = await wexec.process(ws)
            await wexec.persist(wsproc)

        async def test_production_horizontal():
            if not db.is_bound():
                await db.startup()

            hole_dir = HoleDirection.H
            id_path = IHSPath.prod_h_ids

            pexec = ProdExecutor(hole_dir)
            entities = await IHSClient.get_ids_by_area(path=id_path, area="tx-upton")
            entities = entities[:25]
            # entity12s = [x[:12] for x in entities]
            ds: DataSet = await pexec.download(entities=entities)
            ps: ProdSet = await pexec.process(ds)
            await pexec.persist(ps)

        # async def test_wells_supplemental():
        #     wexec = WellExecutor(hole_direction)
        #     wellset = await wexec.download(api14s=api14s)
        #     geoms = await GeomExecutor(hole_direction).download(api14s=api14s)
        #     prod = await ProdExecutor(hole_direction).download(api14s=api14s)
        #     prod_headers = (
        #         prod.reset_index()
        #         .groupby("api14")
        #         .prod_date.max()
        #         .rename("last_prod_date")
        #         .to_frame()
        #     )
        #     wellset_processed = await wexec.process(
        #         wellset=wellset, geoms=geoms, prod_headers=prod_headers
        #     )
        #     await wexec.persist(wellset=wellset_processed)

        async def sync_all():
            if not db.is_bound():
                await db.startup()

            loggers.config(level=20)

            for hole_direction in [HoleDirection.V]:
                if hole_direction == HoleDirection.H:
                    ids_path = IHSPath.well_h_ids
                else:
                    ids_path = IHSPath.well_v_ids

                areas = await IHSClient.get_areas(path=ids_path)
                areas = ["tx-upton", "tx-reagan", "tx-midland"]

                counts = []
                datasets = []

                batch_size = 100
                for area in areas:
                    logger.warning(f"running area: {area}")
                    api14s = await IHSClient.get_ids_by_area(path=ids_path, area=area)

                    for chunk in util.chunks(api14s, n=batch_size):
                        for executor in [WellExecutor, GeomExecutor, ProdExecutor]:
                            try:
                                count, dataset = executor(hole_direction).run(
                                    api14s=chunk
                                )
                                counts.append(count)
                                datasets.append(dataset)
                            except Exception as e:
                                print(e)

    # * multiprocessing, lol
    # hole_directions: List[HoleDirection] = HoleDirection.members()
    # # hole_directions: List[HoleDirection] = [HoleDirection.H]
    # executors = [WellExecutor, GeomExecutor, ProdExecutor]
    # # executors = [WellExecutor]
    # executors = [x(y) for x, y in list(itertools.product(executors, hole_directions))]
    # samples = [[x for x in h_sample], [x for x in v_sample]] * 3
    # starargs = list(zip(executors, samples))

    # starargs[0][1] is h_sample

    # pool = mp.Pool(processes=len(executors))
    # results = pool.starmap(test_executor_arun, starargs)
    # pool.close()
    # print(f"\n\n{results}\n\n")
