import logging
from typing import List, Union

import pandas as pd

import config as conf
from collector import IHSClient, IHSPath
from schemas import ProductionWellSet

logger = logging.getLogger(__name__)


PandasObject = Union[pd.DataFrame, pd.Series]


def _validate_required_columns(required: List[str], columns: List[str]):
    missing = []
    for x in required:
        if x not in columns:
            missing.append(x)

    if len(missing) > 0:
        raise KeyError(f"Missing columns: {missing}")


@pd.api.extensions.register_dataframe_accessor("prodstats")
class ProdStats:
    def __init__(self, obj: PandasObject):
        # self._validate(obj)
        self._obj: PandasObject = obj

    # @staticmethod
    # def _validate(obj: PandasObject):
    #     # verify there is a column latitude and a column longitude
    #     required: List[str] = ["prod_date", "oil", "gas", "water"]
    #     missing: List[str] = [x for x in required if x not in obj.columns]
    #     if len(missing) > 0:
    #         raise AttributeError(f"Must have columns: {missing}")

    @classmethod
    async def from_ihs(
        cls,
        id: Union[str, List[str]],
        path: IHSPath,
        create_index: bool = True,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch production records from the internal IHS service.

        Arguments:
            id {Union[str, List[str]]} -- can be a single or list of producing entities or api10s

        Keyword Arguments:
            params {Dict} -- additional params to pass to client.get() (default: None)
            resource_path {str} -- url resource bath (default: "prod/h")
            timeout {int} -- optional timeout period for production requests

        Returns:
            pd.DataFrame -- DataFrame of monthly production for the given ids
        """
        data = await IHSClient.get_production_wells(id, path=path, **kwargs)
        wellset = ProductionWellSet(wells=data)
        return wellset.df(create_index=True)


@pd.api.extensions.register_dataframe_accessor("prod")
class Prod:
    peak_norm_limit: int = conf.PEAK_NORM_LIMIT

    def __init__(self, obj: PandasObject):
        # self._validate(obj)
        self._obj: PandasObject = obj

    def norm_to_ll(
        self,
        value: int,
        suffix: str,
        columns: List[str] = ["oil", "gas", "water", "boe"],
    ) -> pd.DataFrame:
        """ Normalize to an arbitrary lateral length """

        _validate_required_columns(
            columns + ["prod_month", "perfll"], self._obj.columns
        )

        alias_map = {k: f"{k}_norm_{suffix}" for k in columns}
        factors = (self._obj["perfll"] / value).values
        return self._obj.div(factors, axis=0).loc[:, columns].rename(columns=alias_map)

    def peak30(self):
        _validate_required_columns(["oil", "prod_month"], self._obj.columns)
        first6mo = self._obj.prod_month <= self.peak_norm_limit
        df = self._obj.loc[first6mo, :]

        peak30 = (
            df.loc[
                df.groupby(level=0).oil.idxmax().dropna().values, ["oil", "prod_month"],
            ]
            .reset_index(level=1)
            .rename(
                columns={
                    "prod_date": "peak30_date",
                    "oil": "peak30_oil",
                    "prod_month": "peak30_month",
                }
            )
        )
        peak30["peak30_gas"] = df.groupby(level=0).gas.max()

        return peak30[["peak30_date", "peak30_oil", "peak30_gas", "peak30_month"]]


if __name__ == "__main__":

    import loggers
    import random

    loggers.config()

    async def async_wrapper():
        # id = ["14207C017575", "14207C020251"]
        # # id = ["14207C017575"]
        # id = [
        #     "14207C0155111H",
        #     "14207C0155258418H",
        #     "14207C0155258421H",
        #     "14207C01552617H",
        #     "14207C015535211H",
        #     "14207C015535212H",
        #     "14207C0155368001H",
        #     "14207C0155368002H",
        #     "14207C01558022H",
        #     "14207C0155809H",
        #     "14207C017575",
        #     "14207C020251",
        # ]

        ids = await IHSClient.get_ids("tx-upton", path=IHSPath.prod_h_ids)
        ids = random.choices(ids, k=5)
        df = await pd.DataFrame.prodstats.from_ihs(ids, path=IHSPath.prod_h)
        df.shape

        # df.head(10)
        # df.iloc[0]

        df = df.sort_values(["api14", "status"], ascending=False)

        # df[df.status == "INACTIVE"]

        # df.groupby(level=0).status.unique()

        # df.loc["4246133208"]

        header = df.groupby(level=0).agg(
            {
                "api14": "unique",
                "entity": "unique",
                "entity12": "first",
                "status": "unique",
                "perf_upper": "first",
                "perf_lower": "first",
                "products": "first",
                "perfll": "first",
            }
        )

        monthly = df.copy(deep=True)

        monthly = monthly.loc[
            :, ["oil", "gas", "water", "perfll", "water_cut", "days_in_month", "gor"]
        ]
        monthly["boe"] = monthly.oil + (monthly.gas.div(6))
        monthly = monthly.sort_index(ascending=True)
        monthly["prod_month"] = monthly.groupby(level=0).cumcount() + 1
        # monthly.head(12)

        peak30 = monthly.prod.peak30()

        # peak30["peak30_gas"] = monthly.loc[first6mo, :]

        header = header.join(peak30)
        header["prod_days"] = monthly.groupby(level=0).days_in_month.sum()
        header["prod_months"] = monthly.groupby(level=0).prod_month.max()

        monthly["peak_norm_month"] = monthly.prod_month - header.peak30_month - 1
        monthly["oil_percent"] = monthly.oil.div(monthly.boe)

        monthly = monthly.join(monthly.prod.norm_to_ll(value=1000, suffix="1k"))
        monthly = monthly.join(monthly.prod.norm_to_ll(value=5000, suffix="5k"))
        monthly = monthly.join(monthly.prod.norm_to_ll(value=7500, suffix="7500"))
        monthly = monthly.join(monthly.prod.norm_to_ll(value=10000, suffix="10k"))
        monthly.iloc[10]

        monthly

        # monthly.join()

        # dir(monthly.groupby(level=[0, 1]))
