import numpy as np

__all__ = ["log_transform", "spread_countdown"]


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
