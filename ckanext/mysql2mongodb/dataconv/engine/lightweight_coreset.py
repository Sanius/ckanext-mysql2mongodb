import numpy as np
import pandas as pd


def lightweight_coreset(x: pd.DataFrame, m: int) -> np.array:
    mean_x = np.mean(x, axis=0)
    dist = np.sum(np.power(x - mean_x, 2), axis=1)
    q = 2 / 5 * 1 / x.shape[0] + 3 / 5 * (dist / dist.sum())
    q = q.dropna()
    if q.size == 0:
        return np.arange(0, len(x))
    c = np.random.choice(x.shape[0], size=m, replace=False, p=q)
    c = np.sort(c - 1)
    if c[0] < 0:
        c = np.delete(c, 0)
    return c
