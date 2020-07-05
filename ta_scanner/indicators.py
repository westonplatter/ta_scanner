import numpy as np
import pandas as pd
from talib import abstract
from typing import Dict


def crossover(series, value=0):
    shift = +1
    series_shifted = series.shift(shift)
    conditions = [
        (series <= value) & (series_shifted >= value),
        (series >= value) & (series_shifted <= value),
    ]
    choices = [-1, +1]
    crossover = np.select(conditions, choices, default=0)
    return crossover


def first_x_after_y(xx, yy):
    watching = False
    results = []

    for index, yy_value in np.ndenumerate(yy):
        value = 0

        if yy_value != 0:
            watching = True

        if watching:
            if xx[index[0]] != 0:
                value = xx[index[0]]
                watching = False

        results.append(value)

    return np.array(results)


class IndicatorSma:
    def __init__(self, name: str, params: Dict):
        self.name = name
        self.params = params

    def apply(self, df, field_name: str) -> None:
        sma = abstract.Function("sma")
        df["slow_sma"] = sma(df.Close, timeperiod=self.params["slow_sma"])
        df["fast_sma"] = sma(df.Close, timeperiod=self.params["fast_sma"])
        df[field_name] = crossover(df.fast_sma - df.slow_sma)
        return df
