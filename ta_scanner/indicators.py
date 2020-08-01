import abc
from enum import Enum
import numpy as np
import pandas as pd
from talib import abstract
from typing import Any, Dict, List, Optional


class IndicatorParams(Enum):
    slow_sma = "slow_sma"
    fast_sma = "fast_sma"


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


class IndicatorException(Exception):
    pass


class BaseIndicator(metaclass=abc.ABCMeta):
    def __init__(self):
        pass

    def ensure_required_filter_options(
        self, expected: List[IndicatorParams], actual: Dict[IndicatorParams, Any]
    ):
        for expected_key in expected:
            if expected_key not in actual:
                indicator_name = self.__class__.__name__
                raise IndicatorException(f"{indicator_name} requires key = {expected_key}")

    @abc.abstractmethod
    def apply(self, df, field_name: str) -> None:
        pass


class IndicatorSmaCrossover(BaseIndicator):
    def apply(
        self, df: pd.DataFrame, field_name: str, params: Dict[IndicatorParams, Any]
    ) -> None:
        self.ensure_required_filter_options(
            [IndicatorParams.fast_sma, IndicatorParams.slow_sma], params
        )
        slow_sma = params[IndicatorParams.slow_sma]
        fast_sma = params[IndicatorParams.fast_sma]

        sma = abstract.Function("sma")
        df["slow_sma"] = sma(df.close, timeperiod=slow_sma)
        df["fast_sma"] = sma(df.close, timeperiod=fast_sma)
        df[field_name] = crossover(df.fast_sma - df.slow_sma)
        return df
