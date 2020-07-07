import abc
from enum import Enum
import numpy as np
import pandas as pd
from talib import abstract
from typing import Any, Dict, List, Optional


class IndicatorParams(Enum):
    slow_ema = "slow_ema"
    fast_ema = "fast_ema"


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
        for fo_key in expected:
            if fo_key not in actual:
                raise IndicatorException(f"expected this key = {fo_key}")

    @abc.abstractmethod
    def apply(self, df, field_name: str) -> None:
        pass


class IndicatorSmaCrossover(BaseIndicator):
    def apply(
        self, df: pd.DataFrame, field_name: str, params: Dict[IndicatorParams, Any]
    ) -> None:
        self.ensure_required_filter_options(
            [IndicatorParams.fast_ema, IndicatorParams.slow_ema], params
        )
        slow_ema = params[IndicatorParams.slow_ema]
        fast_ema = params[IndicatorParams.fast_ema]

        sma = abstract.Function("sma")
        df["slow_sma"] = sma(df.close, timeperiod=slow_ema)
        df["fast_sma"] = sma(df.close, timeperiod=fast_ema)
        df[field_name] = crossover(df.fast_sma - df.slow_sma)
        return df
