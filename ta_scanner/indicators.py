import abc
from enum import Enum
import numpy as np
import pandas as pd
from talib import abstract
from typing import Any, Dict, List, Optional


class IndicatorParams(Enum):
    slow_sma = "slow_sma"
    fast_sma = "fast_sma"
    slow_ema = "slow_ema"
    fast_ema = "fast_ema"
    field_names = "field_names"


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
    def __init__(self, field_name: str, params: Dict[IndicatorParams, Any]):
        self.field_name = field_name
        self.params = params

    def ensure_required_filter_options(
        self, expected: List[IndicatorParams], actual: Dict[IndicatorParams, Any]
    ):
        for expected_key in expected:
            if expected_key not in actual:
                indicator_name = self.__class__.__name__
                raise IndicatorException(
                    f"{indicator_name} requires key = {expected_key}"
                )

    @abc.abstractmethod
    def apply(self, df, field_name: str) -> None:
        pass


class IndicatorSmaCrossover(BaseIndicator):
    def apply(self, df: pd.DataFrame) -> None:
        self.ensure_required_filter_options(
            [IndicatorParams.fast_sma, IndicatorParams.slow_sma], self.params
        )
        slow_sma = self.params[IndicatorParams.slow_sma]
        fast_sma = self.params[IndicatorParams.fast_sma]

        sma = abstract.Function("sma")
        df["slow_sma"] = sma(df.close, timeperiod=slow_sma)
        df["fast_sma"] = sma(df.close, timeperiod=fast_sma)
        df[self.field_name] = crossover(df.fast_sma - df.slow_sma)
        return df


class IndicatorEmaCrossover(BaseIndicator):
    def apply(self, df: pd.DataFrame) -> None:
        self.ensure_required_filter_options(
            [IndicatorParams.fast_ema, IndicatorParams.slow_ema], self.params
        )
        slow_ema = self.params[IndicatorParams.slow_ema]
        fast_ema = self.params[IndicatorParams.fast_ema]

        ema = abstract.Function("ema")
        df["slow_ema"] = ema(df.close, timeperiod=slow_ema)
        df["fast_ema"] = ema(df.close, timeperiod=fast_ema)
        df[self.field_name] = crossover(df.fast_ema - df.slow_ema)
        return df


class CombinedBindary(BaseIndicator):
    def apply(self, df: pd.DataFrame) -> None:
        self.ensure_required_filter_options([IndicatorParams.field_names], self.params)
        field_names = self.params[IndicatorParams.field_names]

        df[self.field_name] = 0
        length = len(field_names)
        field_name_values = [None for _ in range(length)]

        signals = df.loc[df[field_names].isin([1, -1]).any(1)][field_names]

        for i, row in signals.iterrows():
            for ii, fn in enumerate(field_names):
                if row[fn] != 0:
                    field_name_values[ii] = row[fn]
            if abs(sum(filter(None, field_name_values))) == length:
                df.loc[i, self.field_name] = field_name_values[0]
