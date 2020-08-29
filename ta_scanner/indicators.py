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
    trailing_bars = "trailing_bars"
    from_field_name = "from_field_name"


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
        self, expected: List[IndicatorParams]
    ):
        for expected_key in expected:
            if expected_key not in self.params:
                indicator_name = self.__class__.__name__
                raise IndicatorException(
                    f"{indicator_name} requires key = {expected_key}"
                )

    @abc.abstractmethod
    def apply(self, df, field_name: str) -> None:
        pass


class IndicatorSmaCrossover(BaseIndicator):
    def apply(self, df: pd.DataFrame) -> None:
        self.ensure_required_filter_options([IndicatorParams.fast_sma, IndicatorParams.slow_sma])
        slow_sma = self.params[IndicatorParams.slow_sma]
        fast_sma = self.params[IndicatorParams.fast_sma]

        sma = abstract.Function("sma")
        df["slow_sma"] = sma(df.close, timeperiod=slow_sma)
        df["fast_sma"] = sma(df.close, timeperiod=fast_sma)
        df[self.field_name] = crossover(df.fast_sma - df.slow_sma)
        return df


class IndicatorEmaCrossover(BaseIndicator):
    def apply(self, df: pd.DataFrame) -> None:
        self.ensure_required_filter_options([IndicatorParams.fast_ema, IndicatorParams.slow_ema])
        slow_ema = self.params[IndicatorParams.slow_ema]
        fast_ema = self.params[IndicatorParams.fast_ema]

        ema = abstract.Function("ema")
        df["slow_ema"] = ema(df.close, timeperiod=slow_ema)
        df["fast_ema"] = ema(df.close, timeperiod=fast_ema)
        df[self.field_name] = crossover(df.fast_ema - df.slow_ema)
        return df


class VWAPTrailing(BaseIndicator):
    def apply(self, df: pd.DataFrame) -> None:
        self.ensure_required_filter_options([IndicatorParams.trailing_bars])
        trailing_bars: int = self.params[IndicatorParams.trailing_bars]
        
        # definition for VWAP = https://www.investopedia.com/articles/trading/11/trading-with-vwap-mvwap.asp
        tpv = ((df.high + df.low + df.close) / 3.0) * df.volume
        cum_tpv = tpv.rolling(trailing_bars, min_periods=trailing_bars).sum()
        cum_vol = df.volume.rolling(trailing_bars, min_periods=trailing_bars).sum()
        df[self.field_name] = cum_tpv/cum_vol


class StdDeviationTrailing(BaseIndicator):
    def apply(self, df: pd.DataFrame) -> None:
        self.ensure_required_filter_options([IndicatorParams.trailing_bars])
        
        trailing_bars: int = self.params[IndicatorParams.trailing_bars]
        from_field_name: str = self.params[IndicatorParams.from_field_name]

        series = df[from_field_name]

        # todo - answer - what is the close price that's 2 std deviations away

        import ipdb; ipdb.set_trace()
