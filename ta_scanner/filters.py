import abc
from enum import Enum
import pandas as pd
from loguru import logger
from typing import Any, Dict, List, Optional, List


class FilterOptions(Enum):
    win_points = "win_points"
    loss_points = "loss_points"
    threshold_intervals = "threshold_intervals"


class FilterNames(Enum):
    filter_cumsum = "filter_cumsum"


class FilterException(Exception):
    pass


class BaseFitler(metaclass=abc.ABCMeta):
    def __init__(self, field_name: str, params: Dict[FilterOptions, Any]):
        self.field_name = field_name
        self.params = params

    def ensure_required_filter_options(
        self, expected: List[FilterOptions], actual: Dict[FilterOptions, Any]
    ):
        for fo_key in expected:
            if fo_key not in actual:
                raise FilterException(f"expected key = {fo_key}")

    @abc.abstractmethod
    def apply(self, df, field_name, filter_options):
        pass


class FilterCumsum(BaseFitler):
    name = FilterNames.filter_cumsum.value

    required_filter_options = [
        FilterOptions.win_points,
        FilterOptions.loss_points,
        FilterOptions.threshold_intervals,
    ]

    def log_exit(self, action: str, diff, row):
        logger.debug(f"Action={action}. Ts={row.ts}. Diff={diff}. Close={row.close}")

    def log_entry(self, action, row):
        logger.debug(f"Action={action}. Ts={row.ts}. Close={row.close}")

    def apply(self, df: pd.DataFrame, inverse: int = 1):
        self.ensure_required_filter_options(self.required_filter_options, self.params)

        query_signals = f"{self.field_name} != 0"

        threshold = self.params[FilterOptions.threshold_intervals]

        for index, rs in df.query(query_signals).iterrows():
            signal_direction = rs[self.field_name] * inverse
            self.log_entry(signal_direction, rs)

            for index_after in range(0, threshold):
                df_index = index + index_after

                if df_index >= len(df.index):
                    rx = df.iloc[df_index - 1]
                    diff = (rx.close - rs.close) * signal_direction
                    self.log_exit("MaxTime", diff, rx)
                    df.at[df_index, self.name] = diff
                    break

                rx = df.iloc[df_index]
                diff = (rx.close - rs.close) * signal_direction

                if diff >= self.params[FilterOptions.win_points]:
                    self.log_exit("Won", diff, df.loc[df_index])
                    df.at[df_index, self.name] = diff
                    break

                if diff <= (self.params[FilterOptions.loss_points] * -1.0):
                    df.at[df_index, self.name] = diff
                    self.log_exit("Lost", diff, df.loc[df_index])
                    break

                if index_after == threshold - 1:
                    self.log_exit("MaxTime", diff, df.loc[df_index])
                    df.at[df_index, self.name] = diff
