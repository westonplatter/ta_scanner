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
    def __init__(self):
        pass

    def ensure_required_filter_options(
        self, expected: List[FilterOptions], actual: Dict[FilterOptions, Any]
    ):
        for fo_key in expected:
            if fo_key not in actual:
                raise FilterException(f"expected this key key = {fo_key}")


class FilterCumsum(BaseFitler):
    name = FilterNames.filter_cumsum.value

    required_filter_options = [
        FilterOptions.win_points,
        FilterOptions.loss_points,
        FilterOptions.threshold_intervals,
    ]

    def apply(
        self,
        df: pd.DataFrame,
        field_name: str,
        filter_options: Dict[FilterOptions, Any],
    ):
        self.ensure_required_filter_options(
            self.required_filter_options, filter_options
        )

        query_signals = f"{field_name} != 0"
        threshold = filter_options[FilterOptions.threshold_intervals]

        for index, rs in df.query(query_signals).iterrows():
            signal_direction = df.loc[index, field_name]
            logger.debug(f"{signal_direction} @ {rs.close}")

            for index_after in range(0, threshold):
                df_index = index + index_after
                rx = df.iloc[df_index]
                diff = (rx.close - rs.close) * signal_direction

                if diff >= filter_options[FilterOptions.win_points]:
                    df.loc[df_index, self.name] = diff
                    logger.debug(f"Won @ {rx.close}. Diff = {diff}")
                    break
                if diff <= (filter_options[FilterOptions.loss_points] * -1.0):
                    df.loc[df_index, self.name] = diff
                    logger.debug(f"Loss @ {rx.close}. Diff = {diff}")
                    break

                if index_after == threshold - 1:
                    logger.debug(f"Max time. Diff = {diff}")
                    df.loc[df_index, self.name] = diff
