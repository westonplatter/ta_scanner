import pandas as pd
from loguru import logger

import datetime
from trading_calendars import get_calendar, TradingCalendar
from typing import Optional, Dict, Any, List, Tuple, Optional

from ta_scanner.data.base_connector import DataFetcherBase
from ta_scanner.data.constants import (
    TimezoneNames,
    WhatToShow,
    Exchange,
    Calendar,
    Currency,
)


class CsvFileFetcher(DataFetcherBase):
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.df = None
        self._load_data_from_file()

    def _load_data_from_file(self):
        df = pd.read_csv(self.file_path)
        self.df = self._prepare_columns(df)

    def _prepare_columns(self, ddf):
        rename_columns = {
            "Date": "date",
            " Time": "time",
            " Open": "open",
            " High": "high",
            " Low": "low",
            " Last": "close",
            " Volume": "volume",
            " Bid Volume": "bid_volume",
            " Ask Volume": "ask_volume",
            " Close": "cumulative_delta_bars",
        }
        ddf = ddf.rename(columns=rename_columns)
        ddf["ts"] = pd.to_datetime(ddf["date"].map(str) + ddf["time"].map(str))
        ddf.set_index("ts", drop=False, inplace=True)
        ddf = ddf.tz_localize("US/Mountain")
        return ddf

    def request_instrument(self):
        return self.df
