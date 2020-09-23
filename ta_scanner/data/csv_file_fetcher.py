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
        self.df = pd.read_csv(self.file_path)

    def request_instrument(self):
        return self.df
