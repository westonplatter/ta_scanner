import pandas as pd
from loguru import logger

import datetime
from trading_calendars import get_calendar, TradingCalendar
from typing import Optional, Dict, Any, List, Tuple, Optional

from ib_insync import IB, Future, ContFuture, Stock, Contract
from ib_insync import util as ib_insync_util

from ta_scanner.data.base_connector import DataFetcherBase
from ta_scanner.data.constants import (
    TimezoneNames,
    WhatToShow,
    Exchange,
    Calendar,
    Currency,
)


class IbDataFetcher(DataFetcherBase):
    def __init__(self, client_id: int = 0):
        self.ib = None
        self.client_id = client_id

    def _init_client(self, host: str = "127.0.0.1", port: int = 4001) -> None:
        ib = IB()
        ib.connect(host, port, clientId=self.client_id)
        self.ib = ib

    def _execute_req_historical(
        self, contract, dt, duration, bar_size_setting, what_to_show, use_rth
    ) -> pd.DataFrame:
        if self.ib is None or not self.ib.isConnected():
            self._init_client()

        dfs = []
        for rth in [True, False]:
            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime=dt,
                durationStr=duration,
                barSizeSetting=bar_size_setting,
                whatToShow=what_to_show,
                useRTH=rth,  # use_rth,
                formatDate=2,  # return as UTC time
            )
            x = ib_insync_util.df(bars)
            if x is None:
                continue
            x["rth"] = rth
            dfs.append(x)

        if dfs == []:
            return None
        df = pd.concat(dfs).drop_duplicates().reset_index(drop=True)
        return df

    def request_stock_instrument(
        self, instrument_symbol: str, dt: datetime.datetime, what_to_show: str
    ) -> pd.DataFrame:
        exchange = Exchange.SMART.value
        contract = Stock(instrument_symbol, exchange, Currency.USD.value)
        duration = "2 D"
        bar_size_setting = "1 min"
        use_rth = False
        return self._execute_req_historical(
            contract, dt, duration, bar_size_setting, what_to_show, use_rth
        )

    def select_exchange_by_symbol(self, symbol):
        kvs = {
            Exchange.GLOBEX: [
                # fmt: off
                # equities
                "/ES", "/MES",
                "/NQ", "/MNQ",
                # currencies
                "/M6A", "/M6B", "/M6E",
                # interest rates
                # '/GE', '/ZN', '/ZN', '/ZT',
                # fmt: on
            ],
            Exchange.ECBOT: ["/ZC", "/YC", "/ZS", "/YK", "/ZW", "/YW"],
            Exchange.NYMEX: ["/GC", "/MGC", "/CL", "/QM",],
        }

        for k, v in kvs.items():
            if symbol in v:
                return k
        raise NotImplementedError

    def request_future_instrument(
        self,
        symbol: str,
        dt: datetime.datetime,
        what_to_show: str,
        contract_date: Optional[str] = None,
    ) -> pd.DataFrame:
        exchange_name = self.select_exchange_by_symbol(symbol).value

        if contract_date:
            raise NotImplementedError
        else:
            contract = ContFuture(symbol, exchange_name, currency=Currency.USD.value)

        duration = "1 D"
        bar_size_setting = "1 min"
        use_rth = False
        return self._execute_req_historical(
            contract, dt, duration, bar_size_setting, what_to_show, use_rth
        )

    def request_instrument(
        self, symbol: str, dt, what_to_show, contract_date: Optional[str] = None,
    ):
        if "/" in symbol:
            return self.request_future_instrument(
                symbol, dt, what_to_show, contract_date
            )
        else:
            return self.request_stock_instrument(symbol, dt, what_to_show)
