from enum import Enum
import pandas as pd
import numpy as np
import os
from loguru import logger
from ib_insync import IB, Forex, Future, ContFuture, Stock, Contract, util
from datetime import datetime, timedelta, timezone
import pytz
from trading_calendars import get_calendar, TradingCalendar
from typing import Optional, Dict, Any

from ta_scanner.models import gen_engine, init_db, Quote


class TimezoneNames(Enum):
    US_EASTERN = "US/Eastern"
    US_CENTRAL = "US/Central"
    US_MOUNTAIN = "US/Mountain"
    US_PACIFIC = "US/Pacific"


class WhatToShow(Enum):
    TRADES = "TRADES"
    MIDPOINT = "MIDPOINT"
    BID = "BID"
    ASK = "ASK"
    BID_ASK = "BID_ASK"
    ADJUSTED_LAST = "ADJUSTED_LAST"
    HISTORICAL_VOLATILITY = "HISTORICAL_VOLATILITY"
    OPTION_IMPLIED_VOLATILITY = "OPTION_IMPLIED_VOLATILITY"
    REBATE_RATE = "REBATE_RATE"
    FEE_RATE = "FEE_RATE"
    YIELD_BID = "YIELD_BID"
    YIELD_ASK = "YIELD_ASK"
    YIELD_BID_ASK = "YIELD_BID_ASK"
    YIELD_LAST = "YIELD_LAST"


class Exchange(Enum):
    SMART = "SMART"
    NYSE = "NYSE"
    GLOBEX = "GLOBEX"
    NYMEX = "NYMEX"
    ECBOT = "ECBOT"
    CBOE = "CBOE"
    ICE = "ICE"



class Calendar(Enum):
    # https://github.com/quantopian/trading_calendars
    DEFAULT = "XNYS" # default to NYSE
    NYSE = "XNYS"
    CME = "CMES"
    CBOE = "XCBF"
    ICE = "IEPA"

    @staticmethod
    def futures_lookup_hash() -> Dict:
        return {
            Calendar.CME: [
                # equities
                '/ES', '/MES', '/MNQ', '/NQ', '/MNQ',
                # metals
                '/GC', '/MGC',
                # energy
                '/CL', '/QM',
                # currencies
                '/M6A', '/M6B', '/M6E',
                # interest rates 
                '/GE', '/ZN', '/ZN', '/ZT',
                # grains
                '/ZC', '/YC', '/ZS', '/YK', '/ZW', '/YW',
            ],
            Calendar.CBOE: [],
            Calendar.ICE: [],
        }

    @staticmethod
    def select_exchange_by_symbol(symbol: str):
        for k, v in Calendar.futures_lookup_hash().items():
            if symbol in v:
                return k
        logger.warning(f"Did not find a calendar entry for symbol={symbol}")
        return Calendar.DEFAULT

    @staticmethod
    def init_by_symbol(symbol: str) -> TradingCalendar:
        if "/" in symbol:
            key = Calendar.select_exchange_by_symbol(symbol)
            name = key.value
        else:
            name = Calendar.NYSE.value
        return get_calendar(name)


class Currency(Enum):
    USD = "USD"


from abc import ABCMeta, abstractmethod

# python3
class DataFetcherBase(object, metaclass=ABCMeta):
    pass


class IbDataFetcher(DataFetcherBase):
    def __init__(self):
        self.ib = None

    def _init_client(
        self, host: str = "127.0.0.1", port: int = 4001, client_id: int = 0
    ) -> None:
        ib = IB()
        ib.connect(host, port, clientId=client_id)
        self.ib = ib

    def _convert_bars_to_df(self, bars) -> pd.DataFrame:
        return util.df(bars)

    def _execute_req_historical(
        self, contract, dt, duration, bar_size_setting, what_to_show, use_rth
    ) -> pd.DataFrame:
        if self.ib is None or not self.ib.isConnected():
            self._init_client()

        bars = self.ib.reqHistoricalData(
            contract,
            endDateTime=dt,
            durationStr=duration,
            barSizeSetting=bar_size_setting,
            whatToShow=what_to_show,
            useRTH=use_rth,
            formatDate=2,  # return as UTC time
        )
        return self._convert_bars_to_df(bars)

    def request_stock_instrument(
        self, instrument_symbol: str, dt: datetime, what_to_show: str
    ) -> pd.DataFrame:
        exchange = Exchange.SMART.value
        contract = Stock(instrument_symbol, exchange, Currency.USD.value)
        duration = "1 D"
        bar_size_setting = "1 min"
        use_rth = False
        return self._execute_req_historical(
            contract, dt, duration, bar_size_setting, what_to_show, use_rth
        )

    def select_echange_by_symbol(self, symbol):
        d = {
            Exchange.GLOBEX: [
                # equities
                '/ES', '/MES', '/MNQ', '/NQ', '/MNQ'
                # # currencies
                # ? '/M6A', '/M6B', '/M6E',
                # # interest rates 
                # ? '/GE', '/ZN', '/ZN', '/ZT',
            ],
            Exchange.ECBOT: ['/ZC', '/YC', '/ZS', '/YK', '/ZW', '/YW'],
            Exchange.NYMEX: ['/GC', '/MGC', '/CL', '/QM',],
        }

        for k, v in d.items():
            if symbol in v:
                return k
        raise NotImplementedError

    def request_future_instrument(
        self,
        symbol: str,
        dt: datetime,
        what_to_show: str,
        contract_date: Optional[str] = None,
    ) -> pd.DataFrame:
        exchange_name = self.select_echange_by_symbol(symbol).value

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


def extract_kwarg(kwargs: Dict, key: str, default_value: Any = None) -> Optional[Any]:
    if key in kwargs:
        return kwargs[key]
    else:
        return default_value        


def load_and_cache(
    instrument_symbol: str, data_fetcher: DataFetcherBase, **kwargs
) -> pd.DataFrame:
    """Fetch data from IB or postgres

    Args:
        instrument_symbol (str): [description]
        data_fetcher (DataFetcherBase): [description]
        kwargs (Dict): [description]

    Returns:
        pd.DataFrame: [description]
    """
    engine = gen_engine()
    init_db()

    # turn kwargs into variables
    previous_days = int(kwargs["previous_days"])
    use_rth = extract_kwarg(kwargs, "use_rth", False)
    contract_date = extract_kwarg(kwargs, "contract_date")
    groupby_minutes = extract_kwarg(kwargs, "groupby_minutes", 1)
    return_tz = extract_kwarg(kwargs, "return_tz", TimezoneNames.US_EASTERN.value)

    tz = pytz.timezone(TimezoneNames.US_EASTERN.value)
    now = datetime.now(tz)
    end_date = now - timedelta(days=previous_days)

    what_to_show = WhatToShow.TRADES.value

    # this is temp - start
    dfs = []
    # temp - stop

    calendar = Calendar.init_by_symbol(instrument_symbol)

    for date in gen_last_x_days_from(previous_days, end_date):
        # if market was closed - skip
        if calendar.is_session(date.date()) == False:
            continue

        # if db already has values - skip
        if db_data_exists(engine, instrument_symbol, date):
            df = db_data_fetch(engine, instrument_symbol, date)
        else:
            df = data_fetcher.request_instrument(instrument_symbol, date, what_to_show)

            df["symbol"] = instrument_symbol
            transform_rename_df_columns(df)
            # convert time from UTC to US/Eastern
            df["ts"] = df["ts"].dt.tz_convert(TimezoneNames.US_EASTERN.value)
            apply_rth(df, calendar)

            try:
                # cache df values
                df.to_sql(
                    "quote", con=engine, if_exists="append", index=False, chunksize=1
                )
            except Exception as e:
                logger.error(e)

        if use_rth:
            df = reduce_to_only_rth(df)

        transform_set_index_ts(df)

        df = aggregate_bars(df, groupby_minutes)
        transform_ts_result_tz(df, return_tz)

        logger.debug(f"--- fetched {instrument_symbol} - {date.strftime('%Y-%m-%d')}")

        # temp - start
        dfs.append(df)
        # temp - stop

    logger.debug(f"finished {instrument_symbol}")

    df = pd.concat(dfs)
    df.sort_values(by=["ts"], inplace=True, ascending=True)
    df.reset_index(inplace=True)
    return df


def gen_last_x_days_from(x, date):
    result = []
    for days_ago in range(1, x):
        y = datetime.now() - timedelta(days=days_ago)
        et_datetime = datetime(
            y.year,
            y.month,
            y.day,
            23,
            59,
            59,
            0,
            pytz.timezone(TimezoneNames.US_EASTERN.value),
        )
        result.append(et_datetime)
    return result


def reduce_to_only_rth(df) -> pd.DataFrame:
    return df[df["rth"] == True]


def apply_rth(df: pd.DataFrame, calendar: TradingCalendar) -> None:
    calendar_name: str = "XNYS"
    calendar = get_calendar(calendar_name)

    def is_open(ts: pd.Timestamp):
        return calendar.is_open_on_minute(ts)

    df["rth"] = df.ts.apply(is_open)


def aggregate_bars(df: pd.DataFrame, groupby_minutes: int) -> pd.DataFrame:
    if groupby_minutes == 1:
        return df

    # this method only intended to handle data that's 
    # aggredating data at intervals less than 1 day
    assert groupby_minutes < 1440
    
    groupby = f"{groupby_minutes}min"

    agg_expression = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }
    df = df.resample(groupby).agg(agg_expression)
    df.dropna(subset=["open", "close", "high", "low"], inplace=True)
    return df


def transform_set_index_ts(df: pd.DataFrame) -> None:
    df.set_index('ts', inplace=True)


def transform_rename_df_columns(df) -> None:
    df.rename(columns={"date": "ts", "barCount": "bar_count"}, inplace=True)


def transform_ts_result_tz(df: pd.DataFrame, return_tz: str) -> None:
    return_tz_value = pytz.timezone(return_tz)
    df.index = df.index.tz_convert(return_tz_value)


def clean_query(query: str) -> str:
    return query.replace("\n", "").replace("\t", "")


def db_data_exists(engine, instrument_symbol: str, date: datetime) -> bool:
    date_str: str = date.strftime("%Y-%m-%d")

    query = f"""
        select count(*) 
        from {Quote.__tablename__}
        where
            symbol = '{instrument_symbol}'
            and date(ts AT TIME ZONE '{TimezoneNames.US_EASTERN.value}') = date('{date_str}') 
    """
    with engine.connect() as con:
        result = con.execute(clean_query(query))
        counts = [x for x in result]

    return counts != [(0,)]


def db_data_fetch(engine, instrument_symbol: str, date: datetime) -> pd.DataFrame:
    date_str: str = date.strftime("%Y-%m-%d")

    query = f"""
        select *
        from {Quote.__tablename__}
        where 
            symbol = '{instrument_symbol}'
            and date(ts AT TIME ZONE '{TimezoneNames.US_EASTERN.value}') = date('{date_str}')
    """
    return pd.read_sql(clean_query(query), con=engine)
