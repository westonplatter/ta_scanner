from enum import Enum
import pandas as pd
import numpy as np
import os
from loguru import logger
from ib_insync import IB, Forex, Future, ContFuture, Stock, Contract, util
from datetime import datetime, timedelta, timezone
import pytz
from trading_calendars import get_calendar, TradingCalendar

from ta_scanner.models import gen_engine, init_db, Quote

VALID_WHAT_TO_SHOW_TYPES = [
    "TRADES",
    "MIDPOINT",
    "BID",
    "ASK",
    "BID_ASK",
    "ADJUSTED_LAST",
    "HISTORICAL_VOLATILITY",
    "OPTION_IMPLIED_VOLATILITY",
    "REBATE_RATE",
    "FEE_RATE",
    "YIELD_BID",
    "YIELD_ASK",
    "YIELD_BID_ASK",
    "YIELD_LAST",
]


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
    NYSE = "NYSE"
    SMART = "NYSE"


class ExchangeCalendar(Enum):
    NYSE = "XNYS"
    SMART = "SMART"


def prepare_db():
    init_db()


def prepare_ib():
    ib = IB()
    ib.connect("127.0.0.1", 4001, clientId=1)
    return ib


from abc import ABCMeta, abstractmethod

# python3
class DataFetcherBase(object, metaclass=ABCMeta):
    pass


class IbDataFetcher(DataFetcherBase):
    def __init__(self):
        self.ib = None

    def init_client(
        self, host: str = "127.0.0.1", port: int = 4001, client_id: int = 0
    ) -> None:
        ib = IB()
        ib.connect(host, port, clientId=client_id)
        self.ib = ib

    def request_stock_instrument(
        self, instrument_symbol: str, dt: datetime, what_to_show: str
    ) -> pd.DataFrame:
        if self.ib is None or not self.ib.isConnected():
            self.init_client()

        exchange = Exchange.SMART
        contract = Stock(instrument_symbol, exchange.value, "USD")
        duration = "1 D"
        bar_size_setting = "1 min"
        use_rth = False

        bars = self.ib.reqHistoricalData(
            contract,
            endDateTime=dt,
            durationStr=duration,
            barSizeSetting=bar_size_setting,
            whatToShow=what_to_show,
            useRTH=use_rth,
            formatDate=2,  # return as UTC time
        )

        df = util.df(bars)
        return df


def load_and_cache(
    instrument_symbol: str, data_fetcher: DataFetcherBase, **kwargs
) -> pd.DataFrame:
    """Fetch data from IB or postgres

    Args:
        instrument_symbol (str): [description]

    Returns:
        pd.DataFrame: [description]
    """
    engine = gen_engine()
    prepare_db()

    previous_days = int(kwargs["previous_days"])
    use_rth = kwargs["use_rth"] if "use_rth" in kwargs else False

    tz = pytz.timezone(TimezoneNames.US_EASTERN.value)
    now = datetime.now(tz)
    end_date = now - timedelta(days=previous_days)

    what_to_show = WhatToShow.TRADES.value

    # this is temp - start
    dfs = []
    # temp - stop

    exchange = Exchange.SMART
    exchange_calendar = ExchangeCalendar[exchange.name]
    calendar = get_calendar(exchange_calendar.value)

    # ib_data_fetcher = IbDataFetcher()

    for date in gen_last_x_days_from(previous_days, end_date):
        # if market was closed - skip
        if calendar.is_session(date.date()) == False:
            continue

        # if db already has values - skip
        if db_data_exists(engine, instrument_symbol, date):
            df = db_data_fetch(engine, instrument_symbol, date)
        else:
            df = data_fetcher.request_stock_instrument(
                instrument_symbol, date, what_to_show
            )

            df["symbol"] = instrument_symbol
            rename_df_columns(df)
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

        logger.debug(f"--- fetched {instrument_symbol} - {date.strftime('%Y-%m-%d')}")

        # temp - start
        dfs.append(df)
        # temp - stop

    logger.debug(f"finished {instrument_symbol}")

    df = pd.concat(dfs)
    df.drop(["id"], axis=1, inplace=True)
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


def rename_df_columns(df) -> None:
    df.rename(columns={"date": "ts", "barCount": "bar_count"}, inplace=True)


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
