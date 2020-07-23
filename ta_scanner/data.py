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


def load_data(instrument_symbol: str):
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, f"./{instrument_symbol}.csv")
    return pd.read_csv(filename)


def prepare_db():
    init_db()


def prepare_ib():
    ib = IB()
    ib.connect("127.0.0.1", 4001, clientId=1)
    return ib


def load_data_ib(instrument_symbol: str):
    ib = prepare_ib()

    contract = Stock(instrument_symbol, "SMART", "USD")

    dt = ""
    barsList = []
    maxTimes = 1
    times = 0

    while True:
        bars = ib.reqHistoricalData(
            contract,
            endDateTime=dt,
            durationStr="10 D",
            barSizeSetting="30 mins",
            whatToShow="TRADES",
            useRTH=True,
            formatDate=1,
        )

        if not bars or times > maxTimes:
            break
        barsList.append(bars)
        dt = bars[0].date
        logger.debug(dt)
        times += 1

    allBars = [b for bars in reversed(barsList) for b in bars]
    df = util.df(allBars)
    return df


def request_ib(
    ib,
    contract: Contract,
    dt: datetime,
    duration: str,
    bar_size_setting: str,
    what_to_show: str,
    use_rth: bool = False,
) -> pd.DataFrame:
    """Execute reqHistoricalData.

    Args:
        ib ([type]): [description]
        contract (Contract): [description]
        dt (datetime): [description]
        duration (str): [description]
        bar_size_setting (str): [description]
        what_to_show (str): [description]
        use_rth (bool, optional): [description]. Defaults to False.

    Returns:
        pd.DataFrame: [description]
    """
    bars = ib.reqHistoricalData(
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


def load_and_cache(instrument_symbol: str, **kwargs) -> pd.DataFrame:
    """Fetch data from IB or postgres

    Args:
        instrument_symbol (str): [description]

    Returns:
        pd.DataFrame: [description]
    """
    ib = prepare_ib()
    engine = gen_engine()

    previous_days = int(kwargs["previous_days"])
    use_rth = kwargs["use_rth"] if "use_rth" in kwargs else False

    tz = pytz.timezone(TimezoneNames.US_EASTERN.value)
    now = datetime.now(tz)
    end_date = now - timedelta(days=previous_days)

    exchange = Exchange.SMART
    contract = Stock(instrument_symbol, exchange.value, "USD")

    duration = "1 D"
    bar_size_setting = "1 min"
    what_to_show = WhatToShow.TRADES.value

    # this is temp - start
    dfs = []
    # temp - stop

    exchange_calendar = ExchangeCalendar[exchange.name]
    calendar = get_calendar(exchange_calendar.value)

    for date in gen_last_x_days_from(previous_days, end_date):
        # if market was closed - skip
        if calendar.is_session(date.date()) == False:
            continue

        # if db already has values - skip
        if db_data_exists(engine, instrument_symbol, date):
            df = db_data_fetch(engine, instrument_symbol, date)
            logger.debug(f"cached values for {instrument_symbol}. {date}")
        else:
            df = request_ib(
                ib, contract, date, duration, bar_size_setting, what_to_show
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
        
        # temp - start
        dfs.append(df)
        # temp - stop

        logger.debug(f"finished {date}")

    return pd.concat(dfs)


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
    return df[df['rth'] == True]


def apply_rth(df: pd.DataFrame, calendar: TradingCalendar) -> None:
    calendar_name: str = "XNYS"
    calendar = get_calendar(calendar_name)
    def is_open(ts: pd.Timestamp):
        return calendar.is_open_on_minute(ts)
    df['rth'] = df.ts.apply(is_open)


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
