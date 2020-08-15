from enum import Enum
import pandas as pd
import numpy as np
import os
from loguru import logger
from ib_insync import IB, Forex, Future, ContFuture, Stock, Contract
from ib_insync import util as ib_insync_util
from psycopg2 import sql

# from datetime import datetime, timedelta, timezone, date
import datetime
import pytz
from trading_calendars import get_calendar, TradingCalendar
from typing import Optional, Dict, Any, List, Tuple

from ta_scanner.models import gen_engine, init_db, Quote


class TimezoneNames(Enum):
    US_EASTERN = "US/Eastern"
    US_CENTRAL = "US/Central"
    US_MOUNTAIN = "US/Mountain"
    US_PACIFIC = "US/Pacific"
    UTC = "UTC"


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
    DEFAULT = "XNYS"  # default to NYSE
    NYSE = "XNYS"
    CME = "CMES"
    CBOE = "XCBF"
    ICE = "IEPA"

    @staticmethod
    def futures_lookup_hash() -> Dict:
        return {
            Calendar.CME: [
                # equities
                "/ES",
                "/MES",
                "/MNQ",
                "/NQ",
                "/MNQ",
                # metals
                "/GC",
                "/MGC",
                # energy
                "/CL",
                "/QM",
                # currencies
                "/M6A",
                "/M6B",
                "/M6E",
                # interest rates
                "/GE",
                "/ZN",
                "/ZN",
                "/ZT",
                # grains
                "/ZC",
                "/YC",
                "/ZS",
                "/YK",
                "/ZW",
                "/YW",
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
    def __init__(self, client_id: int = 0):
        self.ib = None
        self.client_id = client_id

    def _init_client(
        self, host: str = "127.0.0.1", port: int = 4001
    ) -> None:
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
            x['rth'] = rth
            dfs.append(x)
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

    def select_echange_by_symbol(self, symbol):
        d = {
            Exchange.GLOBEX: [
                # equities
                "/ES",
                "/MES",
                "/MNQ",
                "/NQ",
                "/MNQ"
                # currencies
                "/M6A",
                "/M6B",
                "/M6E",
                # # interest rates
                # ? '/GE', '/ZN', '/ZN', '/ZT',
            ],
            Exchange.ECBOT: ["/ZC", "/YC", "/ZS", "/YK", "/ZW", "/YW"],
            Exchange.NYMEX: ["/GC", "/MGC", "/CL", "/QM",],
        }

        for k, v in d.items():
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
    start_date = extract_kwarg(kwargs, "start_date", None)
    end_date = extract_kwarg(kwargs, "end_date", None)

    use_rth = extract_kwarg(kwargs, "use_rth", False)
    contract_date = extract_kwarg(kwargs, "contract_date")
    groupby_minutes = extract_kwarg(kwargs, "groupby_minutes", 1)
    return_tz = extract_kwarg(kwargs, "return_tz", TimezoneNames.US_EASTERN.value)

    what_to_show = WhatToShow.TRADES.value

    # this is temp - start
    dfs = []
    # temp - stop

    calendar = Calendar.init_by_symbol(instrument_symbol)

    for dt in gen_datetime_range(start_date, end_date):
        # if market was closed - skip
        # if calendar.is_session(dt.date()) == False:
        #     logger.debug(f"Market closed on {dt.date()} for {instrument_symbol}")

        # if db already has values - skip
        # if db_data_exists(engine, instrument_symbol, dt):
        #     df = db_data_fetch(engine, instrument_symbol, dt)
        # else:
        if True:
            df = data_fetcher.request_instrument(instrument_symbol, dt, what_to_show)

            df["symbol"] = instrument_symbol
            transform_rename_df_columns(df)
            # convert time from UTC to US/Eastern
            # df["ts"] = df["ts"].dt.tz_convert(TimezoneNames.UTC.value)
            # df["ts"] = df["ts"].dt.tz_localize(TimezoneNames.US_EASTERN.value)
            # apply_rth(df, calendar)
            db_insert_df_conflict_on_do_nothing(engine, df, "quote")

        if use_rth:
            df = reduce_to_only_rth(df)

        transform_set_index_ts(df)

        df = aggregate_bars(df, groupby_minutes)
        transform_ts_result_tz(df, return_tz)

        logger.debug(f"--- fetched {instrument_symbol} - {dt}")

        # temp - start
        dfs.append(df)
        # temp - stop

    logger.debug(f"finished {instrument_symbol}")

    df = pd.concat(dfs)
    df.sort_values(by=["ts"], inplace=True, ascending=True)
    df.reset_index(inplace=True)
    return df


def gen_datetime_range(start, end) -> List[datetime.datetime]:
    result = []
    span = end - start
    for i in range(span.days + 1):
        d = start + datetime.timedelta(days=i)
        result.append(datetime.date(d.year, d.month, d.day))
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
    df.set_index("ts", inplace=True)


def transform_rename_df_columns(df) -> None:
    df.rename(columns={"date": "ts", "barCount": "bar_count"}, inplace=True)


def transform_ts_result_tz(df: pd.DataFrame, return_tz: str) -> None:
    return_tz_value = pytz.timezone(return_tz)
    df.index = df.index.tz_convert(return_tz_value)


def clean_query(query: str) -> str:
    return query.replace("\n", "").replace("\t", "")


def db_data_exists(engine, instrument_symbol: str, date: datetime.datetime) -> bool:
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


def db_data_fetch(
    engine, instrument_symbol: str, date: datetime.datetime
) -> pd.DataFrame:
    date_str: str = date.strftime("%Y-%m-%d")

    query = f"""
        select *
        from {Quote.__tablename__}
        where
            symbol = '{instrument_symbol}'
            and date(ts AT TIME ZONE '{TimezoneNames.US_EASTERN.value}') = date('{date_str}')
    """
    return pd.read_sql(clean_query(query), con=engine)


def db_data_fetch_between(
    engine, instrument_symbol: str, sd: datetime.datetime, ed: datetime.datetime
) -> pd.DataFrame:
    sd_str: str = sd.strftime("%Y-%m-%d")
    ed_str: str = ed.strftime("%Y-%m-%d")

    query = f"""
        select *
        from {Quote.__tablename__}
        where
            symbol = '{instrument_symbol}'
            and date(ts AT TIME ZONE '{TimezoneNames.US_EASTERN.value}') BETWEEN date('{sd}') AND date('{ed}')
    """
    return pd.read_sql(clean_query(query), con=engine)


def db_insert_df_conflict_on_do_nothing(
    engine, df: pd.DataFrame, table_name: str
) -> None:
    cols = __gen_cols(df)
    values = __gen_values(df)

    query_template = """
        INSERT INTO {table_name} ({cols})
        VALUES ({values});
    """

    query = sql.SQL(query_template).format(
        table_name=sql.Identifier(table_name),
        cols=sql.SQL(", ").join(map(sql.Identifier, cols)),
        values=sql.SQL(", ").join(sql.Placeholder() * len(cols)),
    )

    with engine.connect() as con:
        with con.connection.cursor() as cur:
            for v in values:
                try:
                    cur.execute(query, v)
                except Exception as e:
                    pass
        con.connection.commit()


def __gen_values(df: pd.DataFrame) -> List[Tuple[str]]:
    """
    return array of tuples for the df values
    """
    return [tuple([str(xx) for xx in x]) for x in df.to_records(index=False)]


def __gen_cols(df) -> List[str]:
    """
    return column names
    """
    return list(df.columns)
