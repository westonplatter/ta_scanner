import pandas as pd
import numpy as np
import os
from loguru import logger
from ib_insync import IB, Forex, Future, ContFuture, Stock, Contract, util
from datetime import datetime, timedelta, timezone
import pytz

from ta_scanner.models import gen_engine

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


def load_data(instrument_symbol: str):
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, f"./{instrument_symbol}.csv")
    return pd.read_csv(filename)


def prepare_db():
    pass
    #   init_db()


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


def request_ib(ib, contract: Contract, d, duration, barSizeSetting, what_to_show):
    bars = ib.reqHistoricalData(
        contract,
        endDateTime=d,
        durationStr=duration,
        barSizeSetting=barSizeSetting,
        whatToShow=what_to_show,
        useRTH=False,
        formatDate=2,
    )
    df = util.df(bars)
    return df


def load_and_cache(instrument_symbol: str, **kwargs):
    ib = prepare_ib()
    engine = gen_engine()

    previous_days = int(kwargs["previous_days"])

    # import ipdb; ipdb.set_trace()
    tz = pytz.timezone("US/Eastern")
    now = datetime.now(tz)
    end_date = now - timedelta(days=previous_days)

    contract = Stock(instrument_symbol, "SMART", "USD")
    duration = f"1 D"
    bar_size_setting = "1 min"
    what_to_show = "TRADES"

    assert what_to_show in VALID_WHAT_TO_SHOW_TYPES

    # this is temp - start
    dfs = []
    # temp - stop

    for date in gen_last_x_days_from(previous_days, end_date):
        # if market was closed - skip
        # if db already has values - skip
        df = request_ib(ib, contract, date, duration, bar_size_setting, what_to_show)
        
        # cache df values
        df.to_sql('quote', con=engine, if_exists='replace', index=False)

        # temp - start
        dfs.append(df)
        # temp - stop

        logger.debug(f"finished {date}")

    return pd.concat(dfs)


def gen_last_x_days_from(x, date):
    result = []
    for days_ago in range(1, x):
        y = datetime.now(pytz.timezone("US/Eastern")) - timedelta(days=days_ago)
        result.append(y.date())
    return result
