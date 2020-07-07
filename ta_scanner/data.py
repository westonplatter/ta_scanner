import pandas as pd
import numpy as np
import os
from loguru import logger
from ib_insync import IB, Forex, Future, ContFuture, Stock, Contract, util


def load_data(instrument_symbol: str):
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, f"./{instrument_symbol}.csv")
    return pd.read_csv(filename)


def load_data_ib(instrument_symbol: str):
    ib = IB()
    ib.connect("127.0.0.1", 4001, clientId=1)

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
