import datetime
from loguru import logger
import sys

from ta_scanner.data.data import db_data_fetch_between, aggregate_bars
from ta_scanner.models import gen_engine


symbol = "/MES"
sd = datetime.date(2020, 9, 18)
ed = sd

engine = gen_engine()
groupby_minutes = 1


def query_data(engine, symbol, sd, ed, groupby_minutes):
    df = db_data_fetch_between(engine, symbol, sd, ed)
    df.set_index("ts", inplace=True)
    df = aggregate_bars(df, groupby_minutes=groupby_minutes)
    df["ts"] = df.index
    return df


df = query_data(engine, symbol, sd, ed, groupby_minutes)

import ipdb; ipdb.set_trace()
