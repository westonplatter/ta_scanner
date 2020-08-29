# todos
# - [ ] all dates and date deltas are in time, not integers

from loguru import logger
from typing import Dict
import sys
import datetime
from datetime import timedelta, timezone
import pytz
import numpy as np

from ta_scanner.data.data import load_and_cache, db_data_fetch_between, aggregate_bars
from ta_scanner.data.ib import IbDataFetcher
from ta_scanner.experiments.ranging_experiment import RangingExperiment

from ta_scanner.indicators import (
    IndicatorSmaCrossover,
    IndicatorEmaCrossover,
    IndicatorParams,
)

from ta_scanner.filters import (
    FilterCumsum,
    FilterParams,
    FilterNames,
)

from ta_scanner.signals import Signal
from ta_scanner.reports import BasicReport
from ta_scanner.models import gen_engine


engine = gen_engine()
instrument_symbol = "/MGC"
field_name = "my_field_name"
groupby_minutes = 1


def query_data(engine, symbol, sd, ed, groupby_minutes):
    df = db_data_fetch_between(engine, symbol, sd, ed)
    df.set_index("ts", inplace=True)
    df = aggregate_bars(df, groupby_minutes=groupby_minutes)
    df["ts"] = df.index
    return df


train_sd, train_ed = (
    datetime.datetime(2020, 8, 1, 0, 0, 0, tzinfo=timezone.utc),
    datetime.datetime(2020, 8, 18, 0, 0, 0, tzinfo=timezone.utc),
)
test_sd, test_ed = (
    datetime.datetime(2020, 8, 20, 0, 0, 0, tzinfo=timezone.utc),
    datetime.datetime(2020, 8, 24, 0, 0, 0, tzinfo=timezone.utc),
)

indicator = IndicatorSmaCrossover(
    field_name=field_name,
    params={IndicatorParams.slow_sma: 30, IndicatorParams.fast_sma: 60},
)

sfilter = FilterCumsum(
    field_name=field_name,
    params={
        FilterParams.win_points: 10,
        FilterParams.loss_points: 5,
        FilterParams.threshold_intervals: 30,
    },
)

df = query_data(engine, instrument_symbol, train_sd, test_ed, groupby_minutes)

range_e = RangingExperiment(
    df, field_name, train_sd, train_ed, test_sd, test_ed, indicator, sfilter, 2, 60
)

logger.remove()
logger.add(sys.stderr, level="INFO")

range_e.apply()

# x = range_e.analyze_results()

range_e.to_csv()

import ipdb

ipdb.set_trace()


# for i in range(0, len(test_results)):
#     test_return = test_results[i][1][0]
#     train_return = train_results[i][1][0]
#     print(f"i={i}. {test_return / train_return}")
