# todos
# - all dates and date deltas are in time, not integers

from loguru import logger
import datetime
from typing import Dict
import sys

import numpy as np

from ta_scanner.data.data import load_and_cache, db_data_fetch_between
from ta_scanner.data.ib import IbDataFetcher
from ta_scanner.experiments.simple_experiment import SimpleExperiment

from ta_scanner.indicators import IndicatorSmaCrossover, IndicatorParams
from ta_scanner.signals import Signal
from ta_scanner.filters import FilterCumsum, FilterOptions, FilterNames
from ta_scanner.reports import BasicReport
from ta_scanner.models import gen_engine


ib_data_fetcher = IbDataFetcher()
instrument_symbol = "/MES"
rth = False
interval = 3

field_name = "moving_avg_cross"

engine = gen_engine()

logger.remove()
logger.add(sys.stderr, level="INFO")


def gen_params(sd, ed) -> Dict:
    return dict(start_date=sd, end_date=ed, use_rth=rth, groupby_minutes=interval)


def run_cross(original_df, fast_sma: int, slow_sma: int):
    df = original_df.copy()
    
    # indicator setup
    indicator_params = {
        IndicatorParams.fast_sma: fast_sma,
        IndicatorParams.slow_sma: slow_sma,
    }
    indicator = IndicatorSmaCrossover(field_name, indicator_params)
    indicator.apply(df)

    # filter setup
    filter_params = {
        FilterOptions.win_points: 6,
        FilterOptions.loss_points: 3,
        FilterOptions.threshold_intervals: 15,
    }
    sfilter = FilterCumsum(field_name, filter_params)

    # generate results
    results = sfilter.apply(df)

    # get aggregate pnl
    basic_report = BasicReport()
    pnl, count, avg, median = basic_report.analyze(df, FilterNames.filter_cumsum.value)
    return pnl, count, avg, median


def run_cross_range(df, slow_sma: int, fast_sma_min, fast_sma_max):
    results = []
    for fast_sma in range(fast_sma_min, fast_sma_max):
        pnl, count, avg, median = run_cross(df, fast_sma, slow_sma)
        results.append([fast_sma, pnl, count, avg, median])
    return results


def fetch_data():
    sd = datetime.date(2020, 7, 1)
    ed = datetime.date(2020, 8, 15)
    load_and_cache(
        instrument_symbol, ib_data_fetcher, **gen_params(sd, ed)
    )


def query_data(engine, symbol, sd, ed):
    return db_data_fetch_between(engine, symbol, sd, ed)


# fetch_data()

for i in range(10, 30):
    test_start, test_end = i, i
    train_start = i - 3
    train_stop = i - 1

    # fetch training data
    train_sd = datetime.date(2020, 7, train_start)
    train_ed = datetime.date(2020, 7, train_stop)
    df_train = query_data(engine, instrument_symbol, train_sd, train_ed)

    slow_sma = 20

    # for training data, let's find results for a range of SMA
    results = run_cross_range(df_train, slow_sma=slow_sma, fast_sma_min=5, fast_sma_max=15)

    fast_sma_pnl = []

    for resultindex in range(2, len(results)-3):
        fast_sma = results[resultindex][0]
        pnl = results[resultindex][1]
        result_set = results[resultindex-2:resultindex+3]
        total_pnl = sum([x[1] for x in result_set])
        fast_sma_pnl.append([fast_sma, total_pnl, pnl])
    
    arr = np.array(fast_sma_pnl, dtype=float)
    max_tuple = np.unravel_index(np.argmax(arr, axis=None), arr.shape)
    optimal_fast_sma = int(arr[(max_tuple[0], 0)])

    optimal_fast_sma_pnl = [x[2] for x in fast_sma_pnl if x[0] == optimal_fast_sma][0]

    logger.info(f"Selected fast_sma={optimal_fast_sma}. PnL={optimal_fast_sma_pnl}")

    test_sd = datetime.date(2020, 7, test_start)
    test_ed = datetime.date(2020, 7, test_end)
    
    if test_ed.weekday() in [5, 6]:
        continue

    df_test = query_data(engine, instrument_symbol, test_sd, test_ed)
    test_results = run_cross(df_test, optimal_fast_sma, slow_sma)
    logger.info(f"Test Results. pnl={test_results[0]}, count={test_results[1]}, avg={test_results[2]}, median={test_results[3]}")