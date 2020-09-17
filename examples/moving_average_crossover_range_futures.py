import datetime
from loguru import logger
import sys

from ta_scanner.data.data import load_and_cache, db_data_fetch_between, aggregate_bars
from ta_scanner.data.ib import IbDataFetcher
from ta_scanner.indicators import IndicatorSmaCrossover, IndicatorParams
from ta_scanner.signals import Signal
from ta_scanner.filters import FilterCumsum, FilterOptions, FilterNames
from ta_scanner.reports import BasicReport
from ta_scanner.models import gen_engine


# mute the noisy data debug statements
logger.remove()
logger.add(sys.stderr, level="INFO")

ib_data_fetcher = IbDataFetcher()

symbol = "/MGC"

df_original = load_and_cache(
    symbol,
    ib_data_fetcher,
    start_date=datetime.date(2020, 8, 1),
    end_date=datetime.date(2020, 8, 23),
)


def query_data(engine, symbol, sd, ed, groupby_minutes):
    df = db_data_fetch_between(engine, symbol, sd, ed)
    df.set_index("ts", inplace=True)
    df = aggregate_bars(df, groupby_minutes=groupby_minutes)
    df["ts"] = df.index
    return df


engine = gen_engine()
sd, ed = datetime.date(2020, 8, 1), datetime.date(2020, 8, 23)
interval = 1
df_original = query_data(engine, symbol, sd, ed, interval)


# store signals in this field
field_name = "moving_avg_cross"
result_field_name = f"{field_name}_pnl"


def run_cross(fast_sma: int, slow_sma: int):
    df = df_original.copy()

    indicator_params = {
        IndicatorParams.fast_sma: fast_sma,
        IndicatorParams.slow_sma: slow_sma,
    }
    indicator = IndicatorSmaCrossover(field_name, indicator_params)
    indicator.apply(df)

    filter_options = {
        FilterOptions.win_points: 6,
        FilterOptions.loss_points: 4,
        FilterOptions.threshold_intervals: 30,
    }
    sfilter = FilterCumsum(field_name, result_field_name, filter_options)
    results = sfilter.apply(df)

    # get aggregate pnl
    basic_report = BasicReport()
    pnl, count, avg, median = basic_report.analyze(df, field_name)
    return pnl, count, avg, median


slow_sma = 60

results = []

for fast_sma in range(2, slow_sma):
    pnl, count, avg, median = run_cross(fast_sma, slow_sma)
    results.append([slow_sma, fast_sma, pnl, count, avg, median])
    print(f"MA_Crx_{fast_sma}/{slow_sma}, {pnl}, {count}, {avg}, {median}")


# write results to csv

headers = ["slow_sma", "fast_sma", "pnl", "count", "avg", "median"]

filename = f"results/MA_Crx_{symbol.replace('/', '')}.csv"

with open(filename, "w") as f:
    import csv

    writer = csv.writer(f)
    writer.writerow(headers)
    for row in results:
        writer.writerow(row)
