from datetime import date
from loguru import logger
import sys

from ta_scanner.data import load_and_cache, IbDataFetcher
from ta_scanner.indicators import IndicatorSmaCrossover, IndicatorParams
from ta_scanner.signals import Signal
from ta_scanner.filters import FilterCumsum, FilterOptions, FilterNames
from ta_scanner.reports import BasicReport


# mute the noisy data debug statements
logger.remove()
logger.add(sys.stderr, level="INFO")

ib_data_fetcher = IbDataFetcher()

symbol = "/MGC"
df_original = load_and_cache(
    symbol,
    ib_data_fetcher,
    start_date=date(2020, 7, 1),
    end_date=date(2020, 7, 20),
    use_rth=True,
)

indicator_sma_cross = IndicatorSmaCrossover()

# store signals in this field
field_name = "moving_avg_cross"


def run_cross(fast_sma: int, slow_sma: int):
    df = df_original.copy()

    indicator_params = {
        IndicatorParams.fast_sma: fast_sma,
        IndicatorParams.slow_sma: slow_sma,
    }
    # apply indicator to generate signals
    indicator_sma_cross.apply(df, field_name, indicator_params)

    # initialize filter
    sfilter = FilterCumsum()

    filter_options = {
        FilterOptions.win_points: 10,
        FilterOptions.loss_points: 3,
        FilterOptions.threshold_intervals: 40,
    }

    # generate results
    results = sfilter.apply(df, field_name, filter_options)

    # get aggregate pnl
    basic_report = BasicReport()
    pnl, count, avg, median = basic_report.analyze(df, FilterNames.filter_cumsum.value)
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
