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
df = load_and_cache("/MNQ", ib_data_fetcher, previous_days=30, use_rth=True)

indicator_sma_cross = IndicatorSmaCrossover()

# store signals in this field
field_name = "moving_avg_cross"


def run_cross(fast_sma: int, slow_sma: int):
    indicator_params = {
        IndicatorParams.fast_sma: fast_sma,
        IndicatorParams.slow_sma: slow_sma,
    }
    # apply indicator to generate signals
    indicator_sma_cross.apply(df, field_name, indicator_params)

    # initialize filter
    sfilter = FilterCumsum()

    filter_options = {
        FilterOptions.win_points: 4,
        FilterOptions.loss_points: 2,
        FilterOptions.threshold_intervals: 20,
    }

    # generate results
    results = sfilter.apply(df, field_name, filter_options)

    # get aggregate pnl
    basic_report = BasicReport()
    pnl, count, avg, median = basic_report.analyze(df, FilterNames.filter_cumsum.value)
    return pnl, count, avg, median


slow_sma = 100

for fast_sma in range(2, slow_sma):
    pnl, count, avg, median = run_cross(fast_sma, slow_sma)
    print(f"MA_Crx_{fast_sma}/{slow_sma}, {pnl}, {count}, {avg}, {median}")
