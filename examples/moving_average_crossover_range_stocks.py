from datetime import date
from loguru import logger
import sys

from ta_scanner.data.data import load_and_cache
from ta_scanner.data.ib import IbDataFetcher
from ta_scanner.indicators import IndicatorSmaCrossover, IndicatorParams
from ta_scanner.signals import Signal
from ta_scanner.filters import FilterCumsum, FilterOptions, FilterNames
from ta_scanner.reports import BasicReport


# mute the noisy data debug statements
logger.remove()
logger.add(sys.stderr, level="INFO")

# get SPY data
ib_data_fetcher = IbDataFetcher()
df_original = load_and_cache(
    "SPY",
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
        FilterOptions.loss_points: 5,
        FilterOptions.threshold_intervals: 30,
    }

    # generate results
    results = sfilter.apply(df, field_name, filter_options)

    # get aggregate pnl
    basic_report = BasicReport()
    pnl = basic_report.analyze(df, FilterNames.filter_cumsum.value)
    return pnl


slow_sma = 50

for fast_sma in range(2, slow_sma):
    final_pnl = run_cross(fast_sma, slow_sma)
    print(f"MA Crx {fast_sma}/{slow_sma}. Final PnL = {final_pnl}")
