from datetime import datetime, date
from loguru import logger

from ta_scanner.data import load_and_cache, IbDataFetcher
from ta_scanner.indicators import IndicatorSmaCrossover, IndicatorParams
from ta_scanner.signals import Signal
from ta_scanner.filters import FilterCumsum, FilterOptions, FilterNames
from ta_scanner.reports import BasicReport


ib_data_fetcher = IbDataFetcher()
df = load_and_cache(
    "/MES",
    ib_data_fetcher,
    start_date=date(2020, 7, 10),
    end_date=date(2020, 7, 20),
    use_rth=True,
)

indicator_sma_cross = IndicatorSmaCrossover()

# store signals in this field
field_name = "moving_avg_cross"

# Moving Average Crossover, 20 vs 50
indicator_params = {
    IndicatorParams.fast_sma: 30,
    IndicatorParams.slow_sma: 60,
}
# apply indicator to generate signals
indicator_sma_cross.apply(df, field_name, indicator_params)

# initialize filter
sfilter = FilterCumsum()

filter_options = {
    FilterOptions.win_points: 10,
    FilterOptions.loss_points: 3,
    FilterOptions.threshold_intervals: 20,
}

# generate results
results = sfilter.apply(df, field_name, filter_options)

# analyze results
basic_report = BasicReport()
pnl, count, average, median = basic_report.analyze(df, FilterNames.filter_cumsum.value)

logger.info("------------------------")

logger.info(f"Final PnL = {pnl}")
