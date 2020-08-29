from datetime import datetime, date
from loguru import logger

from ta_scanner.data.data import load_and_cache
from ta_scanner.data.ib import IbDataFetcher
from ta_scanner.indicators import IndicatorSmaCrossover, IndicatorParams
from ta_scanner.signals import Signal
from ta_scanner.filters import FilterCumsum, FilterParams, FilterNames
from ta_scanner.reports import BasicReport


ib_data_fetcher = IbDataFetcher()
df = load_and_cache(
    "/MES",
    ib_data_fetcher,
    start_date=date(2020, 7, 10),
    end_date=date(2020, 7, 20),
    use_rth=True,
)

# store signals in this field
field_name = "moving_avg_cross"

# Moving Average Crossover, 20 vs 50
indicator_params = {
    IndicatorParams.fast_sma: 30,
    IndicatorParams.slow_sma: 60,
}

# init
indicator_sma_cross = IndicatorSmaCrossover(
    field_name=field_name, params=indicator_params
)

# apply indicator to generate signals
indicator_sma_cross.apply(df)


filter_options = {
    FilterParams.win_points: 10,
    FilterParams.loss_points: 3,
    FilterParams.threshold_intervals: 20,
}
# initialize filter
sfilter = FilterCumsum(field_name=field_name, params=filter_options)

# generate results
results = sfilter.apply(df)

# analyze results
basic_report = BasicReport()
pnl, count, average, median = basic_report.analyze(df, FilterNames.filter_cumsum.value)

logger.info("------------------------")

logger.info(f"Final PnL = {pnl}")
