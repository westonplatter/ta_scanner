from loguru import logger
from ta_scanner.data import load_data
from ta_scanner.indicators import IndicatorSma
from ta_scanner.signals import Signal
from ta_scanner.filters import FilterCumsum, FilterOptions, FilterNames
from ta_scanner.reports import BasicReport


# initialize signal.
# Moving Average Crossover, 20 vs 50
indicator_sma = IndicatorSma(
    "macrx", {"interval_unit": "day", "slow_sma": 50, "fast_sma": 20}
)

# get SPY data
df = load_data("SPY")

# apply indicator to generate signals
indicator_sma.apply(df, field_name="macrx")

# initialize filter
sfilter = FilterCumsum()

filter_options = {
    FilterOptions.win_points: 40.0,
    FilterOptions.loss_points: 20.0,
    FilterOptions.threshold_intervals: 20
}

# generate results
results = sfilter.apply(df, "macrx", filter_options)

# analyze results
basic_report = BasicReport()
pnl = basic_report.analyze(df, FilterNames.filter_cumsum.value)

logger.info("------------------------")

logger.info(f"Final PnL = {pnl}")
