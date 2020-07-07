from loguru import logger
from ta_scanner.data import load_data, load_data_ib
from ta_scanner.indicators import IndicatorSmaCrossover, IndicatorParams
from ta_scanner.signals import Signal
from ta_scanner.filters import FilterCumsum, FilterOptions, FilterNames
from ta_scanner.reports import BasicReport


# get SPY data
df = load_data_ib("SPY")

indicator_sma_cross = IndicatorSmaCrossover()

# store signals in this field
field_name = "moving_avg_cross"

# Moving Average Crossover, 20 vs 50
indicator_params = {
    IndicatorParams.fast_ema: 20,
    IndicatorParams.slow_ema: 50,
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

# analyze results
basic_report = BasicReport()
pnl = basic_report.analyze(df, FilterNames.filter_cumsum.value)

logger.info("------------------------")

logger.info(f"Final PnL = {pnl}")
