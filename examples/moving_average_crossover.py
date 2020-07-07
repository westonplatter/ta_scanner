from loguru import logger
from ta_scanner.data import load_data, load_data_ib
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
df = load_data_ib("SPY")

# apply indicator to generate signals
indicator_sma.apply(df, field_name="macrx")

# initialize filter
sfilter = FilterCumsum()

filter_options = {
    FilterOptions.win_points: 40.0,
    FilterOptions.loss_points: 20.0,
    FilterOptions.threshold_intervals: 20,
}

# generate results
results = sfilter.apply(df, "macrx", filter_options)

# analyze results
basic_report = BasicReport()
pnl = basic_report.analyze(df, FilterNames.filter_cumsum.value)

logger.info("------------------------")

logger.info(f"Final PnL = {pnl}")


# 2020-07-05 16:29:09.759 | DEBUG    | ta_scanner.filters:apply:58 - -1 @ 319.01
# 2020-07-05 16:29:09.765 | DEBUG    | ta_scanner.filters:apply:75 - Max time. Diff = 14.699999999999989
# 2020-07-05 16:29:09.767 | DEBUG    | ta_scanner.filters:apply:58 - 1 @ 313.48
# 2020-07-05 16:29:09.772 | DEBUG    | ta_scanner.filters:apply:75 - Max time. Diff = -1.920000000000016
# 2020-07-05 16:29:09.772 | DEBUG    | ta_scanner.filters:apply:58 - -1 @ 309.22
# 2020-07-05 16:29:09.777 | DEBUG    | ta_scanner.filters:apply:75 - Max time. Diff = -0.46999999999997044
# 2020-07-05 16:29:09.778 | DEBUG    | ta_scanner.filters:apply:58 - 1 @ 313.67
# 2020-07-05 16:29:09.781 | DEBUG    | ta_scanner.filters:apply:75 - Max time. Diff = -9.550000000000011
# 2020-07-05 16:29:09.782 | DEBUG    | ta_scanner.filters:apply:58 - -1 @ 303.86
# 2020-07-05 16:29:09.786 | DEBUG    | ta_scanner.filters:apply:75 - Max time. Diff = 1.0
# 2020-07-05 16:29:09.787 | DEBUG    | ta_scanner.filters:apply:58 - 1 @ 307.21
# 2020-07-05 16:29:09.791 | DEBUG    | ta_scanner.filters:apply:75 - Max time. Diff = 4.2900000000000205
# 2020-07-05 16:29:09.791 | INFO     | __main__:<module>:37 - ------------------------
# 2020-07-05 16:29:09.791 | INFO     | __main__:<module>:39 - Final PnL = 8.050000000000011