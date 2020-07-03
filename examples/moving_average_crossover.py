from ta_scanner.data import load_data
from ta_scanner.indicators import IndicatorSma
from ta_scanner.signals import Signal
from ta_scanner.filters import FilterCumsum
from ta_scanner.reports import BasicReport


# initialize signal.
# Moving Average Crossover, 20 vs 50
indicator_sma = IndicatorSma("macrx", {"interval_unit": "day", "slow_sma": 50, "fast_sma": 20})

# get SPY data
df = load_data("SPY")

# apply indicator to generate signals
indicator_sma.apply(df, field_name="macrx")

# initialize filter
sfilter = FilterCumsum(threshold_interval=20)

# generate results
results = sfilter.apply(df, field_name="macrx", win_points=40, loss_points=40)

# analyze results
basic_report = BasicReport()
pnl = basic_report.analyze(df, "filter_cumsum")

print("------------------------")

print(f"Final PnL = {pnl}")
