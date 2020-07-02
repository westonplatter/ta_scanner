from tas.data import load_data
from tas.signals import Signal
from tas.filters import Filter
from tas.reports import BasicReport


# initialize signal.
# Moving Average Crossover, 20 vs 50
indicator = Indicator("macrx", {"interval_unit": "day", "slow_ma": 50, "fast_ma": 20})

# get SPY data
df = load_data("SPY")

# apply indicator to generate signals
df.apply_indicator(signal, field_name="macrx")

# initialize filter
sfilter = Filter("cumsum", threshold_interval=3, threshold_unit="day")

# generate results
results = sfilter.apply(df, field_name="macrx")

# analyze results
report = BasicReport()
pnl = report.analyze(results)

print(pnl)
