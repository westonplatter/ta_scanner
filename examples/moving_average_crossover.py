from tas.data import load_data
from tas.signals import Signal
from tas.filters import Filter
from tas.reports import BasicReport


signal = Signal("macrx", {"interval_unit": "day", "slow_ma": 50, "fast_ma": 20})

df = load_data("SPY")

df.decorate_with_signal(signal, "field_name_macrx")

sfilter = Filter("cumsum", threshold_interval=3, threshold_unit="day")

results = cumsum(df, sfilter)

report = BasicReport()
pnl = report.analyze(results)

print(pnl)
