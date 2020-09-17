import datetime
from loguru import logger
import sys

from ta_scanner.data.data import load_and_cache, db_data_fetch_between, aggregate_bars
from ta_scanner.data.ib import IbDataFetcher
from ta_scanner.indicators import (
    IndicatorSmaCrossover,
    IndicatorParams,
    CombinedBindary,
)
from ta_scanner.signals import Signal
from ta_scanner.filters import FilterCumsum, FilterOptions, FilterNames
from ta_scanner.reports import BasicReport
from ta_scanner.models import gen_engine

# mute the noisy data debug statements
# logger.remove()
# logger.add(sys.stderr, level="INFO")

symbol = "/MES"


def query_data(engine, symbol, sd, ed, groupby_minutes):
    df = db_data_fetch_between(engine, symbol, sd, ed)
    df.set_index("ts", inplace=True)
    df = aggregate_bars(df, groupby_minutes=groupby_minutes)
    df["ts"] = df.index
    return df


engine = gen_engine()
sd, ed = datetime.date(2020, 8, 1), datetime.date(2020, 8, 23)
interval = 1
df = query_data(engine, symbol, sd, ed, interval)


# short duration
short_duration_ma_cross = "short_duration_ma_cross"
short_duration_fast_sma = 30
short_duration_slow_sma = 60

# long duration
multiplier = 3
long_duration_ma_cross = "long_duration_ma_cross"
long_duration_fast_sma = short_duration_fast_sma * multiplier
long_duration_slow_sma = short_duration_slow_sma * multiplier

# init and apply short duration crosses
short_duration_indicator = IndicatorSmaCrossover(
    field_name=short_duration_ma_cross,
    params={
        IndicatorParams.fast_sma: short_duration_fast_sma,
        IndicatorParams.slow_sma: short_duration_slow_sma,
    },
)
short_duration_indicator.apply(df)

# init and apply long duration crosses
long_duration_indicator = IndicatorSmaCrossover(
    field_name=long_duration_ma_cross,
    params={
        IndicatorParams.fast_sma: long_duration_fast_sma,
        IndicatorParams.slow_sma: long_duration_slow_sma,
    },
)
long_duration_indicator.apply(df)

# combine indicators
composite_field_name = "composite"
composite_indicator = CombinedBindary(
    field_name=composite_field_name,
    params={
        IndicatorParams.field_names: [short_duration_ma_cross, long_duration_ma_cross]
    },
)
composite_indicator.apply(df)

filter_options = {
    FilterOptions.win_points: 20,
    FilterOptions.loss_points: 6,
    FilterOptions.threshold_intervals: 20,
}
# initialize filter

result_field_name = f"{composite_field_name}_pnl"
sfilter = FilterCumsum(
    field_name=composite_field_name,
    result_field_name=result_field_name,
    params=filter_options,
)

# generate results
results = sfilter.apply(df, -1)

# analyze results
basic_report = BasicReport()
pnl, count, average, median = basic_report.analyze(df, result_field_name)

logger.info(f"Final PnL = {pnl}")
