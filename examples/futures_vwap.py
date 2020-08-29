import datetime
from loguru import logger
import sys

from ta_scanner.data.data import load_and_cache, db_data_fetch_between, aggregate_bars
from ta_scanner.data.ib import IbDataFetcher
from ta_scanner.indicators import IndicatorParams, VWAPTrailing, StdDeviationTrailing
from ta_scanner.signals import Signal
from ta_scanner.filters import FilterCumsum, FilterParams, FilterNames
from ta_scanner.reports import BasicReport
from ta_scanner.models import gen_engine


# mute the noisy data debug statements
logger.remove()
logger.add(sys.stderr, level="INFO")

ib_data_fetcher = IbDataFetcher()

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

trailing_bars = 30

# store signals in this field
vwap_field_name = f"vwap_trailing_{trailing_bars}"
indicator_vwap_trailing = VWAPTrailing(field_name=vwap_field_name, params={
    IndicatorParams.trailing_bars: trailing_bars
})
indicator_vwap_trailing.apply(df)

indicator_std_dev_trailing = StdDeviationTrailing(field_name="field_name", params={
    IndicatorParams.trailing_bars: trailing_bars,
    IndicatorParams.from_field_name: vwap_field_name,
})
indicator_std_dev_trailing.apply(df)

df.to_csv("vwap_std_dev.csv")