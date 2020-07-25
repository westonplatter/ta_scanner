from loguru import logger
from ta_scanner.data import load_data, prepare_db, load_and_cache, IbDataFetcher

# from ta_scanner.indicators import IndicatorSmaCrossover, IndicatorParams
# from ta_scanner.signals import Signal
# from ta_scanner.filters import FilterCumsum, FilterOptions, FilterNames
# from ta_scanner.reports import BasicReport


prepare_db()

ib_data_fetcher = IbDataFetcher()

df = load_and_cache("QQQ", ib_data_fetcher, previous_days=10, use_rth=True)
df = load_and_cache("SPY", ib_data_fetcher, previous_days=10, use_rth=True)
