from loguru import logger
from ta_scanner.data import load_data, load_data_ib, prepare_db, load_and_cache

# from ta_scanner.indicators import IndicatorSmaCrossover, IndicatorParams
# from ta_scanner.signals import Signal
# from ta_scanner.filters import FilterCumsum, FilterOptions, FilterNames
# from ta_scanner.reports import BasicReport


prepare_db()

df = load_and_cache("SPY", previous_days=10)

import ipdb; ipdb.set_trace()
