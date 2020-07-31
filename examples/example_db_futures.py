from loguru import logger
from ta_scanner.data import load_and_cache, IbDataFetcher

ib_data_fetcher = IbDataFetcher()

df = load_and_cache("/MES", ib_data_fetcher, previous_days=20, use_rth=False, groupby_minutes=15)

# df = load_and_cache("/MNQ", ib_data_fetcher, previous_days=20, use_rth=True)
# df = load_and_cache("/ZS", ib_data_fetcher, previous_days=30, use_rth=True)
# df = load_and_cache("/MGC", ib_data_fetcher, previous_days=30, use_rth=True)