from loguru import logger
from ta_scanner.data import load_and_cache, IbDataFetcher

ib_data_fetcher = IbDataFetcher()

df = load_and_cache("QQQ", ib_data_fetcher, previous_days=10, use_rth=True)
df = load_and_cache("SPY", ib_data_fetcher, previous_days=10, use_rth=True)
