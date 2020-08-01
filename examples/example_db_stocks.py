from loguru import logger
from ta_scanner.data import load_and_cache, IbDataFetcher

ib_data_fetcher = IbDataFetcher()

symbols = ["SPY", "QQQ", "AAPL"]

for symbol in symbols:
    df = load_and_cache(
        symbol, ib_data_fetcher, previous_days=20, use_rth=False, groupby_minutes=15
    )
    logger.info(f"{symbol} - {len(df.index)}")
