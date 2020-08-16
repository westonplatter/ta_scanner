from datetime import date
from loguru import logger
from ta_scanner.data.data import load_and_cache
from ta_scanner.data.ib import IbDataFetcher

ib_data_fetcher = IbDataFetcher()

symbols = ["SPY", "QQQ", "AAPL"]

for symbol in symbols:
    df = load_and_cache(
        symbol,
        ib_data_fetcher,
        start_date=date(2020, 6, 1),
        end_date=date(2020, 6, 4),
        use_rth=False,
        groupby_minutes=15,
    )
    logger.info(f"{symbol} - {len(df.index)}")
