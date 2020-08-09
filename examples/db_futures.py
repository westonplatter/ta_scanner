from loguru import logger
from ta_scanner.data import load_and_cache, IbDataFetcher
import datetime

ib_data_fetcher = IbDataFetcher()

future_symbols = ["/MES", "/MNQ", "/ZS", "/GC"]

for symbol in future_symbols:
    df = load_and_cache(
        symbol,
        ib_data_fetcher,
        start_date=datetime.date(2020, 7, 10),
        end_date=datetime.date(2020, 7, 20),
        use_rth=False,
        groupby_minutes=1,
    )
    logger.info(f"{symbol} - {len(df.index)}")
    logger.info(f"{symbol} - first data row = {df.head(1)}")

for symbol in future_symbols:
    df = load_and_cache(
        symbol,
        ib_data_fetcher,
        start_date=datetime.date(2020, 7, 10),
        end_date=datetime.date(2020, 7, 20),
        use_rth=False,
        groupby_minutes=1,
        return_tz="US/Mountain",
    )
    logger.info(f"{symbol} - {len(df.index)}")
    logger.info(f"{symbol} - first data row = {df.head(1)}")
