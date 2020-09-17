from loguru import logger
from ta_scanner.data.data import load_and_cache
from ta_scanner.data.ib import IbDataFetcher
import datetime

ib_data_fetcher = IbDataFetcher()

symbol = "/MES"
sd = datetime.date(2020, 9, 2)
ed = datetime.date(2020, 9, 10)
params = dict(start_date=sd, end_date=ed, use_rth=False, groupby_minutes=1,)

df = load_and_cache(symbol, ib_data_fetcher, **params)
logger.info(f"{symbol} - All hours / 1min bars - {len(df.index)}")

params["use_rth"] = True
df = load_and_cache(symbol, ib_data_fetcher, **params)
logger.info(f"{symbol} - Only RTH / 1min bars - {len(df.index)}")

params["use_rth"] = False
params["groupby_minutes"] = 12
df = load_and_cache(symbol, ib_data_fetcher, **params)
logger.info(f"{symbol} - All hours / 12min bars - {len(df.index)}")
logger.info(f"\n{df.head(10)}")
