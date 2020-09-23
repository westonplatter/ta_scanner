from loguru import logger
from ta_scanner.data.data import load_and_cache
from ta_scanner.data.ib import IbDataFetcher
import datetime

ib_data_fetcher = IbDataFetcher()

# symbols = ["/MES", "/MNQ", "/MGC"]
symbols = ["/MES"]

get_last_n_days = 5
sd = datetime.date.today() - datetime.timedelta(days=get_last_n_days)
ed = datetime.date.today() - datetime.timedelta(days=1)

for symbol in symbols:
    params = dict(start_date=sd, end_date=ed, use_rth=False, groupby_minutes=1,)
    df = load_and_cache(symbol, ib_data_fetcher, **params)

logger.info("Done")