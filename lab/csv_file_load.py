from ta_scanner.data.data import aggregate_bars
from ta_scanner.data.csv_file_fetcher import CsvFileFetcher

data = CsvFileFetcher("example.csv")
df = data.request_instrument()
