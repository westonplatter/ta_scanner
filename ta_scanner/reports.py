import numpy as np
from typing import Tuple


class BasicReport:
    def __init__(self):
        pass

    def analyze(self, df, field_name) -> Tuple[np.float64, int, np.float64, np.float64]:
        trades = df.query(f"0 < {field_name} or {field_name} < 0")

        trades.to_csv("trades.csv")

        pnl = trades[field_name].sum()
        count = trades[field_name].count()
        average = np.average(trades[field_name])
        median = np.median(trades[field_name])

        return pnl, count, average, median
