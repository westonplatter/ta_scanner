import pandas as pd
import numpy as np
import os


def load_data(instrument_symbol: str):
    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, f'./{instrument_symbol}.csv')
    return pd.read_csv(filename)