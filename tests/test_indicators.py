import pandas as pd
import pytest
from typing import Any, Dict

from ta_scanner.indicators import IndicatorSmaCrossover


def gen_df_zeros(field_name="some_field_name"):
    return pd.DataFrame(0, index=[1, 2, 3], columns=[field_name])


def test_init():
    x = IndicatorSmaCrossover()
    assert x is not None
