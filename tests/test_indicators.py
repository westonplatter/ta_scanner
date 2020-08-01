import pandas as pd
import pytest
from typing import Any, Dict

from ta_scanner.indicators import (
    IndicatorSmaCrossover,
    IndicatorParams,
    IndicatorException,
)


def gen_df_zeros(field_name="some_field_name"):
    return pd.DataFrame(0, index=[1, 2, 3], columns=[field_name])


def test_abstract_methods_present():
    IndicatorSmaCrossover()


def test_ensure_required_filter_options():
    sma_crossover = IndicatorSmaCrossover()

    fake_df = gen_df_zeros()
    fake_field_name = "fake_some_name"

    params = {
        IndicatorParams.fast_sma: 20,
        # IndicatorParams.slow_sma: 50, # intentionally missing param
    }

    with pytest.raises(IndicatorException) as e:
        sma_crossover.apply(fake_df, fake_field_name, params=params)

    expected_message = "IndicatorSmaCrossover requires key = IndicatorParams.slow_sma"
    assert expected_message == str(e.value)
