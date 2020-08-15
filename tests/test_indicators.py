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
    field_name, params = "field_name", []
    IndicatorSmaCrossover(field_name=field_name, params=params)


def test_ensure_required_filter_options():
    field_name = "fake_some_name"
    fake_df = gen_df_zeros(field_name)

    params = {
        IndicatorParams.fast_sma: 20,
        # IndicatorParams.slow_sma: 50, # intentionally missing param
    }

    sma_crossover = IndicatorSmaCrossover(field_name=field_name, params=params)

    with pytest.raises(IndicatorException) as e:
        sma_crossover.apply(fake_df)

    expected_message = "IndicatorSmaCrossover requires key = IndicatorParams.slow_sma"
    assert expected_message == str(e.value)
