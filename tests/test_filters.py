import pandas as pd
import pytest
from typing import Any, Dict

from ta_scanner.filters import FilterCumsum, FilterOptions, FilterException


def gen_df_zeros(field_name="some_field_name"):
    return pd.DataFrame(0, index=[1, 2, 3], columns=[field_name])


def test_required_filter_options():
    field_name = "indicator_name"
    df = gen_df_zeros(field_name)
    filter_cumsum = FilterCumsum()

    filter_options: Dict[FilterOptions, Any] = {
        FilterOptions.win_points: 20.0,
        # FilterOptions.loss_points: 10.0,
        # FilterOptions.threshold_intervals: 50,
    }

    with pytest.raises(FilterException):
        filter_cumsum.apply(df, field_name, filter_options)

    filter_options: Dict[FilterOptions, Any] = {
        FilterOptions.win_points: 20.0,
        FilterOptions.loss_points: 10.0,
        FilterOptions.threshold_intervals: 50,
    }

    filter_cumsum.apply(df, field_name, filter_options)
