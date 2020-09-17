import pandas as pd
import pytest
from typing import Any, Dict

from ta_scanner.filters import FilterCumsum, FilterOptions, FilterException


def gen_df_zeros(field_name="some_field_name"):
    return pd.DataFrame(0, index=[1, 2, 3], columns=[field_name])


def test_abstract_methods_present():
    field_name, result_field_name, params = "some_field_name", "result", []
    FilterCumsum(
        field_name=field_name, result_field_name=result_field_name, params=params
    )


def test_required_filter_options():
    field_name = "indicator_name"
    result_field_name = f"{field_name}_png"
    df = gen_df_zeros(field_name)

    params: Dict[FilterOptions, Any] = {
        FilterOptions.win_points: 20.0,
        FilterOptions.loss_points: 10.0,
        # FilterOptions.threshold_intervals: 50,
    }

    filter_cumsum = FilterCumsum(
        field_name=field_name, result_field_name=result_field_name, params=params
    )

    with pytest.raises(FilterException) as execinfo:
        filter_cumsum.apply(df)
        assert "FilterOptions.loss_points" in str(excinfo.value)

    params: Dict[FilterOptions, Any] = {
        FilterOptions.win_points: 20.0,
        FilterOptions.loss_points: 10.0,
        FilterOptions.threshold_intervals: 50,
    }

    filter_cumsum = FilterCumsum(
        field_name=field_name, result_field_name=result_field_name, params=params
    )

    filter_cumsum.apply(df)
