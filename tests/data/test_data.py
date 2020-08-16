import pandas as pd
from ta_scanner.data.data import (
    __gen_values,
    __gen_cols,
    db_insert_df_conflict_on_do_nothing,
)


def fake_df_ab():
    data = {"a": [1, 2, 3], "b": [11, 22, 33]}
    df = pd.DataFrame(data, columns=["a", "b"])
    return df


def test_gen_values():
    df = fake_df_ab()
    expected_values = [("1", "11"), ("2", "22"), ("3", "33")]
    assert __gen_values(df) == expected_values


def test_gen_cols():
    df = fake_df_ab()
    expected_values = ["a", "b"]
    assert __gen_cols(df) == expected_values
