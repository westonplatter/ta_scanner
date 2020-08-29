import datetime
import pandas as pd


class BaseExperiment:
    def gen_subset_df(
        self, sd: datetime.datetime, ed: datetime.datetime, name: str
    ) -> pd.DataFrame:
        subset_df = self.df.query("@sd <= ts and ts <= @ed").copy()
        if len(subset_df.index) == 0:
            raise Exception(f"{name} has 0 rows")
        return subset_df
