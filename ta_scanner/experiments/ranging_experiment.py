import csv
import numpy as np
import pandas as pd
import scipy
from tqdm import tqdm


from ta_scanner.filters import FilterCumsum, FilterParams
from ta_scanner.indicators import IndicatorSmaCrossover, IndicatorParams
from ta_scanner.reports import BasicReport

from ta_scanner.experiments.base_experiment import BaseExperiment


class RangingExperiment(BaseExperiment):
    def __init__(
        self,
        df,
        field_name,
        train_sd,
        train_ed,
        test_sd,
        test_ed,
        indicator,
        sfilter,
        min_slow_sma,
        max_slow_sma,
    ):
        self.df = df
        self.field_name = field_name
        self.train_sd = train_sd
        self.train_ed = train_ed
        self.test_sd = test_sd
        self.test_ed = test_ed
        self.indicator = indicator
        self.sfilter = sfilter

        self.min_slow_sma = min_slow_sma
        self.max_slow_sma = max_slow_sma
        self.results = None

    def gen_test_df(self):
        return self.gen_subset_df(self.test_sd, self.test_ed, "test_df")

    def gen_train_df(self):
        return self.gen_subset_df(self.train_sd, self.train_ed, "train_df")

    def apply(self):
        train_results = self.__apply_range(self.gen_train_df())
        test_results = self.__apply_range(self.gen_test_df())
        self.results = (train_results, test_results)
        return self.results

    def analyze_results(self):
        test_results, train_results = self.results
        test_returns, train_returns = [], []

        for i in range(0, len(test_results)):
            test_returns.append(test_results[i][1][0])
            train_returns.append(train_results[i][1][0])

        from scipy import stats

        corr, pvalue = stats.spearmanr(test_returns, train_returns)
        return {"spearmanr": {"corr": corr, "pvalue": pvalue}}

    def to_csv(self):
        test_results, train_results = self.results

        rows = []

        for i in range(0, len(test_results)):
            row = []
            # slow_sma
            row.append([test_results[i][0]])
            # test_returns
            row.append(test_results[i][1][0])
            # train_returns
            row.append(train_results[i][1][0])
            rows.append(row)

        headers = ["fast_sma", "slow_sma", "test_returns", "train_returns"]

        fn = "results/weston_range_returns.csv"

        with open(fn, "w") as f:
            w = csv.writer(f)
            w.writerow(headers)
            for x in rows:
                w.writerow(x)

    def __apply_range(self, original_df):
        basic_report = BasicReport()
        results = []

        for slow_sma in tqdm(range(self.min_slow_sma, self.max_slow_sma)):
            __df = original_df.copy()

            __indicator = IndicatorSmaCrossover(
                field_name=self.field_name,
                params={
                    IndicatorParams.slow_sma: slow_sma,
                    IndicatorParams.fast_sma: 60,
                },
            )
            __indicator.apply(__df)

            __filter = FilterCumsum(
                field_name=self.field_name,
                params={
                    FilterParams.win_points: 10,
                    FilterParams.loss_points: 5,
                    FilterParams.threshold_intervals: 40,
                },
            )
            __filter.apply(__df)
            report_results = basic_report.analyze(__df, self.field_name)
            results.append([slow_sma, report_results])

        return results
