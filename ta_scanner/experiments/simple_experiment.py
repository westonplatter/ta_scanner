import datetime
import pandas as pd

from ta_scanner.reports import BasicReport


class SimpleExperiment:
    def __init__(
        self, df, field_name, train_sd, train_ed, test_sd, test_ed, indicator, sfilter
    ):
        self.df = df
        self.train_sd = train_sd
        self.train_ed = train_ed
        self.test_sd = test_sd
        self.test_ed = test_ed
        self.indicator = indicator
        self.sfilter = sfilter

        self.field_name = field_name
        self.x = None
        self.y = None

    def apply(self):
        basic_report = BasicReport()

        # train results
        train_df = self.gen_train_df()
        self.indicator.apply(train_df)
        results = self.sfilter.apply(train_df)
        train_report_results = basic_report.analyze(train_df, self.field_name)
        self.x = train_report_results

        # test results
        test_df = self.gen_test_df()
        self.indicator.apply(test_df)
        self.sfilter.apply(test_df)
        test_report_results = basic_report.analyze(test_df, self.field_name)
        self.y = test_report_results

    def results(self):
        return (self.x, self.y)

    def gen_test_df(self) -> pd.DataFrame:
        __df = self.df.query("@self.test_sd <= ts and ts <= @self.test_ed").copy()
        if len(__df.index) == 0:
            raise Exception("no rows in test_df")
        return __df

    def gen_train_df(self) -> pd.DataFrame:
        __df = self.df.query("@self.train_sd <= ts and ts <= @self.train_ed").copy()
        if len(__df.index) == 0:
            raise Exception("no rows in train_df")
        return __df


# indicator_sma_cross = IndicatorSmaCrossover()

# # store signals in this field
# field_name = "moving_avg_cross"

# # Moving Average Crossover, 20 vs 50
# indicator_params = {
#     IndicatorParams.fast_sma: 30,
#     IndicatorParams.slow_sma: 60,
# }
# # apply indicator to generate signals
# indicator_sma_cross.apply(df, field_name, indicator_params)

# # initialize filter
# sfilter = FilterCumsum()

# filter_options = {
#     FilterParams.win_points: 10,
#     FilterParams.loss_points: 3,
#     FilterParams.threshold_intervals: 20,
# }

# # generate results
# results = sfilter.apply(df, field_name, filter_options)

# # analyze results
# basic_report = BasicReport()
# pnl, count, average, median = basic_report.analyze(df, FilterNames.filter_cumsum.value)

# logger.info("------------------------")

# logger.info(f"Final PnL = {pnl}")
