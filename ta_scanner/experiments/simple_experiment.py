from abc import ABC


class BaseExperiment(ABC):
    @staticmethod
    def x() -> str:
        return "x"


class SimpleExperiment(BaseExperiment):
    def __init__(
        self, df_train, df_test, indicator, indicator_params, sfilter, sfilter_params
    ):
        self.df_train = df_train
        self.df_test = df_test
        self.indicator = indicator
        self.indicator_params = indicator_params
        self.sfilter = sfilter
        self.sfilter_params = sfilter_params

    # the goal here is to
    # - apply range of indicators configs to the train data
    # - pick a couple of the bottom, middle, and top results
    # - apply those to the test data
    # - analyze how well they translate


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
#     FilterOptions.win_points: 10,
#     FilterOptions.loss_points: 3,
#     FilterOptions.threshold_intervals: 20,
# }

# # generate results
# results = sfilter.apply(df, field_name, filter_options)

# # analyze results
# basic_report = BasicReport()
# pnl, count, average, median = basic_report.analyze(df, FilterNames.filter_cumsum.value)

# logger.info("------------------------")

# logger.info(f"Final PnL = {pnl}")
