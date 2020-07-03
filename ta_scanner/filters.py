import pandas as pd


class FilterCumsum:
    def __init__(self, threshold_interval):
        self.name = "filter_cumsum"
        self.threshold_interval = threshold_interval
    
    def apply(self, df: pd.DataFrame, field_name: str, win_points, loss_points):
        query_signals = f"{field_name} != 0"

        for index, rs in df.query(query_signals).iterrows():
            signal_direction = df.loc[index, field_name]
            print(f"{signal_direction} @ {rs.Close}")

            for index_after in range(0, self.threshold_interval):
                df_index = index + index_after
                rx = df.iloc[df_index]
                diff = (rx.Close - rs.Close) * signal_direction

                if diff >= win_points:
                    df.loc[df_index, self.name] = diff
                    print(f"Won @ {rx.Close}. Diff = {diff}")
                    break
                if diff <= (loss_points * -1.0):
                    df.loc[df_index, self.name] = diff
                    print(f"Loss @ {rx.Close}. Diff = {diff}")
                    break

                if index_after == self.threshold_interval -1:
                    print(f"Max time. Diff = {diff}")
                    df.loc[df_index, self.name] = diff
                

