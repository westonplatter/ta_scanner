class BasicReport:
    def __init__(self):
        pass

    def analyze(self, df, field_name):
        pnl = df[field_name].sum()
        return pnl
