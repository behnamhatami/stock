from datetime import date, timedelta

from crawler.analytic import is_upper_buy
from crawler.analyzers.analyzer import Analyzer


class NewComer(Analyzer):
    def __init__(self, threshold=30):
        self.threshold = threshold

    def analyze(self, share, daily_history, today_history):
        if share.is_rights_issue or daily_history.iloc[0]['Date'] <= date.today() - timedelta(days=self.threshold) or daily_history.shape[0] == 1:
            return

        if is_upper_buy(daily_history[:-1]) and not is_upper_buy(daily_history[-1:]):
            return {"new commer drop": {"price": daily_history.iloc[-1]['Tomorrow']}}
