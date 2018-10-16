from datetime import date, timedelta

from crawler.analytic import is_upper_buy
from crawler.analyzers.analyzer import Analyzer


class NewComer(Analyzer):
    def __init__(self, threshold=30):
        self.threshold = threshold

    def analyze(self, share):
        if share.is_rights_issue or share.daily_history.iloc[0]['Date'] <= date.today() - timedelta(days=self.threshold) or share.daily_history.shape[0] == 1:
            return

        if is_upper_buy(share.daily_history[:-1]) and not is_upper_buy(share.daily_history[-1:]):
            return "{} is a new comer with price {}".format(share, share.daily_history.iloc[-1]['Tomorrow'])
