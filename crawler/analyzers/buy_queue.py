from crawler.analytic import is_upper_buy_closed
from crawler.analyzers.analyzer import Analyzer


class BuyQueueAnalyzer(Analyzer):
    def __init__(self, threshold=2):
        self.threshold = threshold

    def analyze(self, share):
        if share.daily_history.shape[0] < self.threshold:
            return None

        if is_upper_buy_closed(share.daily_history[-self.threshold:]):
            return {"buy queue": {"days": self.threshold}}
