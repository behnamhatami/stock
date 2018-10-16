from crawler.analytic import is_upper_buy
from crawler.analyzers.analyzer import Analyzer


class BuyQueue(Analyzer):
    def __init__(self, threshold=2):
        self.threshold = threshold

    def analyze(self, share):
        if share.daily_history.shape[0] < self.threshold:
            return None

        if is_upper_buy(share.daily_history[-self.threshold:]):
            return "{} has buy queue for {} days!!!".format(share, self.threshold)
