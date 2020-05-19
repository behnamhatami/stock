from crawler.analytic import is_upper_buy_closed, is_upper_buy_all_day
from crawler.analyzers.analyzer import Analyzer


class NewComerDropAnalyzer(Analyzer):
    def __init__(self, threshold=50):
        self.threshold = threshold

    def analyze(self, share):
        if share.is_special or share.history_size >= self.threshold or share.history_size <= 1:
            return

        if is_upper_buy_closed(share.daily_history[:-1]) and not is_upper_buy_all_day(share.daily_history[-1:]):
            return {"new commer drop": {"price": share.last_day_history['close']}}
