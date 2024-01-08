from crawler.analytic import is_upper_buy_closed
from crawler.analyzers.analyzer import Analyzer
from crawler.models import Share


class BuyQueueAnalyzer(Analyzer):
    def __init__(self, threshold=2):
        self.threshold = threshold

    def analyze(self, share: Share, day_offset: int):
        if share.history_size(day_offset) < self.threshold:
            return None

        if is_upper_buy_closed(share.daily_history(day_offset)[-self.threshold:]):
            return {"buy queue": {"days": self.threshold}}
