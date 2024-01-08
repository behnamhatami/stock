from crawler.analytic import is_upper_buy_closed, is_upper_buy_all_day
from crawler.analyzers.analyzer import Analyzer
from crawler.models import Share


class NewComerDropAnalyzer(Analyzer):
    def __init__(self, threshold=50):
        self.threshold = threshold

    def analyze(self, share: Share, day_offset: int):
        if share.is_special or share.history_size(day_offset) >= self.threshold or share.history_size(day_offset) <= 1:
            return

        if is_upper_buy_closed(share.daily_history(day_offset)[:-1]) and not is_upper_buy_all_day(
                share.daily_history(day_offset)[-1:]):
            return {"new comer drop": {"price": share.last_day_history(day_offset)['close']}}
