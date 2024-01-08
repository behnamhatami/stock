from datetime import timedelta

from crawler.analyzers.analyzer import Analyzer
from crawler.models import Share


class AirAnalyzer(Analyzer):
    def __init__(self, threshold=10):
        self.threshold = threshold

    def analyze(self, share: Share, day_offset: int):
        history = share.daily_history(day_offset)

        if share.is_rights_issue or share.day_history(0, day_offset)['date'] >= Share.get_today_new(
                day_offset) - timedelta(
                days=self.threshold):
            return

        last_day_history = share.last_day_history(day_offset)

        if history['high'].max() == last_day_history['high']:
            return {"aired": {"price": last_day_history['high']}}

        if history['low'].min() == last_day_history['low']:
            return {"dumped": {"price": last_day_history['low']}}

        if history[history['date'] >= Share.get_today_new(day_offset) - timedelta(days=30)]['low'].min() == \
                last_day_history['low']:
            return {"monthly lower bound": {"price": last_day_history['low']}}

        if history[history['date'] >= Share.get_today_new(day_offset) - timedelta(days=180)]['low'].min() == \
                last_day_history['low']:
            return {"half year lower bound": {"price": last_day_history['low']}}
