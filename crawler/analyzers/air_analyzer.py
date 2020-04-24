from datetime import timedelta

from crawler.analyzers.analyzer import Analyzer
from crawler.models import Share


class AirAnalyzer(Analyzer):
    def __init__(self, threshold=10):
        self.threshold = threshold

    def analyze(self, share):
        history = share.daily_history

        if share.is_rights_issue or share.day_history(0)['date'] >= Share.get_today() - timedelta(
                days=self.threshold):
            return

        if history['high'].max() == share.last_day_history['high']:
            return {"aired": {"price": share.last_day_history['high']}}

        if history['low'].min() == share.last_day_history['low']:
            return {"dumped": {"price": share.last_day_history['low']}}

        if history[history['date'] >= Share.get_today() - timedelta(days=30)]['low'].min() == \
                share.last_day_history['low']:
            return {"monthly lower bound": {"price": share.last_day_history['low']}}

        if history[history['date'] >= Share.get_today() - timedelta(days=180)]['low'].min() == \
                share.last_day_history['low']:
            return {"half year lower bound": {"price": share.last_day_history['low']}}
