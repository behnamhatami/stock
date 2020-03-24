from datetime import timedelta

from crawler.analyzers.analyzer import Analyzer
from crawler.models import Share


class AirAnalyzer(Analyzer):
    def __init__(self, threshold=10):
        self.threshold = threshold

    def analyze(self, share):
        history = share.daily_history

        if share.is_rights_issue or share.day_history(0)['Date'] >= Share.get_today() - timedelta(
                days=self.threshold):
            return

        if history['High'].max() == share.last_day_history['High']:
            return {"aired": {"price": share.last_day_history['High']}}

        if history['Low'].min() == share.last_day_history['Low']:
            return {"dumped": {"price": share.last_day_history['Low']}}

        if history[history['Date'] >= Share.get_today() - timedelta(days=30)]['Low'].min() == \
                share.last_day_history['Low']:
            return {"monthly lower bound": {"price": share.last_day_history['Low']}}

        if history[history['Date'] >= Share.get_today() - timedelta(days=180)]['Low'].min() == \
                share.last_day_history['Low']:
            return {"half year lower bound": {"price": share.last_day_history['Low']}}
