from datetime import timedelta

from crawler.analyzers.analyzer import Analyzer
from crawler.models import Share


class AirAnalyzer(Analyzer):
    def __init__(self, threshold=5):
        self.threshold = threshold

    def analyze(self, share: Share, day_offset: int):
        history = share.daily_history(day_offset)

        if (share.base_share or share.day_history(0, day_offset)['date'] >=
                Share.get_today_new(day_offset) - timedelta(days=self.threshold)):
            return

        last_day_history = share.last_day_history(day_offset)

        last_month_history = history[history['date'] >= Share.get_today_new(day_offset) - timedelta(days=30)]
        half_year_history = history[history['date'] >= Share.get_today_new(day_offset) - timedelta(days=180)]

        if (history['high'].max() == last_day_history['high'] and len(half_year_history) > 60 and len(
                last_month_history[last_month_history['high'] == last_day_history['high']]) == 1):
            return {"aired": {"price": last_day_history['high']}}

        if (history['low'].min() == last_day_history['low'] and len(half_year_history) > 60 and len(
                last_month_history[last_month_history['low'] == last_day_history['low']] == 1)):
            return {"dumped": {"price": last_day_history['low']}}

        if half_year_history['close'].max() * 0.8 >= half_year_history['low'].min() == last_day_history['low'] and len(
                half_year_history) > 60:
            return {"half year lower bound": {"price": last_day_history['low']}}

        if last_month_history['close'].max() * 0.9 >= last_month_history['low'].min() == last_day_history[
            'low'] and len(last_month_history) > 10:
            return {"monthly lower bound": {"price": last_day_history['low']}}
