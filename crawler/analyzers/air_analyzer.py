from datetime import date, timedelta

from crawler.analyzers.analyzer import Analyzer


class AirAnalyzer(Analyzer):
    def __init__(self, threshold=10):
        self.threshold = threshold

    def analyze(self, share, daily_history, today_history):
        if share.is_rights_issue or daily_history.iloc[0]['Date'] >= date.today() - timedelta(
                days=self.threshold):
            return

        if daily_history['High'].max() == daily_history.iloc[-1]['High']:
            return {"aired": {"price": daily_history.iloc[-1]['High']}}

        if daily_history['Low'].min() == daily_history.iloc[-1]['Low']:
            return {"dumped": {"price": daily_history.iloc[-1]['Low']}}

        if daily_history[daily_history['Date'] >= date.today() - timedelta(days=30)]['Low'].min() == \
                daily_history.iloc[-1]['Low']:
            return {"monthly lower bound": {"price": daily_history.iloc[-1]['Low']}}

        if daily_history[daily_history['Date'] >= date.today() - timedelta(days=180)]['Low'].min() == \
                daily_history.iloc[-1]['Low']:
            return {"half year lower bound": {"price": daily_history.iloc[-1]['Low']}}
