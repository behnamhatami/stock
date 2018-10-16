from datetime import date, timedelta

from crawler.analyzers.analyzer import Analyzer


class AirAnalyzer(Analyzer):
    def __init__(self, threshold=10):
        self.threshold = threshold

    def analyze(self, share):
        if share.is_rights_issue or share.daily_history.iloc[0]['Date'] >= date.today() - timedelta(
                days=self.threshold):
            return

        if share.daily_history['High'].max() == share.daily_history.iloc[-1]['High']:
            return "{} has been aired with price {}".format(share, share.daily_history.iloc[-1]['High'])

        if share.daily_history['Low'].min() == share.daily_history.iloc[-1]['Low']:
            return "{} has been dumped with price {}".format(share, share.daily_history.iloc[-1]['Low'])

        if share.daily_history[share.daily_history['Date'] >= date.today() - timedelta(days=30)]['Low'].min() == \
                share.daily_history.iloc[-1]['Low']:
            return "{} has been break the lower bound in one month with price {}".format(share,
                                                                                         share.daily_history.iloc[-1][
                                                                                             'Low'])

        if share.daily_history[share.daily_history['Date'] >= date.today() - timedelta(days=180)]['Low'].min() == \
                share.daily_history.iloc[-1]['Low']:
            return "{} has been break the lower bound in six month with price {}".format(share,
                                                                                         share.daily_history.iloc[-1][
                                                                                             'Low'])
