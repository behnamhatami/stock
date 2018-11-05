from crawler.analyzers.analyzer import Analyzer


class VolumeAnalyzer(Analyzer):
    def __init__(self, threshold=10):
        self.threshold = threshold

    def analyze(self, share, daily_history, today_history):
        if daily_history.shape[0] < self.threshold:
            return None

        if daily_history.iloc[-self.threshold - 1:-1]['Volume'].mean() * 2 < daily_history.iloc[-1]['Volume']:
            return {"high volume": {"prev_volume": daily_history.iloc[-self.threshold - 1:-1]['Volume'].mean(), "current_volume": daily_history.iloc[-1]['Volume']}}
