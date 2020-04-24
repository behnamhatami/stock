from crawler.analyzers.analyzer import Analyzer


class VolumeAnalyzer(Analyzer):
    def __init__(self, threshold=10):
        self.threshold = threshold

    def analyze(self, share):
        if share.history_size < self.threshold + 1:
            return None

        last_month_volume = share.daily_history[-self.threshold - 1: -1]['volume'].mean()
        last_day_volume = share.last_day_history['volume']

        if last_month_volume * 2 < last_day_volume:
            return {"high volume": {"last_month_volume": last_month_volume, "last_day_volume": last_day_volume}}
