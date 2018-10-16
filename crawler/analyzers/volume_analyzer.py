from crawler.analyzers.analyzer import Analyzer


class VolumeAnalyzer(Analyzer):
    def __init__(self, threshold=10):
        self.threshold = threshold

    def analyze(self, share):
        if share.daily_history.shape[0] < self.threshold:
            return None

        if share.daily_history.iloc[-self.threshold - 1:-1]['Volume'].mean() * 2 < share.daily_history.iloc[-1]['Volume']:
            return "{} has high volume today!!! ({} -> {})".format(share,
                                                                   share.daily_history.iloc[-self.threshold - 1:-1][
                                                                       'Volume'].mean(),
                                                                   share.daily_history.iloc[-1]['Volume'])
