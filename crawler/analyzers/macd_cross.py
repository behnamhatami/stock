from stockstats import StockDataFrame

from crawler.analyzers.analyzer import Analyzer
from crawler.models import Share


class MACDCrossAnalyzer(Analyzer):
    def __init__(self, fast_period=12, slow_period=26, signal_period=9):
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period

    def analyze(self, share: Share, day_offset: int):
        StockDataFrame.MACD_EMA_SHORT = self.fast_period
        StockDataFrame.MACD_EMA_LONG = self.slow_period
        StockDataFrame.MACD_EMA_SIGNAL = self.signal_period

        if share.history_size(day_offset) < 2 or share.base_share:
            return

        daily_history = StockDataFrame.retype(share.daily_history(day_offset).copy())

        if daily_history["macd"].iloc[-1] <= daily_history["macds"].iloc[-1] and daily_history["macd"].iloc[-2] > \
                daily_history["macds"].iloc[-2]:
            return {"MACD": {"trend": "dec"}}
        elif daily_history["macd"].iloc[-1] < daily_history["macds"].iloc[-1] and daily_history["macd"].iloc[-2] >= \
                daily_history["macds"].iloc[-2]:
            return {"MACD": {"trend": "dec"}}
        elif daily_history["macd"].iloc[-1] >= daily_history["macds"].iloc[-1] and daily_history["macd"].iloc[-2] < \
                daily_history["macds"].iloc[-2]:
            return {"MACD": {"trend": "asc"}}
        elif daily_history["macd"].iloc[-1] > daily_history["macds"].iloc[-1] and daily_history["macd"].iloc[-2] <= \
                daily_history["macds"].iloc[-2]:
            return {"MACD": {"trend": "asc"}}
