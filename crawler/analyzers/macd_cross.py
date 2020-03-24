import talib

from crawler.analyzers.analyzer import Analyzer


class MACDCross(Analyzer):
    def __init__(self, fast_period=12, slow_period=26, signal_period=9):
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period

    def analyze(self, share):
        daily_history = share.daily_history
        daily_history['macd'], daily_history['signal'], daily_history['hist'] = talib.MACD(daily_history['Close'], fastperiod=self.fast_period, slowperiod=self.slow_period,
                                                                signalperiod=self.signal_period)

        if daily_history["macd"].iloc[-1] <= daily_history["signal"].iloc[-1] and daily_history["macd"].iloc[-2] > daily_history["signal"].iloc[-2]:
            return {"MACD": {"trend": "dec"}}
        elif daily_history["macd"].iloc[-1] < daily_history["signal"].iloc[-1] and daily_history["macd"].iloc[-2] >= daily_history["signal"].iloc[-2]:
            return {"MACD": {"trend": "dec"}}
        elif daily_history["macd"].iloc[-1] >= daily_history["signal"].iloc[-1] and daily_history["macd"].iloc[-2] < daily_history["signal"].iloc[-2]:
            return {"MACD": {"trend": "asc"}}
        elif daily_history["macd"].iloc[-1] > daily_history["signal"].iloc[-1] and daily_history["macd"].iloc[-2] <= daily_history["signal"].iloc[-2]:
            return {"MACD": {"trend": "asc"}}
