import talib

from crawler.analyzers.analyzer import Analyzer


class MACDCross(Analyzer):
    def __init__(self, fast_period=12, slow_period=26, signal_period=9):
        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period

    def analyze(self, share):
        data = share.daily_history
        data['macd'], data['signal'], data['hist'] = talib.MACD(data['Close'], fastperiod=self.fast_period, slowperiod=self.slow_period,
                                                                signalperiod=self.signal_period)

        if data["macd"].iloc[-1] <= data["signal"].iloc[-1] and data["macd"].iloc[-2] > data["signal"].iloc[-2]:
            return "{} cross in MACD dec".format(share)
        elif data["macd"].iloc[-1] < data["signal"].iloc[-1] and data["macd"].iloc[-2] >= data["signal"].iloc[-2]:
            return "{} cross in MACD dec".format(share)
        elif data["macd"].iloc[-1] >= data["signal"].iloc[-1] and data["macd"].iloc[-2] < data["signal"].iloc[-2]:
            return "{} cross in MACD asc".format(share)
        elif data["macd"].iloc[-1] > data["signal"].iloc[-1] and data["macd"].iloc[-2] <= data["signal"].iloc[-2]:
            return "{} cross in MACD asc".format(share)


