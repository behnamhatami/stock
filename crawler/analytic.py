import talib

from crawler.models import ShareDailyHistory, Share

try:
    import bulbea as bb


    class IranShare(bb.Share):
        def update(self, start=None, end=None, latest=None, cache=False):
            '''
            Update the share with the latest available data.

            :Example:

            >>> import bulbea as bb
            >>> share = bb.Share(source = 'YAHOO', ticker = 'AAPL')
            >>> share.update()
            '''

            self.data = Share.objects.get(ticker=self.ticker).daily_history
            self.length = len(self.data)
            self.attrs = list(self.data.columns)
except:
    pass


def is_upper_buy(history):
    return ((history['High'] == history['Low']) & (history['Tomorrow'] > history['Yesterday'] * 1.048)).all()


def signal_on_extremum(ticker, neutral_tol=0.0, forecast_tol=0.0):
    data = Share.objects.get(ticker=ticker).daily_history
    data['macd'], data['signal'], data['hist'] = talib.MACD(data['close'], fastperiod=12, slowperiod=26, signalperiod=9)

    slope_21 = data["macd"].iat[-2] - data["macd"].iat[-3]
    slope_10 = (data["macd"].iat[-1] - data["macd"].iat[-2])

    # add 0.0000001 to prevent div by zero
    if abs(slope_21) < 0.0000001:
        slope_21 = 0.0000001 if slope_21 >= 0 else -0.0000001

    if abs((slope_10 - slope_21) / slope_21) <= neutral_tol:
        signal = "NEUTRAL"
    elif abs(slope_10 / slope_21) <= forecast_tol:
        signal = "BUY" if slope_21 < 0 else "SELL"
    elif slope_21 > 0 > slope_10:
        signal = "SELL"
    elif slope_21 < 0 < slope_10:
        signal = "BUY"
    else:
        signal = "NEUTRAL"

    return signal
