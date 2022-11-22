from stockstats import StockDataFrame

from crawler.models import Share

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


def is_upper_buy_closed(history):
    return (history['last'] > history['open'] * 1.068).all()


def is_upper_buy_all_day(history):
    return (history['low'] > history['open'] * 1.068).all()


def signal_on_extremum(ticker, neutral_tol=0.0, forecast_tol=0.0):
    data = StockDataFrame.retype(Share.objects.get(ticker=ticker).daily_history)

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
