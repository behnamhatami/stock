import bulbea as bb
import talib

from crawler.helper import update_stock_history_item
from crawler.models import Share, ShareHistory


class IranShare(bb.Share):
    def update(self, start=None, end=None, latest=None, cache=False):
        '''
        Update the share with the latest available data.

        :Example:

        >>> import bulbea as bb
        >>> share = bb.Share(source = 'YAHOO', ticker = 'AAPL')
        >>> share.update()
        '''

        self.data = ShareHistory.get_historical_data(self.ticker)
        self.length = len(self.data)
        self.attrs = list(self.data.columns)


def signal_on_cross(ticker):
    """
    Generate `BUY`, `SELL`, or `NEUTRAL` signals based on cross point of MACD and signal lines.

    :param pandas.DataFrame df: containing MACD and signal values for two consecutive days
    :return:
    """
    data = ShareHistory.get_historical_data(self.ticker)
    data['macd'], data['signal'], data['hist'] = talib.MACD(prices, fastperiod=12, slowperiod=26, signalperiod=9)

    if df["macd"].iat[-1] <= df["signal"].iat[-1] and df["macd"].iat[-2] > df["signal"].iat[-2]:
        return "SELL"
    elif df["macd"].iat[-1] < df["signal"].iat[-1] and df["macd"].iat[-2] >= df["signal"].iat[-2]:
        return "SELL"
    elif df["macd"].iat[-1] >= df["signal"].iat[-1] and df["macd"].iat[-2] < df["signal"].iat[-2]:
        return "BUY"
    elif df["macd"].iat[-1] > df["signal"].iat[-1] and df["macd"].iat[-2] <= df["signal"].iat[-2]:
        return "BUY"
    else:
        return "NEUTRAL"


def signal_on_extremum(ticker, neutral_tol=0.0, forecast_tol=0.0):
    """
    Generate `BUY`, `SELL`, or `NEUTRAL` signals based on analysis of MACD line

    :param pandas.DataFrame df: containing MACD values for three consecutive days
    :param float neutral_tol: a float in range [0, 1]. An absolute relative slope change of r=abs((s2-s1)/s1)
    is ALWAYS considered as a NEUTRAL signal, if r < neutral_tol.
    :param float forecast_tol: a float in range [0, 1]. A relative slope of r=|s2/s1| is ALWAYS considered as a
    BUY/SELL signal, if r < forecast_tol
    :return:
    """

    data = ShareHistory.get_historical_data(self.ticker)
    data['macd'], data['signal'], data['hist'] = talib.MACD(prices, fastperiod=12, slowperiod=26, signalperiod=9)

    slope_21 = df["macd"].iat[-2] - df["macd"].iat[-3]
    slope_10 = (df["macd"].iat[-1] - df["macd"].iat[-2])

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
