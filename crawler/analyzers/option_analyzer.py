import pandas as pd

from crawler.analyzers.analyzer import Analyzer


class OptionAnalyzer(Analyzer):
    def __init__(self, threshold=10):
        self.threshold = threshold

    def analyze(self, share):
        active_options = share.options.filter(enable=True)
        if not active_options.exists():
            return

        for letter in ['ض', 'ط']:
            similar_options = active_options.filter(ticker__startswith=letter)
            for date in set(similar_options.values_list('option_strike_date', flat=True)):
                options = list(similar_options.filter(option_strike_date=date).order_by('option_strike_price'))
                for i in range(1, len(options) - 1):
                    if options[i].history_size == 0 or options[i + 1].history_size == 0 or options[
                        i - 1].history_size == 0:
                        continue

                    df = pd.merge(options[i + 1].daily_history, options[i - 1].daily_history, left_on='date',
                                  right_on='date', how='inner', suffixes=('_nxt', '_prv'))
                    df = pd.merge(options[i].daily_history, df, left_on='date', right_on='date', how='inner')
                    df['arbitrage'] = df['close'] / (df['close_nxt'] + df['close_prv']) * 2
                    if df.shape[0] > 0:
                        print(options[i].ticker, options[i].option_strike_price)
                        print(df[['date', 'arbitrage', 'value', 'close', 'close_nxt', 'close_prv']])

                    cur_history = options[i].last_day_history
                    prv_history = options[i - 1].last_day_history
                    nxt_history = options[i + 1].last_day_history
                    if cur_history['date'] == nxt_history['date'] == prv_history['date']:
                        arbitrage = cur_history['close'] / (prv_history['close'] + nxt_history['close']) * 2
                        if abs(1 - arbitrage) > 0.1:
                            print(options[i].ticker, arbitrage)
