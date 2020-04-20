from crawler.analyzers.analyzer import Analyzer

import pandas as pd 

class OptionAnalyzer(Analyzer):
    def __init__(self, threshold=10):
        self.threshold = threshold

    def analyze(self, share):
        active_options = share.options.filter(enable=True)
        if not active_options.exists():
            return
        
        for type in ['ض', 'ط']:
            similar_options = active_options.filter(ticker__startswith=type)
            for date in set(similar_options.values_list('option_strike_date', flat=True)):
                options = list(similar_options.filter(option_strike_date=date).order_by('option_strike_price'))
                for i in range(1, len(options) - 1):
                    if options[i].history_size == 0 or options[i+1].history_size == 0 or options[i-1].history_size == 0:
                        continue
                    
                    df = pd.merge(options[i+1].daily_history, options[i-1].daily_history, left_on='Date', right_on='Date', how='inner', suffixes=('_nxt', '_prv'))
                    df = pd.merge(options[i].daily_history, df, left_on='Date', right_on='Date', how='inner')
                    df['arbitrage'] = df['Tomorrow'] / (df['Tomorrow_nxt'] + df['Tomorrow_prv']) * 2
                    if df.shape[0] > 0:
                        print(options[i].ticker, options[i].option_strike_price)
                        print(df[['Date', 'arbitrage', 'Value', 'Tomorrow', 'Tomorrow_nxt', 'Tomorrow_prv']])

                    cur_history = options[i].last_day_history
                    prv_history = options[i-1].last_day_history
                    nxt_history = options[i+1].last_day_history 
                    if cur_history['Date'] == nxt_history['Date'] == prv_history['Date']:
                        arbitrage = cur_history['Tomorrow'] / (prv_history['Tomorrow'] + nxt_history['Tomorrow']) * 2
                        if abs(1 - arbitrage) > 0.1:
                            print(options[i].ticker, arbitrage)