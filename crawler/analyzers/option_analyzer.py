import logging

import pandas as pd

from crawler.analyzers.analyzer import Analyzer
from crawler.models import Share

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class OptionAnalyzer(Analyzer):
    def __init__(self, threshold=10):
        self.threshold = threshold

    def analyze(self, share):
        active_options = share.options.filter(enable=True)
        if not active_options.exists():
            return

        for letter in ['ض', 'ط']:
            similar_options = active_options.filter(ticker__startswith=letter)
            for d in set(similar_options.values_list('strike_date', flat=True)):
                options = list(similar_options.filter(strike_date=d).order_by('option_strike_price'))
                for i in range(1, len(options) - 1):
                    if options[i].history_size == 0 or options[i + 1].history_size == 0 or options[
                        i - 1].history_size == 0:
                        continue

                    df = pd.merge(options[i + 1].daily_history, options[i - 1].daily_history, left_on='date',
                                  right_on='date', how='inner', suffixes=('_nxt', '_prv'))
                    df = pd.merge(options[i].daily_history, df, left_on='date', right_on='date', how='inner')
                    df['arbitrage'] = df['close'] / (df['close_nxt'] + df['close_prv']) * 2
                    if df.shape[0] > 0 and abs(1 - df.iloc[-1]['arbitrage']) > 0.1 and df.iloc[-1][
                        'date'] == Share.get_today():
                        logger.info(f'{options[i].ticker}, {df[["date", "arbitrage"]]}')

