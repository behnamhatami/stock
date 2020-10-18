from crawler.analyzers.analyzer import Analyzer
from crawler.models import Share


# from datetime import date, timedelta
# import matplotlib.pyplot as plt

# import pandas as pd


class GoodPriceRightIssueAnalyzer(Analyzer):
    def analyze(self, share):
        if share.is_rights_issue:
            main_share = share.base_share
            # df = pd.merge(share.daily_history, main_share.daily_history, left_on='Date', right_on='Date', how='inner')
            # df['diff'] = df['Close_y'] / (df['Close_x'] + 1000)
            # df = df[df['Date'] >= Share.get_today() - timedelta(days=365)]
            # if (df['diff'] > 1.2).any():
            #     print(share.ticker, df['diff'].max(), df[df['diff'] == df['diff'].max()]['Date'])

            #     df.plot(kind='line',x='Date',y='diff')
            #     plt.title(share.ticker)
            #     plt.show()

            if main_share.last_day_history['close'] / (share.last_day_history['close'] + 1000) > 1.1:
                return {"good price right issue": {"price": share.last_day_history['close'],
                                                   "main price": main_share.last_day_history['close']}}
