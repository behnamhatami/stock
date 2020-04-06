from crawler.analyzers.analyzer import Analyzer
from crawler.models import Share
from datetime import date, timedelta
import matplotlib.pyplot as plt

import pandas as pd 


class GoodPriceRightIssue(Analyzer):
    def analyze(self, share):
        if share.is_rights_issue:
            main_share = Share.objects.get(ticker=share.ticker[:-1])
            df = pd.merge(share.daily_history, main_share.daily_history, left_on='Date', right_on='Date', how='inner')
            # df['diff'] = df['Tomorrow_y'] / (df['Tomorrow_x'] + 1000)
            # df = df[df['Date'] >= Share.get_today() - timedelta(days=365)]
            # if (df['diff'] > 1.2).any():
            #     print(share.ticker, df['diff'].max(), df[df['diff'] == df['diff'].max()]['Date'])

            #     df.plot(kind='line',x='Date',y='diff')
            #     plt.title(share.ticker)
            #     plt.show()

            if main_share.last_day_history['Tomorrow'] / (share.last_day_history['Tomorrow'] + 1000) > 1.1:
                return {"good price right issue": {"price": share.last_day_history['Tomorrow'], 
                                                    "main price": main_share.last_day_history['Tomorrow']}}
