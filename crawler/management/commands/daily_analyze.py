import logging
from datetime import date, timedelta

from django.core.management.base import BaseCommand

from crawler.analyzers import *
from crawler.models import Share
from django.conf import settings

import pandas as pd

logging.basicConfig(level=logging.DEBUG)


class Command(BaseCommand):
    help = 'Analyze Daily History'
    requires_migrations_checks = True

    def __init__(self, *args, **kwargs):
        # self.daily_analyzers = [VolumeAnalyzer(), BuyQueue(), CheapRightIssue(), GoodPriceRightIssue(), NewComerDrop(), MACDCross(), AirAnalyzer()]
        self.daily_analyzers = [GoodPriceRightIssue()]
        super().__init__(*args, **kwargs)

    def add_arguments(self, parser):
        parser.add_argument('days', type=int, nargs='?', default=0)

    def handle(self, *args, **options):
        Share.DAY_OFFSET = options.get('days', 0)

        row_list = []
        for share in Share.objects.all().order_by('ticker'):
            if share.history_size > 0 and share.last_day_history['Date'] >= Share.get_today() - timedelta(days=1):
                results = dict()
                for analyzer in self.daily_analyzers:
                    result = analyzer.analyze(share)
                    if result:
                        results.update({key: str(value) for key, value in result.items()})

                if results:
                    ticker_link = '<a href="http://www.tsetmc.com/Loader.aspx?ParTree=151311&i={id}">{ticker}</a>'.format(
                        id=share.id, ticker=share.ticker)
                    row_list.append({"ticker": ticker_link, **results})
                    self.stdout.write("{}".format({share.ticker: results}))


        if row_list:
            df = pd.DataFrame(row_list)
            df.sort_values(by=list(df), inplace=True)
            pd.set_option('display.max_colwidth', None)

            from django.template import loader
            template = loader.get_template('daily_report.html')
            html_out = template.render({'date': date.today() - timedelta(days=Share.DAY_OFFSET + 1),
                                        'daily_report_dataframe': df.to_html(escape=False)})

            with open(settings.BASE_DIR + "/report.html", 'w') as f:
                f.write(html_out)
