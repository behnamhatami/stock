import logging
from datetime import timedelta

import pandas as pd
from django.conf import settings
from django.core.management.base import BaseCommand

from crawler.analyzers import *
from crawler.models import Share

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Analyze Daily History'
    requires_migrations_checks = True

    def __init__(self, *args, **kwargs):
        self.daily_analyzers = [AirAnalyzer(), BuyQueueAnalyzer(), CheapRightIssueAnalyzer(),
                                GoodPriceRightIssueAnalyzer(), MACDCrossAnalyzer(), OptionAnalyzer(),
                                NewComerDropAnalyzer(), VolumeAnalyzer()]

        super().__init__(*args, **kwargs)

    def add_arguments(self, parser):
        parser.add_argument('days', type=int, nargs='?', default=0)

    def handle(self, *args, **options):
        day_offset = options.get('days', 0)

        row_list = []
        today = Share.get_today_new(day_offset)
        # traded on the last two days (a history up to `today` whose last date is at least today - 1)
        recent_shares = Share.objects.filter(history__date__range=(today - timedelta(days=1), today)).distinct()
        for share in recent_shares.order_by('ticker'):
            results = dict()
            for analyzer in self.daily_analyzers:
                result = analyzer.analyze(share, day_offset)
                if result:
                    results.update({key: str(value) for key, value in result.items()})

            if results:
                ticker_link = f'<a href="https://www.tsetmc.com/instInfo/{share.id}">{share.ticker}</a>'
                row_list.append({"ticker": ticker_link, **results})
                logger.info(f"{share.ticker}: {results}")

        if row_list:
            df = pd.DataFrame(row_list)
            df.sort_values(by=list(df), inplace=True)
            pd.set_option('display.max_colwidth', None)

            from django.template import loader
            template = loader.get_template('daily_report.html')
            html_out = template.render({'date': Share.get_today_new(day_offset),
                                        'daily_report_dataframe': df.to_html(escape=False)})

            with open(settings.BASE_DIR + "/data/report.html", 'w') as f:
                f.write(html_out)
