import logging
from functools import partial

from django.core.management.base import BaseCommand

from crawler.helper import search_share, get_share_detailed_info, run_jobs
from crawler.models import Share

logging.basicConfig(level=logging.DEBUG)


class Command(BaseCommand):
    help = 'Update share list by search'
    requires_migrations_checks = True

    def handle(self, *args, **options):
        persian_char = ["آ", "ا", "ب", "ت", "ث", "ج", "ح", "خ", "د", "ذ", "ر", "ز", "س", "ش", "ص", "ض", "ط", "ظ", "ع",
                        "غ", "ف", "ق", "ل", "م", "ن", "ه", "و", "پ", "چ", "ژ", "ک", "گ", "ی"]
        tickers = list(Share.objects.all().values_list('ticker', flat=True))
        two_chars = [x + y for x in persian_char for y in persian_char]

        three_chars = []
        for name in two_chars:
            if Share.objects.filter(ticker__contains=name).count() > 30:
                for extra_char in persian_char:
                    three_chars.append(name + extra_char)

        self.stdout.write(
            f"searching for {len(tickers)} ticker name, {len(two_chars)} two chars and {len(three_chars)} three chars")

        jobs = [partial(search_share, name) for name in tickers + two_chars + three_chars]
        run_jobs("Update Share List by Search", jobs, log=True, log_exception_on_failure=False)
        jobs = [partial(get_share_detailed_info, share) for share in Share.objects.filter(extra_data__isnull=True)]
        run_jobs("Update Share Detail Info", jobs, log=True, log_exception_on_failure=False)
        self.stdout.write(f"Share list updated. {Share.objects.count() - len(tickers)} new added.")
