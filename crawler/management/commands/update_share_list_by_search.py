import logging
from functools import partial

from django.core.management.base import BaseCommand

from crawler.helper import run_jobs, search_share
from crawler.models import Share

logging.basicConfig(level=logging.DEBUG)


class Command(BaseCommand):
    help = 'Update share list by search'
    requires_migrations_checks = True

    def handle(self, *args, **options):
        persian_char = ["آ", "ا", "ب", "ت", "ث", "ج", "ح", "خ", "د", "ذ", "ر", "ز", "س", "ش", "ص", "ض", "ط", "ظ", "ع",
                        "غ", "ف", "ق", "ل", "م", "ن", "ه", "و", "پ", "چ", "ژ", "ک", "گ", "ی"]
        share_jobs = [partial(search_share, share.ticker) for share in Share.objects.all()]
        char_jobs = [partial(search_share, x + y) for x in persian_char for y in persian_char]
        run_jobs(share_jobs + char_jobs)
        self.stdout.write("Share list updated.")
