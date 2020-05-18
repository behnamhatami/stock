import logging
from functools import partial

from django.core.management.base import BaseCommand

from crawler.helper import run_jobs, update_share_history_item
from crawler.models import Share, ShareDailyHistory

logging.basicConfig(level=logging.DEBUG)


class Command(BaseCommand):
    help = 'Update share history'
    requires_migrations_checks = True

    def handle(self, *args, **options):
        history_count = ShareDailyHistory.objects.count()
        jobs = [partial(update_share_history_item, share) for share in Share.objects.all()]
        run_jobs(jobs)
        self.stdout.write("Share history updated. {} added.".format(ShareDailyHistory.objects.count() - history_count))
