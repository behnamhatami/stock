import logging
from functools import partial

from django.core.management.base import BaseCommand

from crawler.helper import run_jobs, update_share_history_item
from crawler.models import Share, ShareDailyHistory

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Update share history'
    requires_migrations_checks = True

    def handle(self, *args, **options):
        history_count = ShareDailyHistory.objects.count()
        share_list = list(Share.objects.all())
        jobs = [partial(update_share_history_item, share, last_update=False) for share in share_list]
        run_jobs("Update Share History", jobs, log=True, log_exception_on_failure=False)
        Share.objects.bulk_update(share_list, ['last_update'], batch_size=100)

        logger.info(f"Share history updated. {ShareDailyHistory.objects.count() - history_count} added.")
