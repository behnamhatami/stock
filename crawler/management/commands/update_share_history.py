import logging
from functools import partial

from django.core.management.base import BaseCommand

from crawler.helper import run_jobs, update_share_history_item, update_contract_history_item
from crawler.models import Share, DailyHistory, Contract

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Update share history'
    requires_migrations_checks = True

    def handle(self, *args, **options):
        history_count: int = DailyHistory.objects.filter(share__isnull=False).count()
        share_list: list[Share] = list(Share.objects.all())
        jobs = [partial(update_share_history_item, share, last_update=False) for share in share_list]
        run_jobs("Update Share History", jobs, log=True, log_exception_on_failure=False)
        Share.objects.bulk_update(share_list, ['last_update'], batch_size=100)

        contract_list: list[Contract] = list(Contract.objects.all())
        jobs = [partial(update_contract_history_item, contract) for contract in contract_list]
        run_jobs("Update Contract History", jobs, log=True, log_exception_on_failure=False)

        new_history_count: int = DailyHistory.objects.filter(share__isnull=False).count()
        logger.info(f"Share history updated. {new_history_count - history_count} added.")
