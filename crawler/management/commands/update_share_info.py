from functools import partial

from django.core.management.base import BaseCommand

from crawler.helper import get_share_detailed_info, run_jobs, \
    update_contract_list, update_share_identity
from crawler.models import Share


class Command(BaseCommand):
    help = 'Update share info'
    requires_migrations_checks = True

    def handle(self, *args, **options):
        jobs = [partial(get_share_detailed_info, share) for share in Share.objects.all()]
        run_jobs("Update Share Detail Info", jobs, log=True, log_exception_on_failure=False)

        jobs = [partial(update_share_identity, share) for share in Share.objects.all()]
        run_jobs("Update Share Identity Info", jobs, log=True, log_exception_on_failure=False)

        update_contract_list()
