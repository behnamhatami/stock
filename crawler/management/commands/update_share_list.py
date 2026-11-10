from functools import partial

from django.core.management.base import BaseCommand

from crawler.helper import update_share_list, update_share_groups, run_jobs, update_contract_list, update_share_identity
from crawler.models import Share


class Command(BaseCommand):
    help = 'Update share list'
    requires_migrations_checks = True

    def handle(self, *args, **options):
        update_share_groups()
        update_share_list()

        jobs = [partial(update_share_identity, share) for share in Share.objects.filter(identity__isnull=True)]
        run_jobs("Update Share Identity Info", jobs, log=True, log_exception_on_failure=False)

        update_contract_list()
