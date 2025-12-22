from functools import partial

from django.core.management.base import BaseCommand

from crawler.helper import update_share_list, update_share_groups, get_share_detailed_info, run_jobs, \
    update_contract_list
from crawler.models import Share


class Command(BaseCommand):
    help = 'Update share list'
    requires_migrations_checks = True

    def handle(self, *args, **options):
        update_share_groups()
        update_share_list()

        jobs = [partial(get_share_detailed_info, share) for share in Share.objects.filter(extra_data__isnull=True)]
        run_jobs("Update Share Detail Info", jobs, log=True, log_exception_on_failure=False)

        update_contract_list()
