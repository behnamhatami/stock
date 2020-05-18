import logging
from functools import partial

from django.core.management.base import BaseCommand

from crawler.helper import update_share_list, update_share_groups, get_share_detailed_info, run_jobs
from crawler.models import Share

logging.basicConfig(level=logging.DEBUG)


class Command(BaseCommand):
    help = 'Update share list'
    requires_migrations_checks = True

    def handle(self, *args, **options):
        update_share_groups()
        update_share_list()

        run_jobs([partial(get_share_detailed_info, share) for share in Share.objects.filter(extra_data=None)])

        self.stdout.write("Share list updated.")
