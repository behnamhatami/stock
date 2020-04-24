import logging

from django.core.management.base import BaseCommand

from crawler.helper import update_share_list, update_share_groups

logging.basicConfig(level=logging.DEBUG)


class Command(BaseCommand):
    help = 'Update share list'
    requires_migrations_checks = True

    def handle(self, *args, **options):
        update_share_groups()
        update_share_list()
        self.stdout.write("Share list updated.")
