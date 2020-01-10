import asyncio
import logging

from django.core.management.base import BaseCommand

from crawler.helper import update_stock_list


logging.basicConfig(level=logging.DEBUG)


class Command(BaseCommand):
    help = 'Update stock list'
    requires_migrations_checks = True

    def handle(self, *args, **options):
        update_stock_list()
        self.stdout.write("Stock list updated.")
