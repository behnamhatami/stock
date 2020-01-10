import asyncio
import logging

from django.core.management.base import BaseCommand

from crawler.helper import update_stock_history

logging.basicConfig(level=logging.DEBUG)


class Command(BaseCommand):
    help = 'Update stock history'
    requires_migrations_checks = True

    def handle(self, *args, **options):
        update_stock_history()
        self.stdout.write("Stock history updated.")
