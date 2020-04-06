import asyncio
import logging

from functools import partial

from django.core.management.base import BaseCommand

from crawler.helper import run_jobs, update_stock_history_item
from crawler.models import Share

logging.basicConfig(level=logging.DEBUG)


class Command(BaseCommand):
    help = 'Update stock history'
    requires_migrations_checks = True

    def handle(self, *args, **options):
        jobs = [partial(update_stock_history_item, share) for share in Share.objects.all()]
        run_jobs(jobs)
        self.stdout.write("Stock history updated.")
