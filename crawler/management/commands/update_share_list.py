import asyncio
from functools import partial
from typing import Any

from django.core.management.base import BaseCommand, CommandParser

from crawler.helper import Crawler
from crawler.models import Share


class Command(BaseCommand):
    help: str = 'Update share list'
    requires_migrations_checks: bool = True

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument('--concurrency', type=int, default=10, help='maximum simultaneous requests')

    def handle(self, *args: Any, **options: Any) -> None:
        asyncio.run(self.crawl(options['concurrency']))

    @staticmethod
    async def crawl(concurrency: int) -> None:
        async with Crawler(concurrency=concurrency) as crawler:
            await crawler.update_share_groups()
            await crawler.update_share_list()

            jobs = [partial(crawler.update_share_identity, share)
                    async for share in Share.objects.filter(identity__isnull=True)]
            await crawler.run_jobs("Update Share Identity Info", jobs, log=True, log_exception_on_failure=False)

            await crawler.update_contract_list()
