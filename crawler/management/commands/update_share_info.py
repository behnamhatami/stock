import asyncio
from functools import partial
from typing import Any

from django.core.management.base import BaseCommand, CommandParser

from crawler.helper import Crawler
from crawler.models import Share


class Command(BaseCommand):
    help: str = 'Update share info'
    requires_migrations_checks: bool = True

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument('--concurrency', type=int, default=10, help='maximum simultaneous requests')

    def handle(self, *args: Any, **options: Any) -> None:
        asyncio.run(self.crawl(list(Share.objects.all()), options['concurrency']))

    @staticmethod
    async def crawl(share_list: list[Share], concurrency: int) -> None:
        async with Crawler(concurrency=concurrency) as crawler:
            jobs = [partial(crawler.update_share_identity, share) for share in share_list]
            await crawler.run_jobs("Update Share Identity Info", jobs, log=True, log_exception_on_failure=False)

            await crawler.update_contract_list()
