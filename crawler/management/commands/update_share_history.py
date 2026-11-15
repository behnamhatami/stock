import asyncio
import logging
from functools import partial
from typing import Any

from django.core.management.base import BaseCommand, CommandParser

from crawler.helper import Crawler
from crawler.models import Share, DailyHistory, Contract

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help: str = 'Update share history'
    requires_migrations_checks: bool = True

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument('--concurrency', type=int, default=10, help='maximum simultaneous requests')

    def handle(self, *args: Any, **options: Any) -> None:
        history_count: int = DailyHistory.objects.filter(share__isnull=False).count()
        share_list: list[Share] = list(Share.objects.all())
        contract_list: list[Contract] = list(Contract.objects.all())

        asyncio.run(self.crawl(share_list, contract_list, options['concurrency']))

        new_history_count: int = DailyHistory.objects.filter(share__isnull=False).count()
        logger.info(f"Share history updated. {new_history_count - history_count} added.")

    @staticmethod
    async def crawl(share_list: list[Share], contract_list: list[Contract], concurrency: int) -> None:
        async with Crawler(concurrency=concurrency) as crawler:
            jobs = [partial(crawler.update_share_history_item, share) for share in share_list]
            await crawler.run_jobs("Update Share History", jobs, log=True, log_exception_on_failure=False)

            jobs = [partial(crawler.update_contract_history_item, contract) for contract in contract_list]
            await crawler.run_jobs("Update Contract History", jobs, log=True, log_exception_on_failure=False)
