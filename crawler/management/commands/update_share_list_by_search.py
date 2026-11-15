import asyncio
from typing import Any
import logging
import re
from functools import partial

from django.core.management.base import BaseCommand, CommandParser

from crawler.helper import Crawler
from crawler.models import Share

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help: str = 'Update share list by search'
    requires_migrations_checks: bool = True

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument('--concurrency', type=int, default=10, help='maximum simultaneous requests')

    def handle(self, *args: Any, **options: Any) -> None:
        persian_char: list[str] = ["آ", "ا", "ب", "ت", "ث", "ج", "ح", "خ", "د", "ذ", "ر", "ز", "س", "ش", "ص", "ض", "ط", "ظ", "ع",
                        "غ", "ف", "ق", "ل", "م", "ن", "ه", "و", "پ", "چ", "ژ", "ک", "گ", "ی"]
        tickers: set[str] = set(Share.objects.all().values_list('ticker', flat=True))
        numbered_tickers: set[str] = {re.sub(r'[0-9]+', '', ticker) for ticker in tickers if bool(re.search(r'\d', ticker))} - {
            ''}
        two_chars: list[str] = [x + y for x in persian_char for y in persian_char]

        three_chars: list[str] = []
        for name in two_chars:
            if Share.objects.filter(ticker__contains=name).count() > 30:
                for extra_char in persian_char:
                    three_chars.append(name + extra_char)

        logger.info(f"search for {len(tickers)} ticker name, {len(two_chars)} two and {len(three_chars)} three chars")

        keywords: list[str] = list(tickers | numbered_tickers) + two_chars + three_chars
        asyncio.run(self.crawl(keywords, options['concurrency']))
        logger.info(f"Share list updated. {Share.objects.count() - len(tickers)} new added.")

    @staticmethod
    async def crawl(keywords: list[str], concurrency: int) -> None:
        async with Crawler(concurrency=concurrency) as crawler:
            jobs = [partial(crawler.search_share, name) for name in keywords]
            await crawler.run_jobs("Update Share List by Search", jobs, log=True, log_exception_on_failure=False)

            jobs = [partial(crawler.update_share_identity, share)
                    async for share in Share.objects.filter(identity__isnull=True)]
            await crawler.run_jobs("Update Share Identity Info", jobs, log=True, log_exception_on_failure=False)
