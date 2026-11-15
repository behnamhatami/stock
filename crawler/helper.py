import asyncio
import contextlib
import json
import logging
import time
import typing
from email.utils import parsedate_to_datetime
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta, timezone as dt_timezone
from typing import Any, Final, TypeAlias, TypeVar
from urllib.parse import urlsplit

import aiohttp
import orjson
from asgiref.sync import sync_to_async
from django.db import transaction
from django.utils import timezone
from django.utils.timezone import make_aware
from getuseragent import UserAgent
from persiantools import characters
from persiantools.jdatetime import JalaliDate
from tenacity import _utils, retry_if_exception_type
from tenacity import stop_after_attempt, wait_random_exponential, retry, RetryCallState

from crawler.decorators import log_time
from crawler.models import Share, DailyHistory, ShareGroup, Contract
from crawler.time_helper import convert_integer_to_parts

logger = logging.getLogger(__name__)
user_agent = UserAgent()

JSONDict: TypeAlias = dict[str, Any]
Headers: TypeAlias = dict[str, str]
QueryParams: TypeAlias = dict[str, str | int]
Job: TypeAlias = Callable[[], Awaitable[Any]]

TSE_API: Final[str] = 'https://cdn.tsetmc.com/api'
CDC_API: Final[str] = 'https://dataapi.ime.co.ir/api/CDC'
T = TypeVar('T')


RETRYABLE_STATUS_CODES: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})
MAX_RETRY_AFTER_SECONDS: float = 300


class HttpError(Exception):
    """A response the crawler cannot use. Only `TransientHttpError` is retried."""

    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class TransientHttpError(HttpError):
    """Rate limiting (429), a transient server error (5xx, 408, 425), or an empty / HTML error page."""

    def __init__(self, message: str, status_code: int | None = None, retry_after: float | None = None) -> None:
        super().__init__(message, status_code)
        self.retry_after = retry_after


class PermanentHttpError(HttpError):
    """Any other non-200 response, such as 400, 401, 403 or 404. Retrying would not help."""


def parse_retry_after(value: str | None, now: datetime | None = None) -> float | None:
    """Seconds to wait from a Retry-After header, given as seconds or as an HTTP date."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return float(value)
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=dt_timezone.utc)
    return max(0.0, (when - (now or datetime.now(dt_timezone.utc))).total_seconds())


_backoff = wait_random_exponential(multiplier=1, max=60)


def wait_backoff_or_retry_after(retry_state: RetryCallState) -> float:
    """Exponential backoff with jitter, but never shorter than the server's Retry-After (capped)."""
    delay = _backoff(retry_state)
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    retry_after = getattr(exc, 'retry_after', None)
    if retry_after is not None:
        delay = max(delay, min(retry_after, MAX_RETRY_AFTER_SECONDS))
    return delay


# Network failures, timeouts and transient responses are retried. Programming errors, a closed
# session (RuntimeError), permanent HTTP errors and cancellation are raised at once.
retry_transient = retry_if_exception_type((aiohttp.ClientError, asyncio.TimeoutError, TransientHttpError))


def after_log(
        logger: logging.Logger,
        log_level: int,
        sec_format: str = "%0.3f",
) -> Callable[[RetryCallState], None]:
    """After call strategy that logs to some logger the finished attempt."""

    def log_it(retry_state: RetryCallState) -> None:
        args: list[Any]
        kwargs: dict[str, Any]
        if retry_state.fn is None:
            # NOTE(sileht): can't really happen, but we must please mypy
            fn_name = "<unknown>"
            args, kwargs = [], {}
        else:
            fn_name = _utils.get_callback_name(retry_state.fn)
            args, kwargs = list(retry_state.args), dict(retry_state.kwargs)
            if args and isinstance(args[0], Crawler):
                args = args[1:]
            for key in {'headers', 'timeout'}:
                if key in kwargs:
                    del kwargs[key]

        logger.log(
            log_level,
            f"Finished call to '{fn_name}/{args}/{kwargs} "
            f"after {sec_format % retry_state.seconds_since_start}(s), "
            f"this was the {_utils.to_ordinal(retry_state.attempt_number)} time calling it.",
        )

    return log_it


@dataclass(frozen=True)
class Response:
    """The fully read body of an HTTP response, with the parts of `requests.Response` this project uses."""
    url: str
    status_code: int
    content: bytes
    encoding: str = 'utf-8'
    headers: Mapping[str, str] = field(default_factory=dict)

    @property
    def text(self) -> str:
        return self.content.decode(self.encoding, errors='replace')

    def json(self) -> Any:
        # orjson parses the raw bytes directly (UTF-8), without building an intermediate str.
        return orjson.loads(self.content)


class Crawler:
    """Async TSETMC / IME crawler.

    One `aiohttp.ClientSession` is shared by every request and every domain: aiohttp pools
    connections per host inside one session, and headers are set per request, so a session per
    domain is not needed. Concurrency is limited per request attempt (the slot is released while
    a failed request waits for its retry), globally and optionally per host.

        async with Crawler(concurrency=10) as crawler:
            await crawler.update_share_history_item(share)

    The class only talks HTTP; everything that reads or writes the database is in `CrawlerStorage`.
    """

    def __init__(self, concurrency: int = 10, concurrency_per_host: int | None = None) -> None:
        if concurrency < 1:
            raise ValueError('concurrency must be at least 1')
        self.concurrency: int = concurrency
        self.concurrency_per_host: int | None = concurrency_per_host
        self._session: aiohttp.ClientSession | None = None
        self._semaphore: asyncio.Semaphore = asyncio.Semaphore(concurrency)
        self._host_semaphores: dict[str, asyncio.Semaphore] = {}

    async def __aenter__(self) -> typing.Self:
        self._semaphore = asyncio.Semaphore(self.concurrency)
        self._host_semaphores = {}
        connector = aiohttp.TCPConnector(limit=self.concurrency, limit_per_host=self.concurrency_per_host or 0,
                                         ssl=False)  # same as `verify=False` before
        self._session = aiohttp.ClientSession(connector=connector, json_serialize=self._json_serialize)
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, exc: BaseException | None,
                        tb: typing.Any) -> None:
        if self._session is not None:
            await self._session.close()
        self._session = None

    @staticmethod
    def _json_serialize(obj: Any) -> str:
        return orjson.dumps(obj).decode()

    @property
    def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            raise RuntimeError('Crawler session is not open; use "async with Crawler() as crawler:"')
        return self._session

    def _host_semaphore(self, url: str) -> asyncio.Semaphore | None:
        if not self.concurrency_per_host:
            return None
        host = urlsplit(url).netloc
        if host not in self._host_semaphores:
            self._host_semaphores[host] = asyncio.Semaphore(self.concurrency_per_host)
        return self._host_semaphores[host]

    @staticmethod
    def get_tse_new_site_headers() -> Headers:
        return {
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.5',
            'Origin': 'https://www.tsetmc.com',
            'DNT': '1',
            'Referer': 'https://www.tsetmc.com/',
            'User-Agent': user_agent.Random(),
        }

    async def submit_request(self, method: str, url: str, attempts: int = 6, **kwargs: Any) -> Response:
        """One HTTP request, tried up to `attempts` times (e.g. 2 for a polling loop). `kwargs` are the
        options of `_submit_request` (params, headers, data, retry_on_*_response, timeout)."""
        submit = Crawler._submit_request.retry_with(stop=stop_after_attempt(attempts))
        return await submit(self, method=method, url=url, **kwargs)

    @retry(reraise=True, wait=wait_backoff_or_retry_after, after=after_log(logger, logging.DEBUG),
           retry=retry_transient)
    async def _submit_request(self, method: str, url: str, params: QueryParams | None = None,
                              headers: Headers | None = None, data: JSONDict | None = None,
                              retry_on_empty_response: bool = False, retry_on_html_response: bool = False,
                              timeout: float = 5) -> Response:
        """One attempt; the number of attempts is set by `submit_request`."""
        # Take the per-host slot first, so a busy host does not hold global slots other hosts could use.
        async with self._host_semaphore(url) or contextlib.nullcontext():
            async with self._semaphore:
                async with self.session.request(method.upper(), url, params=params or None, headers=headers,
                                                json=data, timeout=aiohttp.ClientTimeout(total=timeout)) as raw:
                    response = Response(url=str(raw.url), status_code=raw.status, content=await raw.read(),
                                        encoding=raw.get_encoding(), headers=dict(raw.headers))

        name = url.split('/')[-1]
        if response.status_code != 200:
            message = f"Http Error: {response.status_code}, {name}, {params}"
            if response.status_code in RETRYABLE_STATUS_CODES:
                raise TransientHttpError(message, response.status_code,
                                         parse_retry_after(response.headers.get('Retry-After')))
            raise PermanentHttpError(message, response.status_code)

        if retry_on_empty_response and len(response.text.strip()) == 0:
            raise TransientHttpError(f"Http Error: empty response, {name}, {params}", response.status_code)

        if retry_on_html_response and ('<html>' in response.text or '<!doctype html>' in response.text):
            raise TransientHttpError(f"Http Error: html response, {name}, {params}", response.status_code)

        return response

    @staticmethod
    def parse_json(response: Response) -> Any:
        """JSON from a response, tolerating a UTF-8 BOM and JSON encoded twice (a JSON string), as some fund
        sites send."""
        try:
            data = orjson.loads(response.content)
        except orjson.JSONDecodeError:
            data = json.loads(response.text.lstrip('\ufeff'))
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                pass
        return data

    async def get_json(self, url: str, *, method: str = 'get', headers: Headers | None = None,
                       data: JSONDict | None = None, params: QueryParams | None = None, timeout: float = 25,
                       attempts: int = 6, retry_on_empty_response: bool = False,
                       retry_on_html_response: bool = False) -> Any:
        """JSON from any site (fund NAV pages, ...), with a random User-Agent unless `headers` sets one."""
        response = await self.submit_request(method=method, url=url, params=params, data=data, timeout=timeout,
                                             headers={'User-Agent': user_agent.Random(), **(headers or {})},
                                             retry_on_empty_response=retry_on_empty_response,
                                             retry_on_html_response=retry_on_html_response, attempts=attempts)
        return self.parse_json(response)

    async def tse_json(self, path: str, *, params: QueryParams | None = None, timeout: float = 25,
                       attempts: int = 6, retry_on_empty_response: bool = False) -> Any:
        """JSON from the TSETMC API, e.g. `await crawler.tse_json('/BestLimits/<id>')`."""
        return await self.get_json(TSE_API + path, headers=self.get_tse_new_site_headers(), params=params,
                                   timeout=timeout, attempts=attempts, retry_on_html_response=True,
                                   retry_on_empty_response=retry_on_empty_response)

    async def ime_json(self, path: str, *, method: str = 'get', data: JSONDict | None = None, timeout: float = 25,
                       attempts: int = 6) -> Any:
        """JSON from the IME certificate-of-deposit API, e.g. `await crawler.ime_json('/CDCLiveMarket')`."""
        return await self.get_json(CDC_API + path, method=method, data=data, timeout=timeout, attempts=attempts)

    # Raw TSETMC / IME data: each returns the API's own rows; callers decide how to store or shape them.

    async def get_market_overview(self, market: int, **kwargs: Any) -> JSONDict:
        """Index, state and last activity time of a market (1: bourse, 2: fara bourse)."""
        return (await self.tse_json(f'/MarketData/GetMarketOverview/{market}', **kwargs))['marketOverview']

    async def get_market_watch(self, h_even: int = 0, ref_id: int = 0, with_best_limits: bool = False,
                               **kwargs: Any) -> list[JSONDict]:
        """Whole-market snapshot from the new TSETMC market watch (replaces old.tsetmc MarketWatchInit).

        `h_even` / `ref_id` are the incremental cursor the market watch page uses; 0 / 0 returns every
        instrument. Fields used by `CrawlerStorage.save_share_list` (old MarketWatchInit column in brackets):
        insCode [0 id], lva [2 ticker], lvc [3 description], eps [14], bv [15 base volume], flow [17 bazaar type],
        csv [18 sector code, space padded], pMax / pMin [19 / 20 price band], ztd [21 number of shares],
        yVal [22 bazaar group]. Prices: pDrCotVal last, pClosing close, pf first, py yesterday, pmn / pmx low / high,
        zTotTran / qTotTran5J / qTotCap count / volume / value. With best limits, `blDs` holds the 5 levels.
        """
        response = await self.tse_json(
            '/ClosingPrice/GetMarketWatch', retry_on_empty_response=True,
            params={'market': 0, 'paperTypes[0]': 1, 'showTraded': 'false',
                    'withBestLimits': 'true' if with_best_limits else 'false', 'hEven': h_even, 'RefID': ref_id},
            **kwargs)
        return response['marketwatch']

    async def get_trades(self, share_id: int, **kwargs: Any) -> list[JSONDict]:
        """Today's trades (`nTran`, `hEven`, `qTitTran`, `pTran`, ...)."""
        return (await self.tse_json(f'/Trade/GetTrade/{share_id}', **kwargs))['trade']

    async def get_trade_history(self, share_id: int, day: date, summarize: bool = True,
                                **kwargs: Any) -> list[JSONDict]:
        """Trades of a past day; `summarize=False` also returns cancelled trades (`canceled`)."""
        path = f'/Trade/GetTradeHistory/{share_id}/{day:%Y%m%d}/{"true" if summarize else "false"}'
        return (await self.tse_json(path, **kwargs))['tradeHistory']

    async def get_best_limits_history(self, share_id: int, day: date, **kwargs: Any) -> list[JSONDict]:
        """Every change of the 5 best limits on `day`, sorted by (`hEven`, `refID`)."""
        rows = (await self.tse_json(f'/BestLimits/{share_id}/{day:%Y%m%d}', **kwargs))['bestLimitsHistory']
        return sorted(rows, key=lambda row: (row['hEven'], row.get('refID') or 0))

    async def get_etf(self, share_id: int, **kwargs: Any) -> JSONDict | None:
        """ETF data (`pRedTran` NAV, `deven`/`hEven` publish time), or None when TSETMC has none."""
        return (await self.tse_json(f'/Fund/GetETFByInsCode/{share_id}', **kwargs) or {}).get('etf')

    async def get_cdc_live_market(self, **kwargs: Any) -> list[JSONDict]:
        """IME certificate-of-deposit contracts with their live trades."""
        return await self.ime_json('/CDCLiveMarket', **kwargs)

    async def get_cdc_trades(self, commodity_id: int, from_date: date, to_date: date,
                             **kwargs: Any) -> list[JSONDict]:
        """Daily IME trades of one commodity between two dates (at most 90 rows per call)."""
        response = await self.ime_json('/CDCTrades', method='post', data={
            "fromDate": str(from_date), "toDate": str(to_date), "pageNumber": 1, "pageSize": 90, "marketId": 22,
            "customFilter": str(commodity_id)}, **kwargs)
        return response['Data']

    @log_time
    async def run_jobs(self, job_title: str, jobs: Sequence[Job], log: bool = True,
                       log_exception_on_failure: bool = True) -> tuple[int, int]:
        """Run coroutine factories (e.g. `partial(crawler.search_share, name)`) concurrently.

        All jobs are scheduled at once; the number of simultaneous HTTP requests is limited by
        the crawler's `concurrency`, not by this function. Returns (succeeded, failed).
        """
        number_of_buckets = max(min(20, len(jobs) // 10), 2)
        tasks: list[asyncio.Future[Any]] = [asyncio.ensure_future(job()) for job in jobs]

        success = 0
        try:
            for index, future in enumerate(asyncio.as_completed(tasks)):
                try:
                    await future
                except Exception as e:
                    if log:
                        if log_exception_on_failure:
                            logger.exception(f'{job_title}: job crashed!', exc_info=e)
                        else:
                            logger.warning(f'{job_title}: job crashed!', exc_info=e)
                else:
                    success += 1

                if log:
                    if int((index + 1) * number_of_buckets / len(tasks)) - int(index * number_of_buckets / len(tasks)):
                        percent = round((index + 1) / len(tasks) * 100, 2)
                        logger.info(f"{job_title}: {success}/{index + 1} out of {len(tasks)}({percent}%)")
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        logger.info(f"Task {job_title}: {success} tasks completed out of {len(tasks)}")
        return success, len(tasks) - success

    @log_time
    async def update_share_groups(self) -> None:
        response = await self.tse_json('/StaticData/GetStaticData')
        await sync_to_async(CrawlerStorage.save_share_groups)(response['staticData'])

    async def update_share_identity(self, share: Share) -> None:
        response: JSONDict = (await self.tse_json(f'/Instrument/GetInstrumentIdentity/{share.id}'))[
            'instrumentIdentity']
        await sync_to_async(CrawlerStorage.save_share_identity)(share, response)

    async def search_share(self, keyword: str) -> None:
        response = await self.tse_json(f'/Instrument/GetInstrumentSearch/{keyword}')
        await sync_to_async(CrawlerStorage.save_search_result)(keyword, response['instrumentSearch'])

    async def update_share_history_item(self, share: Share, days: int | None = None, batch_size: int = 100) -> None:
        """Download recent daily history and store it together with `share.last_update`.

        Missing dates are inserted even when newer or older dates already exist. The checkpoint is the time
        the request started and is written in the same transaction as the rows, so a failed request or
        insert never advances it.
        """
        if days is None:
            days = (timezone.now() - share.last_update).days + 1 if share.last_update else 0

        requested_at = timezone.now()
        response = await self.tse_json(f'/ClosingPrice/GetClosingPriceDailyList/{share.id}/{days}')
        await sync_to_async(CrawlerStorage.save_share_history)(share, response['closingPriceDaily'],
                                                               requested_at, batch_size)

    @log_time
    async def update_share_list(self, batch_size: int = 100) -> None:
        rows = await self.get_market_watch()
        await sync_to_async(CrawlerStorage.save_share_list)(rows, batch_size)

    async def update_contract_list(self, batch_size: int = 100) -> None:
        live_response = await self.get_cdc_live_market(timeout=5)
        await sync_to_async(CrawlerStorage.save_contract_list)(live_response, batch_size)

    async def update_contract_history_item(self, contract: Contract, days: int | None = None,
                                           batch_size: int = 100) -> None:
        if days is None:
            days = await sync_to_async(CrawlerStorage.contract_history_days)(contract)

        rows: list[JSONDict] = []
        for i in range(days // 90 + 1):
            from_date = date.today() - timedelta(days=min(days, (i + 1) * 90 - 1))
            to_date = min(date.today(), from_date + timedelta(days=90 - 1))
            trades = await self.get_cdc_trades(contract.commodity_id, from_date, to_date, timeout=30)
            if not trades:
                break
            rows.extend(trades)

        await sync_to_async(CrawlerStorage.save_contract_history)(contract, rows, batch_size)


class CrawlerStorage:
    """Every database read and write of the crawler, kept apart from the HTTP code in `Crawler`.

    All methods are synchronous Django ORM code. From async code call them with
    `await sync_to_async(CrawlerStorage.save_share_list)(rows, batch_size)`; each one takes the API rows
    exactly as the matching `Crawler` getter returns them.
    """

    @staticmethod
    def save_share_groups(rows: list[JSONDict]) -> None:
        names: dict[int, str] = {int(item['code']): characters.ar_to_fa(item['name']).strip()
                                 for item in rows if item['type'] == 'IndustrialGroup'}
        existing: dict[int, ShareGroup] = ShareGroup.objects.in_bulk(list(names))

        new_list = [ShareGroup(id=code, name=name) for code, name in names.items() if code not in existing]
        update_list: list[ShareGroup] = []
        for code, name in names.items():
            group = existing.get(code)
            if group is not None and group.name != name:
                group.name = name
                update_list.append(group)

        with transaction.atomic():
            ShareGroup.objects.bulk_create(new_list, batch_size=100)
            ShareGroup.objects.bulk_update(update_list, ['name'], batch_size=100)

        logger.info(f"Share group info updated. {len(new_list)} added, {len(update_list)} renamed, "
                    f"{len(names)} groups in total.")

    @staticmethod
    def save_share_identity(share: Share, response: JSONDict) -> None:
        try:
            response = orjson.loads(characters.ar_to_fa(orjson.dumps(response).decode()))
            group_id = int(response['sector']['cSecVal'].strip())
            group: ShareGroup | None = ShareGroup.objects.filter(id=group_id).first()
            if group is None:
                logger.warning(f'{share.ticker}: unknown sector {group_id}; run update_share_groups')

            if share.identity and share.identity != response:
                logger.warning(f'{share.ticker}: identity changed from {share.identity} to {response}')

            if (share.identity, share.group, share.isin) == (response, group, response['instrumentID']):
                return

            share.identity, share.group, share.isin = response, group, response['instrumentID']
            share.save(update_fields=['identity', 'group', 'isin'])
        except:
            logger.exception(f'{share.ticker} update share detailed info failed {response}!!')
            raise

    @staticmethod
    def save_search_result(keyword: str, rows: list[JSONDict]) -> None:
        found: dict[int, JSONDict] = {}
        for row in rows:
            share_id = int(row['insCode'])
            found.setdefault(share_id, {
                'id': share_id,
                'ticker': characters.ar_to_fa(str(row['lVal18AFC'])).strip(),
                'description': characters.ar_to_fa(row['lVal30']).strip(),
                'bazaar_type': row['flow'],
                'enable': row['lastDate'],
            })
        existing: dict[int, Share] = Share.objects.in_bulk(list(found))

        new_list: list[Share] = []
        update_list: list[Share] = []
        for share_id, data in found.items():
            share = existing.get(share_id)
            if share is None:
                new_list.append(Share(**data))
                continue

            updated: bool = False
            for key, value in data.items():
                if getattr(share, key) != value:
                    setattr(share, key, value)
                    updated = True

            parsed_data = share.parse_data()
            if (share.strike_date, share.option_strike_price, share.base_share) != parsed_data:
                share.strike_date, share.option_strike_price, share.base_share = parsed_data
                updated = True

            if updated:
                update_list.append(share)

        with transaction.atomic():
            Share.objects.bulk_create(new_list, batch_size=100)
            Share.objects.bulk_update(update_list,
                                      ['ticker', 'description', 'bazaar_type', 'enable', 'option_strike_price',
                                       'strike_date', 'base_share'], batch_size=100)
        if new_list or update_list:
            logger.info(f"update share list {keyword}, {len(new_list)} added ({new_list}), "
                        f"{len(update_list)} updated.")

    @staticmethod
    def save_share_list(rows: list[JSONDict], batch_size: int) -> None:
        shares: dict[int, Share] = Share.objects.in_bulk([int(row['insCode']) for row in rows])
        groups: dict[int, ShareGroup] = ShareGroup.objects.in_bulk()

        new_list: list[Share] = []
        update_list: list[Share] = []
        for row in rows:
            share_id = int(row['insCode'])
            share = shares.get(share_id) or Share()

            try:
                share.enable = True
                share.ticker = characters.ar_to_fa(str(row['lva'])).strip()
                share.description = characters.ar_to_fa(row['lvc']).strip()
                share.eps = int(row['eps']) if row['eps'] else None
                share.base_volume = int(row['bv'])
                share.bazaar_type = row['flow']
                share.group = groups[int(row['csv'].strip())]
                share.total_count = int(row['ztd'])
                share.bazaar_group = row['yVal']
                share.strike_date, share.option_strike_price, share.base_share = share.parse_data()
                (update_list if share.id else new_list).append(share)
                share.id = share_id
            except Exception:
                logger.warning(f'{share.ticker} parse share data failed! {row}!!')

        Share.objects.bulk_create(new_list, batch_size=batch_size)
        Share.objects.bulk_update(update_list,
                                  ['enable', 'ticker', 'description', 'eps', 'base_volume', 'bazaar_type', 'group',
                                   'total_count', 'bazaar_group', 'option_strike_price', 'strike_date', 'base_share'],
                                  batch_size=100)
        logger.info(f"update share list, {len(new_list)} ({new_list}) added, {len(update_list)} updated.")

    @staticmethod
    def parse_share_history(share: Share, rows: list[JSONDict]) -> list[DailyHistory]:
        """Traded days only, one per date, oldest first."""
        by_date: dict[date, DailyHistory] = {}
        for row in rows:
            if row['zTotTran'] == 0:
                continue
            day = date(*convert_integer_to_parts(row['dEven']))
            by_date[day] = DailyHistory(share=share, date=day,
                                        high=row['priceMax'],
                                        low=row['priceMin'],
                                        close=row['pClosing'],
                                        last=row['priceYesterday'] + row['priceChange'],
                                        first=row['priceFirst'],
                                        open=row['priceYesterday'],
                                        value=row['qTotCap'],
                                        volume=row['qTotTran5J'],
                                        count=row['zTotTran'])
        return [by_date[day] for day in sorted(by_date)]

    @staticmethod
    def without_existing(histories: list[DailyHistory], **owner: Share | Contract) -> list[DailyHistory]:
        """Drop dates already stored for this share or contract, with one query for the whole date range."""
        if not histories:
            return []
        dates = [history.date for history in histories]
        existing: set[date] = set(DailyHistory.objects.filter(date__gte=min(dates), date__lte=max(dates), **owner)
                                  .values_list('date', flat=True))
        return [history for history in histories if history.date not in existing]

    @classmethod
    def save_share_history(cls, share: Share, rows: list[JSONDict], requested_at: datetime,
                           batch_size: int) -> None:
        histories = cls.parse_share_history(share, rows)
        with transaction.atomic():
            new_histories = cls.without_existing(histories, share=share)
            # ignore_conflicts: another crawler process may insert the same date concurrently.
            DailyHistory.objects.bulk_create(new_histories, batch_size=batch_size, ignore_conflicts=True)
            Share.objects.filter(id=share.id).update(last_update=requested_at)
        share.last_update = requested_at

        if new_histories:
            logger.info(f"history of {share.ticker} in {len(new_histories)} days added.")

    CONTRACT_FIELDS: typing.ClassVar[dict[str, str]] = {
        'code': 'ContractCode', 'description': 'ContractDescription', 'size': 'ContractSize',
        'commodity_id': 'CommodityID', 'commodity_name': 'CommodityName'}

    @classmethod
    def save_contract_list(cls, rows: list[JSONDict], batch_size: int) -> None:
        found: dict[int, JSONDict] = {int(row['ContractID']): row for row in rows}
        existing: dict[int, Contract] = Contract.objects.in_bulk(list(found))

        new_list: list[Contract] = []
        update_list: list[Contract] = []
        for contract_id, row in found.items():
            values = {field_name: row[key] for field_name, key in cls.CONTRACT_FIELDS.items()}
            contract = existing.get(contract_id)
            if contract is None:
                new_list.append(Contract(id=contract_id, **values))
            elif any(getattr(contract, field_name) != value for field_name, value in values.items()):
                for field_name, value in values.items():
                    setattr(contract, field_name, value)
                update_list.append(contract)

        with transaction.atomic():
            Contract.objects.bulk_create(new_list, batch_size=batch_size)
            Contract.objects.bulk_update(update_list, list(cls.CONTRACT_FIELDS), batch_size=100)
        logger.info(f"update contract list, {len(new_list)} ({new_list}) added, {len(update_list)} updated.")

    @staticmethod
    def contract_history_days(contract: Contract) -> int:
        return (date.today() - contract.last_day_history()['date']).days - 1 if contract.history_size() > 0 else 200000

    @staticmethod
    def parse_contract_history(contract: Contract, rows: list[JSONDict]) -> list[DailyHistory]:
        """Valid days only, one per date, oldest first."""
        by_date: dict[date, DailyHistory] = {}
        for row in rows:
            history = DailyHistory(contract=contract,
                                   date=make_aware(datetime.fromisoformat(row['DT'])).date(),
                                   high=row['MaxPrice'],
                                   low=row['MinPrice'],
                                   close=row['TodaySettlementPrice'],
                                   last=row['LastPrice'],
                                   first=row['FirstPrice'],
                                   open=row['LastSettlementPrice'],
                                   value=row['TradesValue'],
                                   volume=row['TradesVolume'],
                                   count=row['C_Buy'] + row['C_Sell'])

            if history.last == 0 or history.open == 0 or history.first == 0:
                continue

            by_date[history.date] = history
        return [by_date[day] for day in sorted(by_date)]

    @classmethod
    def save_contract_history(cls, contract: Contract, rows: list[JSONDict], batch_size: int) -> None:
        histories = cls.parse_contract_history(contract, rows)
        with transaction.atomic():
            new_histories = cls.without_existing(histories, contract=contract)
            DailyHistory.objects.bulk_create(new_histories, batch_size=batch_size, ignore_conflicts=True)

        if new_histories:
            logger.info(f"history of {contract.code} in {len(new_histories)} days added.")


def run_with_crawler(job: Callable[[Crawler], Awaitable[T]], concurrency: int = 10,
                     concurrency_per_host: int | None = None) -> T:
    """Open a crawler, run `job(crawler)` and close it, from synchronous code (commands, crons):

        run_with_crawler(lambda crawler: asyncio.gather(*(crawler.search_share(k) for k in keywords)))
    """

    async def main() -> T:
        async with Crawler(concurrency=concurrency, concurrency_per_host=concurrency_per_host) as crawler:
            return await job(crawler)

    return asyncio.run(main())


def _int(value: Any) -> int:
    return int(value or 0)


def tse_quote(row: JSONDict) -> dict[str, Any]:
    """One market watch row with the old MarketWatchInit column names (plus nav and best prices).

    Checked against GetClosingPriceInfo on 2026-11-15: pf first, pcl close, pdv last, py yesterday (`open`
    here, as in DailyHistory), pmn / pmx low / high, ztt / qtj / qtc count / volume / value, hEven time,
    pRedTran ETF NAV (0 when none).
    """
    return {
        'IR': row.get('insID'),
        'ticker': characters.ar_to_fa(str(row.get('lva', ''))).strip(),
        'description': characters.ar_to_fa(str(row.get('lvc', ''))).strip(),
        'time': _int(row.get('hEven')),
        'first': _int(row.get('pf')),
        'close': _int(row.get('pcl')),
        'last': _int(row.get('pdv')),
        'count': _int(row.get('ztt')),
        'volume': _int(row.get('qtj')),
        'value': _int(row.get('qtc')),
        'low': _int(row.get('pmn')),
        'high': _int(row.get('pmx')),
        'open': _int(row.get('py')),
        'eps': _int(row.get('eps')) or None,
        'base_volume': _int(row.get('bv')),
        'bazaar_type': row.get('flow'),
        'group': _int(str(row.get('csv') or '0').strip() or 0),
        'max_price_possible': _int(row.get('pMax')),
        'min_price_possible': _int(row.get('pMin')),
        'number_of_share': _int(row.get('ztd')),
        'bazaar_group': row.get('yVal'),
        'nav': _int(row.get('pRedTran')),
        'best_buy_price': _int(row.get('pmd')),
        'best_sell_price': _int(row.get('pmo')),
    }


ORDER_BOOK_FIELDS: Final[dict[str, str]] = {'zmd': 'buy_count', 'qmd': 'buy_volume', 'pmd': 'buy_price',
                                            'pmo': 'sell_price', 'qmo': 'sell_volume', 'zmo': 'sell_count'}


@dataclass
class TSEMarketWatch:
    """The whole TSETMC market kept up to date with as little download as possible.

    The first `refresh` downloads every instrument with its 5 best limits (about 3.7 MB). Later calls pass
    the cursor the TSETMC market watch page uses (`hEven` = latest quote time, `RefID` = latest book event
    id) and get only the instruments that changed, each with only its changed levels, which are merged in:
    one request per refresh. Every `full_refresh_seconds`, or after a failed incremental call, everything is
    downloaded again; this also resets the cursor for a new trading day and picks up NAV (`pRedTran`)
    changes, which incremental responses do not report on their own. Only full refreshes read the market
    overviews, so `activity_time` (the market's last activity, for display) is up to `full_refresh_seconds`
    old. Everything is kept in plain dictionaries.
    """
    full_refresh_seconds: float = 30
    rows: dict[int, JSONDict] = field(default_factory=dict)
    order_books: dict[int, dict[int, dict[str, int]]] = field(default_factory=dict)
    h_even: int = 0
    ref_id: int = 0
    activity_time: datetime | None = None
    markets: tuple[int, ...] = (1, 2)
    _last_full_refresh: float = float('-inf')

    async def refresh(self, crawler: Crawler) -> int:
        """Download the changes (or everything) and merge them. Returns the number of instruments received."""
        full = not self.rows or time.monotonic() - self._last_full_refresh >= self.full_refresh_seconds
        try:
            received = await self._download(crawler, full)
        except Exception:
            if full:
                raise
            logger.warning('incremental market watch failed; downloading the full market watch', exc_info=True)
            full, received = True, await self._download(crawler, True)

        logger.debug(f'market watch {"full" if full else "incremental"}: {received} instruments, '
                     f'hEven={self.h_even}, RefID={self.ref_id}')
        return received

    async def _read_activity_time(self, crawler: Crawler) -> datetime | None:
        """The latest activity time of `markets`, or None when no overview could be read."""
        overviews = await asyncio.gather(*(crawler.get_market_overview(market, attempts=2, timeout=10)
                                           for market in self.markets), return_exceptions=True)
        times = [t for t in (self._activity_time(overview) for overview in overviews
                             if not isinstance(overview, BaseException)) if t is not None]
        return max(times) if times else None

    @staticmethod
    def _activity_time(overview: JSONDict) -> datetime | None:
        data = overview or {}
        if not data.get('marketActivityDEven'):
            return None
        year, month, day = convert_integer_to_parts(int(data['marketActivityDEven']))
        hour, minute, second = convert_integer_to_parts(int(data.get('marketActivityHEven') or 0))
        return timezone.make_aware(datetime(year, month, day, hour, minute, second))

    async def _download(self, crawler: Crawler, full: bool) -> int:
        if not full:
            rows = await crawler.get_market_watch(self.h_even, self.ref_id, with_best_limits=True, attempts=2,
                                                  timeout=15)
        else:
            rows, activity_time = await asyncio.gather(
                crawler.get_market_watch(0, 0, with_best_limits=True, attempts=2, timeout=30),
                self._read_activity_time(crawler))
            self.rows, self.order_books, self.h_even, self.ref_id = {}, {}, 0, 0
            self.activity_time = activity_time or self.activity_time
            self._last_full_refresh = time.monotonic()
        self.merge(rows)
        return len(rows)

    def merge(self, rows: list[JSONDict]) -> None:
        for row in rows:
            share_id = int(row['insCode'])
            self.rows[share_id] = {key: value for key, value in row.items() if key != 'blDs'}
            book = self.order_books.setdefault(share_id, {})
            for level in row.get('blDs') or []:
                book[int(level['n'])] = {name: _int(level.get(key)) for key, name in ORDER_BOOK_FIELDS.items()}
                self.ref_id = max(self.ref_id, _int(level.get('rid')))
            self.h_even = max(self.h_even, _int(row.get('hEven')))

    def live_data(self) -> dict[int, dict[str, Any]]:
        """share id -> quote (see `tse_quote`)."""
        return {share_id: tse_quote(row) for share_id, row in self.rows.items()}

    def order_book(self, share_id: int, nav: int | None = None) -> list[dict[str, Any]]:
        """The 5 best limits, best first: `order`, buy/sell count, volume and price, and with a NAV also
        `buy_price_normalized` / `sell_price_normalized`."""
        levels: list[dict[str, Any]] = []
        for order, values in sorted(self.order_books.get(share_id, {}).items()):
            level: dict[str, Any] = {'order': order, **values}
            if nav:
                level['buy_price_normalized'] = values['buy_price'] / nav
                level['sell_price_normalized'] = values['sell_price'] / nav
            levels.append(level)
        return levels

    def nav(self, share_id: int) -> int:
        """The ETF NAV published with the market watch (`pRedTran`), 0 when unknown."""
        return _int((self.rows.get(share_id) or {}).get('pRedTran'))


def ime_quote(row: JSONDict, today: date | None = None) -> dict[str, Any]:
    """One CDCLiveMarket row as a quote (the analyzer's `CDC_DATA` layout).

    `open` is yesterday's settlement price and `close` today's, both taken from `LastSettlementPrice` by its
    Jalali date (`LastSettlementPricePersianDate`); otherwise 0. Field names follow the CDC API; they have not
    been checked against a live response from outside Iran.
    """
    today = today or timezone.localdate()
    settle_text = row.get('LastSettlementPricePersianDate')
    settle_date = JalaliDate.fromisoformat(settle_text.replace('/', '-')).to_gregorian() if settle_text else today
    settlement = row.get('LastSettlementPrice') or 0
    return {
        'id': int(row['ContractID']),
        'code': row['ContractCode'],
        'description': row.get('ContractDescription'),
        'open': settlement if settle_date == today - timedelta(days=1) else 0,
        'close': settlement if settle_date == today else 0,
        'first': row.get('FirstTradedPrice') or 0,
        'last': row.get('LastTradedPrice') or 0,
        'count': row.get('TradesCount') or 0,
        'volume': row.get('TradesVolume') or 0,
        'value': row.get('TradesValue') or 0,
        'low': row.get('LowTradedPrice') or 0,
        'high': row.get('HighTradedPrice') or 0,
        'datetime': make_aware(datetime.fromisoformat(row['LastUpdate'])),
    }


@dataclass
class IMEMarketWatch:
    """The IME certificate-of-deposit market (CDCLiveMarket), kept as plain dictionaries.

    Unlike TSETMC, the CDC API has no incremental cursor: every `refresh` downloads the whole (small) list
    and replaces each contract's row. `refresh` returns how many contracts changed.
    """
    rows: dict[int, JSONDict] = field(default_factory=dict)
    last_update: datetime | None = None

    async def refresh(self, crawler: Crawler) -> int:
        rows = await crawler.get_cdc_live_market(attempts=2, timeout=10)
        return self.merge(rows)

    def merge(self, rows: list[JSONDict]) -> int:
        changed = 0
        for row in rows:
            try:
                contract_id = int(row['ContractID'])
            except (KeyError, TypeError, ValueError):
                logger.warning(f'IME live market: row without a contract id {row}')
                continue
            if self.rows.get(contract_id) != row:
                self.rows[contract_id] = row
                changed += 1
            if row.get('LastUpdate'):
                try:
                    updated = make_aware(datetime.fromisoformat(row['LastUpdate']))
                except ValueError:
                    continue
                self.last_update = max(self.last_update, updated) if self.last_update else updated
        return changed

    def live_data(self, today: date | None = None) -> dict[int, dict[str, Any]]:
        """contract id -> quote (see `ime_quote`); rows that cannot be read are logged and skipped."""
        quotes: dict[int, dict[str, Any]] = {}
        for contract_id, row in self.rows.items():
            try:
                quotes[contract_id] = ime_quote(row, today)
            except (KeyError, TypeError, ValueError):
                logger.warning(f'IME live market: cannot read {row}', exc_info=True)
        return quotes
