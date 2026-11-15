import asyncio
from typing import Any
from datetime import date, datetime, timezone as dt_timezone
from types import SimpleNamespace
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
from aiohttp import web
from aiohttp.test_utils import TestServer
from django.core.management import call_command
from django.db import IntegrityError, connection
from django.test import TestCase, TransactionTestCase
from django.test.utils import CaptureQueriesContext
from django.utils import timezone
from tenacity import wait_none

from crawler.decorators import log_time
from crawler.helper import (Crawler, CrawlerStorage, IMEMarketWatch, PermanentHttpError, Response, TransientHttpError,
                            TSEMarketWatch, ime_quote, parse_retry_after, run_with_crawler, tse_quote,
                            wait_backoff_or_retry_after)
from crawler.models import Share, DailyHistory, ShareGroup, Contract


def json_response(payload: Any, status_code: int = 200) -> Response:
    return Response(url='https://cdn.tsetmc.com/api/test', status_code=status_code, content=orjson.dumps(payload))


class SubmitRequestTests(IsolatedAsyncioTestCase):
    """`submit_request` against a local aiohttp server: concurrency limit, retries and response checks."""

    async def asyncSetUp(self) -> None:
        self.in_flight: int = 0
        self.max_in_flight: int = 0
        self.calls: dict[str, int] = {}

        async def slow(request: web.Request) -> web.Response:
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
            await asyncio.sleep(0.05)
            self.in_flight -= 1
            return web.json_response({'ok': True})

        async def flaky(request: web.Request) -> web.Response:
            self.calls['flaky'] = self.calls.get('flaky', 0) + 1
            if self.calls['flaky'] == 1:
                return web.Response(status=500)
            return web.json_response({'attempt': self.calls['flaky']})

        async def html(request: web.Request) -> web.Response:
            return web.Response(text='<html>blocked</html>', content_type='text/html')

        async def echo(request: web.Request) -> web.Response:
            body = await request.json() if request.can_read_body else None
            return web.json_response({'method': request.method, 'query': dict(request.query), 'body': body,
                                      'agent': request.headers.get('User-Agent')})

        async def not_found(request: web.Request) -> web.Response:
            self.calls['not_found'] = self.calls.get('not_found', 0) + 1
            return web.Response(status=404)

        async def rate_limited(request: web.Request) -> web.Response:
            self.calls['rate_limited'] = self.calls.get('rate_limited', 0) + 1
            if self.calls['rate_limited'] == 1:
                return web.Response(status=429, headers={'Retry-After': '0'})
            return web.json_response({'attempt': self.calls['rate_limited']})

        async def unavailable(request: web.Request) -> web.Response:
            self.calls['unavailable'] = self.calls.get('unavailable', 0) + 1
            return web.Response(status=503)

        async def bom(request: web.Request) -> web.Response:
            return web.Response(body='\ufeff{"a": 1}'.encode(), content_type='application/json')

        async def twice(request: web.Request) -> web.Response:
            return web.Response(body=orjson.dumps('{"nav": 5}'), content_type='application/json')

        app = web.Application()
        app.router.add_get('/bom', bom)
        app.router.add_get('/twice', twice)
        app.router.add_get('/not_found', not_found)
        app.router.add_get('/rate_limited', rate_limited)
        app.router.add_get('/unavailable', unavailable)
        app.router.add_get('/slow', slow)
        app.router.add_get('/flaky', flaky)
        app.router.add_get('/html', html)
        app.router.add_route('*', '/echo', echo)
        self.server = TestServer(app)
        await self.server.start_server()
        self.no_wait = patch.object(Crawler._submit_request.retry, 'wait', wait_none())
        self.no_wait.start()

    async def asyncTearDown(self) -> None:
        self.no_wait.stop()
        await self.server.close()

    def url(self, path: str) -> str:
        return str(self.server.make_url(path))

    async def test_limits_simultaneous_requests(self) -> None:
        async with Crawler(concurrency=3) as crawler:
            await asyncio.gather(*(crawler.submit_request('get', self.url('/slow')) for _ in range(12)))
        self.assertEqual(self.max_in_flight, 3)

    async def test_per_host_limit(self) -> None:
        async with Crawler(concurrency=10, concurrency_per_host=2) as crawler:
            await asyncio.gather(*(crawler.submit_request('get', self.url('/slow')) for _ in range(8)))
        self.assertEqual(self.max_in_flight, 2)

    async def test_retries_non_200(self) -> None:
        async with Crawler() as crawler:
            response = await crawler.submit_request('get', self.url('/flaky'))
        self.assertEqual(response.json(), {'attempt': 2})

    async def test_html_response_is_retried_then_raised(self) -> None:
        async with Crawler() as crawler:
            with self.assertRaisesRegex(Exception, 'html response'):
                await crawler.submit_request('get', self.url('/html'), retry_on_html_response=True)
            response = await crawler.submit_request('get', self.url('/html'))
        self.assertIn('blocked', response.text)

    async def test_params_headers_and_json_body(self) -> None:
        async with Crawler() as crawler:
            got = (await crawler.submit_request('get', self.url('/echo'), params={'h': 0, 'r': 0},
                                                headers={'User-Agent': 'test'})).json()
            posted = (await crawler.submit_request('post', self.url('/echo'), data={'a': 1})).json()
        self.assertEqual(got, {'method': 'GET', 'query': {'h': '0', 'r': '0'}, 'body': None, 'agent': 'test'})
        self.assertEqual((posted['method'], posted['body']), ('POST', {'a': 1}))

    async def test_orjson_body_and_persian_response(self) -> None:
        async with Crawler() as crawler:
            posted = await crawler.submit_request('post', self.url('/echo'), data={'name': 'فملی', 'n': 1})
        self.assertEqual(posted.json()['body'], {'name': 'فملی', 'n': 1})
        self.assertIsInstance(posted.content, bytes)

    async def test_permanent_http_error_is_not_retried(self) -> None:
        async with Crawler() as crawler:
            with self.assertRaises(PermanentHttpError) as raised:
                await crawler.submit_request('get', self.url('/not_found'))
        self.assertEqual((raised.exception.status_code, self.calls['not_found']), (404, 1))

    async def test_rate_limit_is_retried(self) -> None:
        async with Crawler() as crawler:
            response = await crawler.submit_request('get', self.url('/rate_limited'))
        self.assertEqual(response.json(), {'attempt': 2})

    async def test_transient_error_stops_after_six_attempts(self) -> None:
        async with Crawler() as crawler:
            with self.assertRaises(TransientHttpError):
                await crawler.submit_request('get', self.url('/unavailable'))
        self.assertEqual(self.calls['unavailable'], 6)

    async def test_submit_request_can_lower_attempts(self) -> None:
        async with Crawler() as crawler:
            with self.assertRaises(TransientHttpError):
                await crawler.submit_request('get', self.url('/unavailable'), attempts=2)
        self.assertEqual(self.calls['unavailable'], 2)

    async def test_get_json_tolerates_bom_and_double_encoding(self) -> None:
        async with Crawler() as crawler:
            self.assertEqual(await crawler.get_json(self.url('/bom')), {'a': 1})
            self.assertEqual(await crawler.get_json(self.url('/twice')), {'nav': 5})
            with self.assertRaises(PermanentHttpError):
                await crawler.get_json(self.url('/not_found'))

    async def test_session_is_closed_on_exit(self) -> None:
        async with Crawler() as crawler:
            session = crawler.session
        self.assertTrue(session.closed)
        with self.assertRaises(RuntimeError):
            await crawler.submit_request('get', self.url('/slow'))


class CrawlerWithMockedRequestTests(TestCase):
    """Crawler methods with `submit_request` mocked, so no network is used."""

    async def test_update_share_history_item(self) -> None:
        share = await Share.objects.acreate(id=1, ticker='فملی', description='ملی صنایع مس ایران')
        rows = [{'dEven': 20261115, 'priceMax': 110, 'priceMin': 90, 'pClosing': 100, 'priceYesterday': 95,
                 'priceChange': 5, 'priceFirst': 96, 'qTotCap': 1000, 'qTotTran5J': 10, 'zTotTran': 3},
                {'dEven': 20261112, 'priceMax': 1, 'priceMin': 1, 'pClosing': 1, 'priceYesterday': 1,
                 'priceChange': 0, 'priceFirst': 1, 'qTotCap': 0, 'qTotTran5J': 0, 'zTotTran': 0}]

        async with Crawler() as crawler:
            with patch.object(crawler, 'submit_request',
                              AsyncMock(return_value=json_response({'closingPriceDaily': rows}))) as mock:
                await crawler.update_share_history_item(share, days=3)

        assert mock.await_args is not None
        self.assertIn('/GetClosingPriceDailyList/1/3', mock.await_args.kwargs['url'])
        histories = [h async for h in DailyHistory.objects.filter(share=share)]
        self.assertEqual([(h.date, h.last, h.close) for h in histories], [(date(2026, 11, 15), 100, 100)])
        await share.arefresh_from_db()
        self.assertIsNotNone(share.last_update)

    async def test_search_share_creates_and_updates(self) -> None:
        await Share.objects.acreate(id=2, ticker='old', description='old')
        rows = [{'insCode': '2', 'lVal18AFC': 'وبملت', 'lVal30': 'بانک ملت', 'flow': 1, 'lastDate': 1},
                {'insCode': '3', 'lVal18AFC': 'خودرو', 'lVal30': 'ایران خودرو', 'flow': 1, 'lastDate': 1}]

        async with Crawler() as crawler:
            with patch.object(crawler, 'submit_request',
                              AsyncMock(return_value=json_response({'instrumentSearch': rows}))):
                await crawler.search_share('test')

        tickers = {s.id: s.ticker async for s in Share.objects.all()}
        self.assertEqual(tickers, {2: 'وبملت', 3: 'خودرو'})

    async def test_update_share_list_from_new_market_watch(self) -> None:
        await ShareGroup.objects.acreate(id=27, name='فلزات اساسی')
        await ShareGroup.objects.acreate(id=68, name='صندوق سرمایه گذاری قابل معامله')
        await Share.objects.acreate(id=35425587644337450, ticker='قدیمی', description='old', enable=False)
        # Rows from a real GetMarketWatch response (2026-11-08), trimmed to the fields the crawler reads.
        rows: list[dict[str, Any]] = [
            {'insCode': '35425587644337450', 'lva': 'فملي', 'lvc': 'ملي\u200c صنايع\u200c مس\u200c ايران\u200c',
             'eps': 2073.0, 'bv': 1.0, 'flow': 1, 'csv': '27 ', 'ztd': 1440000000000.0, 'yVal': 300},
            {'insCode': '71806970632897', 'lva': 'سهامدار2', 'lvc': 'صندوق س. بازده سهام-س',
             'eps': 0.0, 'bv': 1.0, 'flow': 2, 'csv': '68 ', 'ztd': 200000000.0, 'yVal': 305},
            {'insCode': '1', 'lva': 'بی‌گروه', 'lvc': 'unknown sector', 'eps': 0.0, 'bv': 1.0, 'flow': 1,
             'csv': '99 ', 'ztd': 1.0, 'yVal': 300},
        ]

        async with Crawler() as crawler:
            with patch.object(crawler, 'submit_request',
                              AsyncMock(return_value=json_response({'marketwatch': rows}))) as mock:
                await crawler.update_share_list()

        assert mock.await_args is not None
        self.assertEqual(mock.await_args.kwargs['url'], 'https://cdn.tsetmc.com/api/ClosingPrice/GetMarketWatch')
        self.assertNotIn('old.tsetmc', mock.await_args.kwargs['url'])
        self.assertEqual(mock.await_args.kwargs['params']['withBestLimits'], 'false')

        fameli = await Share.objects.aget(id=35425587644337450)
        self.assertEqual((fameli.ticker, fameli.eps, fameli.group_id, fameli.total_count, fameli.bazaar_type,
                          fameli.bazaar_group, fameli.enable), ('فملی', 2073, 27, 1440000000000, 1, 300, True))
        etf = await Share.objects.aget(id=71806970632897)
        self.assertEqual((etf.ticker, etf.eps, etf.group_id), ('سهامدار2', None, 68))
        self.assertFalse(await Share.objects.filter(id=1).aexists())  # unknown sector is skipped, as before

    async def test_run_jobs_counts_failures(self) -> None:
        async def ok() -> None:
            await asyncio.sleep(0)

        async def fail() -> None:
            raise ValueError('boom')

        async with Crawler() as crawler:
            with self.assertLogs('crawler.helper', level='WARNING'):
                result = await crawler.run_jobs('test', [ok, fail, ok], log_exception_on_failure=False)
        self.assertEqual(result, (2, 1))


class CommandTests(TransactionTestCase):
    """Management commands run the crawler with asyncio.run; `Crawler.submit_request` is mocked."""

    def test_update_share_history_command(self) -> None:
        Share.objects.create(id=1, ticker='فملی', description='ملی صنایع مس ایران')
        row = {'dEven': 20261115, 'priceMax': 110, 'priceMin': 90, 'pClosing': 100, 'priceYesterday': 95,
               'priceChange': 5, 'priceFirst': 96, 'qTotCap': 1000, 'qTotTran5J': 10, 'zTotTran': 3}

        with patch.object(Crawler, 'submit_request',
                          AsyncMock(return_value=json_response({'closingPriceDaily': [row]}))):
            call_command('update_share_history', concurrency=5)

        self.assertEqual(DailyHistory.objects.filter(share_id=1).count(), 1)
        self.assertIsNotNone(Share.objects.get(id=1).last_update)

    def test_update_share_info_command(self) -> None:
        Share.objects.create(id=1, ticker='فملی', description='ملی صنایع مس ایران')
        ShareGroup.objects.create(id=27, name='فلزات اساسی')
        identity = {'sector': {'cSecVal': '27 '}, 'instrumentID': 'IRO1MSMI0000'}
        contracts = [{'ContractID': 41, 'ContractCode': 'GB', 'ContractDescription': 'gold bar', 'ContractSize': 1,
                      'CommodityID': 1, 'CommodityName': 'gold'}]

        async def fake_submit_request(self: Crawler, method: str, url: str, **kwargs: Any) -> Response:
            if 'GetInstrumentIdentity' in url:
                return json_response({'instrumentIdentity': identity})
            return json_response(contracts)

        with patch.object(Crawler, 'submit_request', fake_submit_request):
            call_command('update_share_info')

        share = Share.objects.get(id=1)
        self.assertEqual((share.group_id, share.isin), (27, 'IRO1MSMI0000'))
        self.assertTrue(Contract.objects.filter(id=41).exists())


class RetryPolicyTests(IsolatedAsyncioTestCase):
    def test_parse_retry_after(self) -> None:
        now = datetime(2026, 11, 15, 8, 0, tzinfo=dt_timezone.utc)
        self.assertEqual(parse_retry_after('120'), 120)
        self.assertEqual(parse_retry_after('Sun, 15 Nov 2026 08:00:30 GMT', now=now), 30)
        self.assertEqual(parse_retry_after('Sun, 15 Nov 2026 07:59:00 GMT', now=now), 0)
        self.assertIsNone(parse_retry_after(None))
        self.assertIsNone(parse_retry_after('soon'))

    def _wait(self, exc: BaseException) -> float:
        state = SimpleNamespace(attempt_number=1, outcome=SimpleNamespace(exception=lambda: exc))
        return wait_backoff_or_retry_after(state)  # type: ignore[arg-type]

    def test_wait_respects_retry_after_with_a_cap(self) -> None:
        self.assertGreaterEqual(self._wait(TransientHttpError('429', 429, retry_after=40)), 40)
        self.assertLessEqual(self._wait(TransientHttpError('429', 429, retry_after=10_000)), 300)
        self.assertLessEqual(self._wait(TransientHttpError('503', 503)), 2)

    async def test_closed_session_is_not_retried(self) -> None:
        crawler = Crawler()
        with patch.object(Crawler._submit_request.retry, 'wait', wait_none()), \
                patch.object(crawler, '_host_semaphore', MagicMock(return_value=None)) as attempt:
            with self.assertRaises(RuntimeError):
                await crawler.submit_request('get', 'https://cdn.tsetmc.com/api/test')
        self.assertEqual(attempt.call_count, 1)


def history_row(d_even: int, close: int, count: int = 3) -> dict[str, Any]:
    return {'dEven': d_even, 'priceMax': close + 5, 'priceMin': close - 5, 'pClosing': close, 'priceYesterday': close,
            'priceChange': 0, 'priceFirst': close, 'qTotCap': 1000, 'qTotTran5J': 10, 'zTotTran': count}


class HistoryStorageTests(TestCase):
    def setUp(self) -> None:
        self.share = Share.objects.create(id=1, ticker='فملی', description='ملی صنایع مس ایران')

    def stored_dates(self) -> list[date]:
        return list(DailyHistory.objects.filter(share=self.share).order_by('date').values_list('date', flat=True))

    def test_fills_gaps_around_existing_dates_in_any_order(self) -> None:
        CrawlerStorage.save_share_history(self.share, [history_row(20261110, 100)], timezone.now(), 100)
        rows = [history_row(20261108, 90), history_row(20261112, 120), history_row(20261110, 999),
                history_row(20261111, 0, count=0), history_row(20261109, 95), history_row(20261112, 120)]

        CrawlerStorage.save_share_history(self.share, rows, timezone.now(), 100)

        self.assertEqual(self.stored_dates(), [date(2026, 11, 8), date(2026, 11, 9), date(2026, 11, 10),
                                               date(2026, 11, 12)])
        self.assertEqual(DailyHistory.objects.get(share=self.share, date=date(2026, 11, 10)).close, 100)

    def test_existing_dates_cost_one_query(self) -> None:
        rows = [history_row(20200101 + i, 100) for i in range(28)]
        with self.assertNumQueries(5):  # savepoint, select existing dates, insert, update checkpoint, release
            CrawlerStorage.save_share_history(self.share, rows, timezone.now(), 100)

    def test_failed_insert_does_not_advance_checkpoint(self) -> None:
        old = timezone.now() - timezone.timedelta(days=10)
        Share.objects.filter(id=1).update(last_update=old)
        self.share.refresh_from_db()

        with patch.object(DailyHistory.objects, 'bulk_create', side_effect=IntegrityError('disk full')):
            with self.assertRaises(IntegrityError):
                CrawlerStorage.save_share_history(self.share, [history_row(20261112, 120)], timezone.now(), 100)

        self.share.refresh_from_db()
        self.assertEqual(self.share.last_update, old)
        self.assertEqual(self.stored_dates(), [])

    def test_contract_history_fills_gaps(self) -> None:
        contract = Contract.objects.create(id=41, code='GB', description='gold bar', size=1, commodity_id=1,
                                           commodity_name='gold')

        def row(day: str, price: int) -> dict[str, Any]:
            return {'DT': day, 'MaxPrice': price, 'MinPrice': price, 'TodaySettlementPrice': price, 'LastPrice': price,
                    'FirstPrice': price, 'LastSettlementPrice': price, 'TradesValue': 1, 'TradesVolume': 1,
                    'C_Buy': 1, 'C_Sell': 1}

        CrawlerStorage.save_contract_history(contract, [row('2026-11-10T00:00:00', 100)], 100)
        CrawlerStorage.save_contract_history(contract, [row('2026-11-12T00:00:00', 120), row('2026-11-10T00:00:00', 1),
                                                        row('2026-11-08T00:00:00', 90), row('2026-11-09T00:00:00', 0)],
                                             100)

        dates = list(DailyHistory.objects.filter(contract=contract).order_by('date').values_list('date', flat=True))
        self.assertEqual(dates, [date(2026, 11, 8), date(2026, 11, 10), date(2026, 11, 12)])


class HistoryCheckpointTests(TestCase):
    async def test_failed_request_does_not_advance_checkpoint(self) -> None:
        share = await Share.objects.acreate(id=1, ticker='فملی', description='ملی صنایع مس ایران')
        async with Crawler() as crawler:
            with patch.object(crawler, 'submit_request', AsyncMock(side_effect=PermanentHttpError('404', 404))):
                with self.assertRaises(PermanentHttpError):
                    await crawler.update_share_history_item(share, days=3)
        await share.arefresh_from_db()
        self.assertIsNone(share.last_update)


class BulkSaveTests(TestCase):
    """Saving a list costs a fixed number of queries, however many rows it has."""

    def test_share_groups_insert_and_rename(self) -> None:
        ShareGroup.objects.create(id=27, name='old name')
        rows = [{'type': 'IndustrialGroup', 'code': 27, 'name': 'فلزات اساسي'},
                {'type': 'IndustrialGroup', 'code': 68, 'name': 'صندوق سرمايه گذاري'},
                {'type': 'Sector', 'code': 1, 'name': 'not a group'}]
        with self.assertNumQueries(5):  # select, savepoint, insert, update, release
            CrawlerStorage.save_share_groups(rows)
        self.assertEqual(dict(ShareGroup.objects.values_list('id', 'name')),
                         {27: 'فلزات اساسی', 68: 'صندوق سرمایه گذاری'})

    def test_search_result_queries_do_not_grow_with_rows(self) -> None:
        Share.objects.create(id=1, ticker='old', description='old', bazaar_type=1, enable=True)

        def rows(n: int) -> list[dict[str, Any]]:
            return [{'insCode': str(i), 'lVal18AFC': f'نماد{i}', 'lVal30': f'شرکت {i}', 'flow': 1, 'lastDate': 1}
                    for i in range(1, n + 1)]

        with CaptureQueriesContext(connection) as small:
            CrawlerStorage.save_search_result('a', rows(2))
        with CaptureQueriesContext(connection) as large:
            CrawlerStorage.save_search_result('b', rows(40) + rows(40))  # duplicates are ignored
        self.assertLessEqual(len(large), len(small))  # 80 rows cost no more queries than 2
        self.assertEqual(Share.objects.count(), 40)
        self.assertEqual(Share.objects.get(id=1).ticker, 'نماد1')

    def test_contract_list_inserts_updates_and_skips_unchanged(self) -> None:
        Contract.objects.create(id=41, code='GB', description='gold bar', size=1, commodity_id=1, commodity_name='gold')
        Contract.objects.create(id=42, code='GC', description='gold coin', size=1, commodity_id=2,
                                commodity_name='coin')

        def row(contract_id: int, code: str, description: str, commodity_id: int, name: str) -> dict[str, Any]:
            return {'ContractID': contract_id, 'ContractCode': code, 'ContractDescription': description,
                    'ContractSize': 1, 'CommodityID': commodity_id, 'CommodityName': name}

        rows = [row(41, 'GB', 'gold bar', 1, 'gold'), row(42, 'GC2', 'gold coin', 2, 'coin'),
                row(43, 'SB', 'silver bar', 3, 'silver')]
        with self.assertLogs('crawler.helper', level='INFO') as logs, self.assertNumQueries(5):
            CrawlerStorage.save_contract_list(rows, 100)
        self.assertIn('1 ([<Contract: silver>]) added, 1 updated', logs.output[-1])
        self.assertEqual(Contract.objects.get(id=42).code, 'GC2')
        self.assertTrue(Contract.objects.filter(id=43).exists())


class SettingsTests(TestCase):
    def test_request_headers_use_the_new_site(self) -> None:
        headers = Crawler.get_tse_new_site_headers()
        self.assertEqual((headers['Origin'], headers['Referer']), ('https://www.tsetmc.com', 'https://www.tsetmc.com/'))

    def test_sqlite_takes_the_write_lock_when_a_transaction_starts(self) -> None:
        self.assertEqual(connection.transaction_mode, 'IMMEDIATE')


class RunWithCrawlerTests(TestCase):
    def test_run_with_crawler_opens_and_closes_one_crawler(self) -> None:
        seen: list[Crawler] = []

        async def job(crawler: Crawler) -> int:
            seen.append(crawler)
            self.assertFalse(crawler.session.closed)
            return 7

        self.assertEqual(run_with_crawler(job), 7)
        self.assertIsNone(seen[0]._session)


# فملی from GetMarketWatch on 2026-11-15, checked against GetClosingPriceInfo and BestLimits.
FAMELI: dict[str, Any] = {
    'lva': 'فملي', 'lvc': 'ملي\u200c صنايع\u200c مس\u200c ايران\u200c', 'eps': 2073, 'pmd': 27850, 'pmo': 27860,
    'qtj': 646367280, 'pdv': 27850, 'ztt': 58743, 'qtc': 17767582673760, 'bv': 1, 'pmn': 26860, 'pmx': 28370,
    'py': 27690, 'pf': 26860, 'pcl': 27490, 'yVal': 300, 'flow': 1, 'csv': '27 ', 'insID': 'IRO1MSMI0001',
    'pMax': 28520, 'pMin': 26860, 'ztd': 1440000000000, 'pRedTran': 0, 'insCode': '35425587644337450',
    'hEven': 123001,
    'blDs': [
        {'n': 1, 'qmd': 30575, 'zmd': 3, 'pmd': 27850, 'pmo': 27860, 'zmo': 9, 'qmo': 193644, 'rid': 16458105089},
        {'n': 2, 'qmd': 10000, 'zmd': 1, 'pmd': 27830, 'pmo': 27870, 'zmo': 2, 'qmo': 6485, 'rid': 16457687092},
        {'n': 3, 'qmd': 10000, 'zmd': 1, 'pmd': 27820, 'pmo': 27880, 'zmo': 9, 'qmo': 143079, 'rid': 16457984852},
        {'n': 4, 'qmd': 5358, 'zmd': 2, 'pmd': 27810, 'pmo': 27890, 'zmo': 11, 'qmo': 61012, 'rid': 16458030742},
        {'n': 5, 'qmd': 207600, 'zmd': 5, 'pmd': 27800, 'pmo': 27900, 'zmo': 30, 'qmo': 520966, 'rid': 16458076989},
    ],
}
ETF_ROW: dict[str, Any] = {**FAMELI, 'insCode': '1', 'lva': 'كاريس', 'csv': '68 ', 'pRedTran': 94487, 'hEven': 122955,
                           'blDs': [{**FAMELI['blDs'][0], 'pmd': 94000, 'pmo': 94500, 'rid': 16458000000}]}
FAMELI_ID = 35425587644337450


class FakeCrawler:
    """Answers the two calls `TSEMarketWatch.refresh` makes, from a queue of market watch responses."""

    def __init__(self, *market_watch: list[dict[str, Any]] | Exception, day: int = 20261115) -> None:
        self.responses = list(market_watch)
        self.day = day
        self.cursors: list[tuple[int, int]] = []
        self.overview_calls = 0

    async def get_market_overview(self, market: int, **kwargs: Any) -> Any:
        self.overview_calls += 1
        return {'marketActivityDEven': self.day, 'marketActivityHEven': 142154}

    async def get_market_watch(self, h_even: int, ref_id: int, **kwargs: Any) -> list[dict[str, Any]]:
        self.cursors.append((h_even, ref_id))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class MarketWatchTests(IsolatedAsyncioTestCase):
    def test_quote_uses_the_old_column_names(self) -> None:
        quote = tse_quote(FAMELI)
        keys = ('ticker', 'IR', 'time', 'first', 'close', 'last', 'open', 'low', 'high', 'count', 'volume', 'value',
                'group', 'eps', 'nav')
        self.assertEqual({key: quote[key] for key in keys},
                         {'ticker': 'فملی', 'IR': 'IRO1MSMI0001', 'time': 123001, 'first': 26860, 'close': 27490,
                          'last': 27850, 'open': 27690, 'low': 26860, 'high': 28370, 'count': 58743,
                          'volume': 646367280, 'value': 17767582673760, 'group': 27, 'eps': 2073, 'nav': 0})

    async def test_full_then_incremental_merge(self) -> None:
        changed_level = {**FAMELI['blDs'][1], 'qmd': 99, 'rid': 16458500000}
        crawler = FakeCrawler([FAMELI, ETF_ROW], [{**FAMELI, 'pdv': 27900, 'hEven': 123500, 'blDs': [changed_level]}])
        market_watch = TSEMarketWatch()
        await market_watch.refresh(crawler)  # type: ignore[arg-type]
        await market_watch.refresh(crawler)  # type: ignore[arg-type]

        self.assertEqual(crawler.cursors, [(0, 0), (123001, 16458105089)])
        self.assertEqual(crawler.overview_calls, 2)  # both markets, on the full refresh only
        self.assertEqual(market_watch.live_data()[FAMELI_ID]['last'], 27900)
        book = market_watch.order_book(FAMELI_ID)
        self.assertEqual([level['order'] for level in book], [1, 2, 3, 4, 5])
        self.assertEqual((book[1]['buy_volume'], book[0]['buy_volume']), (99, 30575))  # only level 2 changed
        self.assertEqual((market_watch.h_even, market_watch.ref_id), (123500, 16458500000))
        self.assertEqual(market_watch.nav(1), 94487)
        assert market_watch.activity_time is not None
        self.assertEqual(timezone.localtime(market_watch.activity_time),
                         timezone.make_aware(datetime(2026, 11, 15, 14, 21, 54)))

    async def test_full_download_after_failed_increment_and_after_the_interval(self) -> None:
        market_watch = TSEMarketWatch(full_refresh_seconds=3600)
        crawler = FakeCrawler([FAMELI], TransientHttpError('503', 503), [ETF_ROW], [ETF_ROW], [FAMELI])
        await market_watch.refresh(crawler)  # type: ignore[arg-type]  # full
        await market_watch.refresh(crawler)  # type: ignore[arg-type]  # incremental fails -> full
        self.assertEqual(set(market_watch.rows), {1})  # a full download replaces the state
        await market_watch.refresh(crawler)  # type: ignore[arg-type]  # incremental: one request
        self.assertEqual(crawler.overview_calls, 4)
        market_watch._last_full_refresh -= 3600
        await market_watch.refresh(crawler)  # type: ignore[arg-type]  # interval passed -> full
        self.assertEqual([cursor[0] for cursor in crawler.cursors], [0, 123001, 0, 122955, 0])
        self.assertEqual(crawler.overview_calls, 6)
        self.assertEqual(set(market_watch.rows), {FAMELI_ID})

    async def test_failed_market_overview_keeps_the_last_activity_time(self) -> None:
        crawler = FakeCrawler([FAMELI], [FAMELI])
        market_watch = TSEMarketWatch(full_refresh_seconds=0)  # every refresh is full
        await market_watch.refresh(crawler)  # type: ignore[arg-type]
        first = market_watch.activity_time
        with patch.object(crawler, 'get_market_overview', AsyncMock(side_effect=TransientHttpError('503', 503))):
            await market_watch.refresh(crawler)  # type: ignore[arg-type]
        self.assertEqual(crawler.cursors, [(0, 0), (0, 0)])
        self.assertIsNotNone(first)
        self.assertEqual(market_watch.activity_time, first)

    def test_order_book_with_nav(self) -> None:
        market_watch = TSEMarketWatch()
        market_watch.merge([ETF_ROW])
        level = market_watch.order_book(1, nav=94487)[0]
        self.assertEqual((level['order'], level['buy_price'], level['sell_volume']), (1, 94000, 193644))
        self.assertAlmostEqual(level['buy_price_normalized'], 94000 / 94487)
        self.assertNotIn('buy_price_normalized', market_watch.order_book(1)[0])
        self.assertEqual(market_watch.order_book(999), [])


class DataGetterTests(IsolatedAsyncioTestCase):
    async def test_best_limits_history_is_sorted_and_etf_may_be_missing(self) -> None:
        rows = [{'hEven': 90010, 'refID': 3}, {'hEven': 90000, 'refID': 2}, {'hEven': 90000, 'refID': 1}]
        crawler = Crawler()
        with patch.object(crawler, 'tse_json', AsyncMock(return_value={'bestLimitsHistory': rows})) as mock:
            history = await crawler.get_best_limits_history(7, date(2026, 11, 15))
        self.assertEqual([row['refID'] for row in history], [1, 2, 3])
        self.assertEqual(mock.await_args.args[0], '/BestLimits/7/20261115')  # type: ignore[union-attr]

        with patch.object(crawler, 'tse_json', AsyncMock(return_value={})):
            self.assertIsNone(await crawler.get_etf(7))

    async def test_ime_paths(self) -> None:
        crawler = Crawler()
        with patch.object(crawler, 'get_json', AsyncMock(return_value={'Data': [{'DT': 1}]})) as mock:
            rows = await crawler.get_cdc_trades(5, date(2026, 11, 1), date(2026, 11, 15))
        self.assertEqual(rows, [{'DT': 1}])
        assert mock.await_args is not None
        self.assertEqual(mock.await_args.args[0], 'https://dataapi.ime.co.ir/api/CDC/CDCTrades')
        self.assertEqual((mock.await_args.kwargs['method'], mock.await_args.kwargs['data']['customFilter'],
                          mock.await_args.kwargs['data']['fromDate']), ('post', '5', '2026-11-01'))


def cdc_row(contract_id: int, last: int, settle_date: str = '', settlement: int = 0, **extra: Any) -> dict[str, Any]:
    return {'ContractID': contract_id, 'ContractCode': f'C{contract_id}', 'ContractDescription': 'gold coin',
            'LastUpdate': '2026-11-15T11:00:00', 'LastSettlementPricePersianDate': settle_date,
            'LastSettlementPrice': settlement, 'FirstTradedPrice': last, 'LastTradedPrice': last, 'TradesCount': 3,
            'TradesVolume': 10, 'TradesValue': last * 10, 'LowTradedPrice': last, 'HighTradedPrice': last, **extra}


class IMEMarketWatchTests(IsolatedAsyncioTestCase):
    def test_quote_takes_open_and_close_from_the_settlement_date(self) -> None:
        today = date(2026, 11, 15)  # 1405/08/24
        yesterday = ime_quote(cdc_row(1, 500, '1405/08/23', 480), today)
        self.assertEqual((yesterday['open'], yesterday['close'], yesterday['last']), (480, 0, 500))
        settled = ime_quote(cdc_row(1, 500, '1405/08/24', 490), today)
        self.assertEqual((settled['open'], settled['close']), (0, 490))
        self.assertEqual((settled['id'], settled['code'], settled['count'], settled['value']), (1, 'C1', 3, 5000))
        self.assertEqual(timezone.localtime(settled['datetime']).hour, 11)

    async def test_refresh_replaces_rows_and_counts_changes(self) -> None:
        responses = [[cdc_row(1, 500), cdc_row(2, 700)],
                     [cdc_row(1, 500), cdc_row(2, 710, LastUpdate='2026-11-15T11:05:00')]]
        crawler = Crawler()
        market_watch = IMEMarketWatch()
        with patch.object(crawler, 'get_cdc_live_market', AsyncMock(side_effect=responses)) as mock:
            self.assertEqual(await market_watch.refresh(crawler), 2)
            self.assertEqual(await market_watch.refresh(crawler), 1)
        self.assertEqual(mock.await_count, 2)
        self.assertEqual(market_watch.live_data(date(2026, 11, 15))[2]['last'], 710)
        assert market_watch.last_update is not None
        self.assertEqual(timezone.localtime(market_watch.last_update).minute, 5)

    def test_unreadable_rows_are_skipped(self) -> None:
        market_watch = IMEMarketWatch()
        with self.assertLogs('crawler.helper', 'WARNING'):
            market_watch.merge([{'ContractCode': 'no id'}, cdc_row(3, 100), {'ContractID': 4}])
        with self.assertLogs('crawler.helper', 'WARNING'):
            quotes = market_watch.live_data(date(2026, 11, 15))
        self.assertEqual(list(quotes), [3])


class LogTimeTests(IsolatedAsyncioTestCase):
    async def test_sync_and_async_timings(self) -> None:
        @log_time
        def add(a: int, b: int) -> int:
            return a + b

        @log_time
        async def double(a: int) -> int:
            return a * 2

        with self.assertLogs('crawler.decorators', 'INFO') as logs, \
                patch('crawler.decorators.time.perf_counter', side_effect=[10.0, 11.5, 20.0, 20.25]):
            self.assertEqual(add(1, 2), 3)
            self.assertEqual(await double(4), 8)
        self.assertEqual([record.getMessage() for record in logs.records],
                         ['add runs in 1.50s', 'double runs in 0.25s'])
        self.assertEqual((add.__name__, double.__name__), ('add', 'double'))
