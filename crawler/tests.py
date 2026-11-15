import asyncio
from typing import Any
from datetime import date
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock, patch

import orjson
from aiohttp import web
from aiohttp.test_utils import TestServer
from django.core.management import call_command
from django.test import TestCase, TransactionTestCase
from tenacity import wait_none

from crawler.helper import Crawler, Response
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

        app = web.Application()
        app.router.add_get('/slow', slow)
        app.router.add_get('/flaky', flaky)
        app.router.add_get('/html', html)
        app.router.add_route('*', '/echo', echo)
        self.server = TestServer(app)
        await self.server.start_server()
        self.no_wait = patch.object(Crawler.submit_request.retry, 'wait', wait_none())
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
            got = (await crawler.submit_request('get', self.url('/echo'), params=(('h', 0), ('r', 0)),
                                                headers={'User-Agent': 'test'})).json()
            posted = (await crawler.submit_request('post', self.url('/echo'), data={'a': 1})).json()
        self.assertEqual(got, {'method': 'GET', 'query': {'h': '0', 'r': '0'}, 'body': None, 'agent': 'test'})
        self.assertEqual((posted['method'], posted['body']), ('POST', {'a': 1}))

    async def test_orjson_body_and_persian_response(self) -> None:
        async with Crawler() as crawler:
            posted = await crawler.submit_request('post', self.url('/echo'), data={'name': 'فملی', 'n': 1})
        self.assertEqual(posted.json()['body'], {'name': 'فملی', 'n': 1})
        self.assertIsInstance(posted.content, bytes)

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
        self.assertIn(('withBestLimits', 'false'), mock.await_args.kwargs['params'])

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
