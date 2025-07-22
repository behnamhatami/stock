import concurrent
import logging
import math
import typing
from datetime import date
from io import StringIO

import numpy as np
import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup
from django.utils import timezone
from getuseragent import UserAgent
from persiantools import characters
from tenacity import _utils
from tenacity import stop_after_attempt, wait_random_exponential, retry, RetryCallState

from crawler.decorators import log_time
from crawler.models import Share, ShareDailyHistory, ShareGroup
from crawler.time_helper import convert_integer_to_parts

logger = logging.getLogger(__name__)
user_agent = UserAgent()

urllib3.disable_warnings()


def get_headers(share, referer=None):
    return {
        'User-Agent': user_agent.Random(),
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Referer': referer if referer else f'http://old.tsetmc.com/Loader.aspx?ParTree=151311&i={share.id}',
        'X-Requested-With': 'XMLHttpRequest',
    }


def get_tse_new_site_headers():
    return {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Origin': 'http://tsetmc.com',
        'DNT': '1',
        'Referer': 'http://tsetmc.com/',
        'User-Agent': user_agent.Random(),
    }


def after_log(
        logger: "logging.Logger",
        log_level: int,
        sec_format: str = "%0.3f",
) -> typing.Callable[[RetryCallState], None]:
    """After call strategy that logs to some logger the finished attempt."""

    def log_it(retry_state: RetryCallState) -> None:
        if retry_state.fn is None:
            # NOTE(sileht): can't really happen, but we must please mypy
            fn_name = "<unknown>"
            args, kwargs = [], {}
        else:
            fn_name = _utils.get_callback_name(retry_state.fn)
            args, kwargs = list(retry_state.args), dict(retry_state.kwargs)
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


@retry(reraise=True, stop=stop_after_attempt(6), wait=wait_random_exponential(multiplier=1, max=60),
       after=after_log(logger, logging.DEBUG))
def submit_request(url, params, headers, retry_on_empty_response=False, retry_on_html_response=False, timeout=5):
    response = requests.get(url, params=params, headers=headers, timeout=timeout, verify=False)

    if response.status_code != 200:
        raise Exception(f"Http Error: {response.status_code}, {url.split('/')[-1]}, {params}")

    if retry_on_empty_response and len(response.text.strip()) == 0:
        raise Exception(f"Http Error: empty response, {url.split('/')[-1]}, {params}")

    if retry_on_html_response and ('<html>' in response.text or '<!doctype html>' in response.text):
        raise Exception(f"Http Error: html response, {url.split('/')[-1]}, {params}")

    return response


@log_time
def run_jobs(job_title, jobs, max_workers=10, log=True, log_exception_on_failure=True):
    number_of_buckets = max(min(20, len(jobs) // 10), 2)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(job) for job in jobs]

        success, error = 0, 0
        for index, future in enumerate(concurrent.futures.as_completed(futures)):
            if future.exception():
                error += 1
                if log:
                    if log_exception_on_failure:
                        logger.exception(f'{job_title}: job crashed!', exc_info=future.exception())
                    else:
                        logger.warning(f'{job_title}: job crashed!', exc_info=future.exception())
            else:
                success += 1

            if log:
                if int((index + 1) * number_of_buckets / len(futures)) - int(index * number_of_buckets / len(futures)):
                    percent = round((index + 1) / len(futures) * 100, 2)
                    logger.info(f"{job_title}: {success}/{index + 1} out of {len(futures)}({percent}%)")

        logger.info(f"Task {job_title}: {success} tasks completed out of {len(futures)}")


@log_time
def update_share_list_by_group(group):
    params = (
        ('g', group.id),
        ('t', 'g'),
        ('s', '0'),
    )
    response = requests.get('http://old.tsetmc.com/tsev2/data/InstValue.aspx', params=params, timeout=10)

    if response.status_code != 200:
        raise Exception(f"Http Error: {response.status_code}")

    return response.text


@log_time
def update_share_groups():
    response = requests.get('https://cdn.tsetmc.com/api/StaticData/GetStaticData', headers=get_tse_new_site_headers())
    response.raise_for_status()

    for item in response.json()['staticData']:
        if item['type'] == 'IndustrialGroup':
            code: int = item['code']
            name: str = characters.ar_to_fa(item['name']).strip()

            try:
                group = ShareGroup.objects.get(id=code)
            except ShareGroup.DoesNotExist:
                group = ShareGroup()

            group.id = int(code)
            group.name = name
            group.save()

    logger.info(f"Share group info updated. number of groups: {ShareGroup.objects.count()}")


def search_share(keyword):
    response = submit_request(f'https://cdn.tsetmc.com/api/Instrument/GetInstrumentSearch/{keyword}',
                              params=(), headers=get_tse_new_site_headers(), retry_on_html_response=True, timeout=25)

    new_list, update_list = [], []
    for row in response.json()['instrumentSearch']:
        data = {
            'id': int(row['insCode']),
            'ticker': characters.ar_to_fa(str(row['lVal18AFC'])).strip(),
            'description': characters.ar_to_fa(row['lVal30']).strip(),
            'bazaar_type': row['flow'],
            'enable': row['lastDate'],
        }
        try:
            share = Share.objects.get(id=data['id'])
            updated: bool = False
            for key, value in data.items():
                if getattr(share, key) != value:
                    setattr(share, key, value)
                    updated: bool = True

            parsed_data = share.parse_data()
            if (share.strike_date, share.option_strike_price, share.base_share) != parsed_data:
                share.strike_date, share.option_strike_price, share.base_share = parsed_data
                updated = True

            if updated:
                update_list.append(share)
        except Share.DoesNotExist:
            for share in new_list:
                if share.id == data['id']:
                    break
            else:
                new_list.append(Share(**data))

    Share.objects.bulk_create(new_list, batch_size=100)
    Share.objects.bulk_update(update_list, ['ticker', 'description', 'bazaar_type', 'enable', 'option_strike_price',
                                            'strike_date', 'base_share'], batch_size=100)
    if new_list or update_list:
        logger.info(f"update share list {keyword}, {len(new_list)} added ({new_list}), {len(update_list)} updated.")


def update_share_history_item(share, last_update: bool = True, days=None, batch_size=100):
    if days is None:
        days = (timezone.now() - share.last_update).days + 1 if share.last_update else 0

    response = submit_request(f'https://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyList/{share.id}/{days}',
                              params=(), headers=get_tse_new_site_headers(), retry_on_html_response=True, timeout=25)

    share_histories = []
    for row in response.json()['closingPriceDaily']:
        data = {'share': share,
                'date': date(*convert_integer_to_parts(row['dEven'])),
                'high': row['priceMax'],
                'low': row['priceMin'],
                'close': row['pClosing'],
                'last': row['priceYesterday'] + row['priceChange'],
                'first': row['priceFirst'],
                'open': row['priceYesterday'],
                'value': row['qTotCap'],
                'volume': row['qTotTran5J'],
                'count': row['zTotTran']}

        if data['count'] == 0:
            continue

        if ShareDailyHistory.objects.filter(share=share, date=data['date']).exists():
            break

        share_histories.append(ShareDailyHistory(**data))

    share.last_update = timezone.now()

    ShareDailyHistory.objects.bulk_create(share_histories, batch_size=batch_size)
    if last_update:
        share.save()

    if share_histories:
        logger.info(f"history of {share.ticker} in {len(share_histories)} days added.")


@log_time
def update_share_list(batch_size=100):
    text = get_watch_list(h=0, r=0)

    df = pd.read_csv(StringIO(text.split("@")[2]), sep=',', lineterminator=';', header=None)
    df = df.replace({np.nan: None})

    new_list, update_list = [], []
    for index, row in df.iterrows():
        try:
            share = Share.objects.get(id=row[0])
        except Share.DoesNotExist:
            share = Share()

        try:
            share.enable = True
            share.ticker = characters.ar_to_fa(str(row[2])).strip()
            share.description = characters.ar_to_fa(row[3]).strip()
            share.eps = row[14] if row[14] and row[14] != 'nan' and not math.isnan(row[14]) else None
            share.base_volume = row[15]
            share.bazaar_type = row[17]
            share.group = ShareGroup.objects.get(id=row[18])
            share.total_count = row[21]
            share.bazaar_group = row[22]
            share.strike_date, share.option_strike_price, share.base_share = share.parse_data()
            (update_list if share.id else new_list).append(share)
            share.id = row[0]
        except:
            logger.warning(f'{share.ticker} parse share data failed! {row}!!')

    Share.objects.bulk_create(new_list, batch_size=batch_size)
    Share.objects.bulk_update(update_list,
                              ['enable', 'ticker', 'description', 'eps', 'base_volume', 'bazaar_type', 'group',
                               'total_count', 'bazaar_group', 'option_strike_price', 'strike_date', 'base_share'],
                              batch_size=100)
    logger.info(f"update share list, {len(new_list)} ({new_list}) added, {len(update_list)} updated.")


def get_watch_list(h='0', r='0'):
    response = submit_request('http://old.tsetmc.com/tsev2/data/MarketWatchInit.aspx',
                              headers=get_headers(None, 'http://old.tsetmc.com/Loader.aspx?ParTree=15131F'),
                              params=(('h', h), ('r', r)), retry_on_empty_response=True,
                              retry_on_html_response=True, timeout=10)

    '''
        separated with @ text
        part 0: 
        part 1: general info of bazaar  ['date and time of last_transaction', 'boorse_status', 'boorse_index',
        'boorse_index_diff', 'boorse_market cap', 'boorse_volume', 'boorse_value', 'boorse_count', 'faraboorse_status',
        'faraboorse_volume', 'faraboorse_value', 'faraboorse_count', 'moshtaghe_status', 'moshtaghe_volume',
        'moshtaghe_value', 'moshtaghe_count']
        part 2: ['id', 'IR', 'ticker', 'description', '?', 'first', 'close', 'last', 'count', 'volume', 'value', 
        'low', 'high', 'open', 'eps', 'base volume', '', 'bazaar type', 'group', 'max_price_possible', 
        'min_price_possible', 'number of share', 'bazaar group']
        part 3; ['id', 'order', 'sell_count', 'buy_count', 'buy_price', 'sell_price', 'buy_volume', 'sell_volume']
        part 4: last transaction id
    '''

    return response.text


def get_share_detailed_info(share):
    response = requests.get('http://old.tsetmc.com/Loader.aspx', headers=get_headers(share),
                            params=(('Partree', '15131M'), ('i', share.id),), timeout=10)

    data = {}
    for row in BeautifulSoup(response.text, features='html.parser').body.select('tr'):
        key = row.select('td')[0].contents[0].strip()

        value_contents = row.select('td')[1].contents
        value = value_contents[0].strip() if value_contents else None

        data[key] = value

    if not data or 'کد گروه صنعت' not in data or 'کد 12 رقمی نماد' not in data:
        return

    try:
        share.extra_data = data
        share.group = ShareGroup.objects.get(id=data['کد گروه صنعت'])
        share.isin = data['کد 12 رقمی نماد']
        share.save()
    except:
        logger.exception(f'{share.ticker} update share detailed info failed {data}!!')
        raise
