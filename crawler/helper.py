import concurrent
import logging
import math
import typing
from io import StringIO

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
from django.utils import timezone
from getuseragent import UserAgent
from persiantools import characters
from tenacity import stop_after_attempt, wait_random_exponential, retry, RetryCallState
from tenacity import _utils

from crawler.decorators import log_time
from crawler.models import Share, ShareDailyHistory, ShareGroup

logger = logging.getLogger(__name__)
user_agent = UserAgent()


def get_headers(share, referer=None):
    return {
        'User-Agent': user_agent.Random(),
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Referer': referer if referer else f'http://www.tsetmc.com/Loader.aspx?ParTree=151311&i={share.id}',
        'X-Requested-With': 'XMLHttpRequest',
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
            if 'headers' in kwargs:
                del kwargs['headers']
                
        logger.log(
            log_level,
            f"Finished call to '{fn_name}/{args}/{kwargs}"
            f"after {sec_format % retry_state.seconds_since_start}(s), "
            f"this was the {_utils.to_ordinal(retry_state.attempt_number)} time calling it.",
        )

    return log_it


@retry(reraise=True, stop=stop_after_attempt(6), wait=wait_random_exponential(multiplier=1, max=60),
       after=after_log(logger, logging.DEBUG))
def submit_request(url, params, headers, retry_on_empty_response=False, timeout=5):
    response = requests.get(url, params=params, headers=headers, timeout=timeout, verify=False)

    if response.status_code != 200:
        raise Exception(f"Http Error: {response.status_code}, {url.split('/')[-1]}, {params}")

    if retry_on_empty_response and len(response.text.strip()) == 0:
        raise Exception(f"Http Error: empty response, {url.split('/')[-1]}, {params}")

    return response


@log_time
def run_jobs(job_title, jobs, max_workers=100, log=True, log_exception_on_failure=True):
    number_of_buckets = max(min(20, len(jobs) // 10), 2)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(job) for job in jobs]

        success, error = 0, 0
        for index, future in enumerate(concurrent.futures.as_completed(futures)):
            if future.exception():
                error += 1
                if log:
                    if log_exception_on_failure:
                        logger.exception(f'{job_title}: {future.exception()}')
                    else:
                        logger.warning(f'{job_title}: {future.exception()}')
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
    response = requests.get('http://www.tsetmc.com/tsev2/data/InstValue.aspx', params=params, timeout=10)

    if response.status_code != 200:
        raise Exception(f"Http Error: {response.status_code}")

    return response.text


@log_time
def update_share_groups():
    response = requests.get('http://www.tsetmc.com/Loader.aspx?ParTree=111C1213')

    if response.status_code != 200:
        raise Exception(f"Http Error: {response.status_code}")

    for group_data in BeautifulSoup(response.text, features='html.parser').body.select('tr[id]'):
        id = group_data.select('td')[0].contents[0].strip()
        name = group_data.select('td')[1].contents[0].strip()

        if not id.isdigit():
            continue

        try:
            group = ShareGroup.objects.get(id=id)
        except ShareGroup.DoesNotExist:
            group = ShareGroup()

        group.id = int(id)
        group.name = characters.ar_to_fa(name)
        group.save()

    logger.info(f"Share group info updated. number of groups: {ShareGroup.objects.count()}")


def search_share(keyword):
    response = submit_request('http://www.tsetmc.com/tsev2/data/search.aspx', params=(('skey', keyword),),
                              headers=get_headers(None, 'http://www.tsetmc.com/Loader.aspx?ParTree=15'), timeout=25)

    if len(response.text) == 0:
        return

    # lables = ['ticker', 'description', 'id', '', '', '', 'bazaar type', 'enable', 'bazaar', 'bazaar']
    df = pd.read_csv(StringIO(response.text), sep=',', lineterminator=';', header=None)
    df = df.where((pd.notnull(df)), None)

    new_list, update_list = [], []
    for index, row in df.iterrows():
        id = row[2]
        try:
            share = Share.objects.get(id=id)
        except Share.DoesNotExist:
            for share in new_list:
                if share.id == row[2]:
                    break
            else:
                share = Share()

        (update_list if share.id else new_list).append(share)

        share.ticker = characters.ar_to_fa(str(row[0])).strip()
        share.description = characters.ar_to_fa(row[1]).strip()
        share.id = row[2]
        share.bazaar_type = row[6]
        share.enable = bool(row[7])
        share.strike_date, share.option_strike_price, share.base_share = share.parse_data()

    Share.objects.bulk_create(new_list, batch_size=100)
    Share.objects.bulk_update(update_list, ['ticker', 'description', 'bazaar_type', 'enable', 'option_strike_price',
                                            'strike_date', 'base_share'], batch_size=100)
    if new_list:
        logger.info(f"update share list, {len(new_list)} added ({new_list}), {len(update_list)} updated.")


def update_share_history_item(share, days=None, batch_size=100):
    if days is None:
        days = (timezone.now() - share.last_update).days + 1 if share.last_update else 999999

    params = (
        ('i', share.id),
        ('Top', days),
        ('A', '0'),
    )
    response = submit_request('http://tsetmc.com/tsev2/data/InstTradeHistory.aspx', params=params,
                              headers=get_headers(share), timeout=25)

    share.last_update = timezone.now()

    labels = ['date', 'high', 'low', 'close', 'last', 'first', 'open', 'value', 'volume', 'count']
    df = pd.read_csv(StringIO(response.text), sep='@', lineterminator=';', names=labels, parse_dates=['date'])
    df = df.where((pd.notnull(df)), None)

    share_histories = []
    for index, row in df.iterrows():
        data = row.to_dict()
        data['share'] = share
        if ShareDailyHistory.objects.filter(share=share, date=data['date']).exists():
            break

        share_histories.append(ShareDailyHistory(**data))

    ShareDailyHistory.objects.bulk_create(share_histories, batch_size=batch_size)

    share.save()
    if share_histories:
        logger.info(f"history of {share.ticker} in {len(share_histories)} days added.")


@log_time
def update_share_list(batch_size=100):
    text = get_watch_list()

    df = pd.read_csv(StringIO(text.split("@")[2]), sep=',', lineterminator=';', header=None)
    df = df.replace({np.nan: None})

    new_list, update_list = [], []
    for index, row in df.iterrows():
        try:
            share = Share.objects.get(id=row[0])
        except Share.DoesNotExist:
            share = Share()

        (update_list if share.id else new_list).append(share)

        share.enable = True
        share.id = row[0]
        share.ticker = characters.ar_to_fa(str(row[2])).strip()
        share.description = characters.ar_to_fa(row[3]).strip()
        share.eps = row[14] if row[14] and row[14] != 'nan' and not math.isnan(row[14]) else None
        share.base_volume = row[15]
        share.bazaar_type = row[17]
        share.group = ShareGroup.objects.get(id=row[18])
        share.total_count = row[21]
        share.bazaar_group = row[22]
        share.strike_date, share.option_strike_price, share.base_share = share.parse_data()

    Share.objects.bulk_create(new_list, batch_size=batch_size)
    Share.objects.bulk_update(update_list,
                              ['enable', 'ticker', 'description', 'eps', 'base_volume', 'bazaar_type', 'group',
                               'total_count', 'bazaar_group', 'option_strike_price', 'strike_date', 'base_share'],
                              batch_size=100)
    logger.info(f"update share list, {len(new_list)} ({new_list}) added, {len(update_list)} updated.")


def get_watch_list():
    response = submit_request('http://www.tsetmc.com/tsev2/data/MarketWatchInit.aspx',
                              headers=get_headers(None, 'http://www.tsetmc.com/Loader.aspx?ParTree=15131F'),
                              params=(('h', '0'), ('r', '0')), retry_on_empty_response=True, timeout=10)

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
    response = requests.get('http://www.tsetmc.com/Loader.aspx', headers=get_headers(share),
                            params=(('Partree', '15131M'), ('i', share.id),), timeout=10)

    data = {}
    for row in BeautifulSoup(response.text, features='html.parser').body.select('tr'):
        key = row.select('td')[0].contents[0].strip()

        value_contents = row.select('td')[1].contents
        value = value_contents[0].strip() if value_contents else None

        data[key] = value

    share.extra_data = data
    share.group = ShareGroup.objects.get(id=data['کد گروه صنعت'])
    share.save()
