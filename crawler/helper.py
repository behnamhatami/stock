import asyncio
import concurrent
import logging
from datetime import datetime, timezone
from functools import partial
from io import StringIO

import pandas as pd
import requests

from crawler.models import Share, ShareDailyHistory

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:61.0) Gecko/20100101 Firefox/61.0',
    'Accept': 'text/plain, */*; q=0.01',
    'Accept-Language': 'en-GB,en;q=0.5',
    'Origin': 'http://www.tsetmc.com',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
}


async def update_stock_history():
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as pool:
        futures = [loop.run_in_executor(pool, partial(update_stock_history_item, share)) for share in
                   Share.objects.all()]
        await asyncio.gather(*futures)
        logger.info(
            "{} tasks completed out of {}".format(len(list(filter(lambda future: future.done(), futures))),
                                                  len(futures)))


def update_stock_history_item(share, days=None, batch_size=100):
    if days is None:
        days = (datetime.now(timezone.utc) - share.last_update).days + 1 if share.last_update else 999999

    headers = HEADERS.copy()
    headers['Referer'] = 'http://www.tsetmc.com/Loader.aspx?ParTree=151311&i={}'.format(share.id)

    params = (
        ('i', share.id),
        ('Top', days),
        ('A', '0'),
    )
    share.last_update = datetime.now(timezone.utc)
    response = requests.get('http://members.tsetmc.com/tsev2/data/InstTradeHistory.aspx', headers=headers,
                            params=params)

    if response.status_code != 200:
        logger.error("Http Error {}".format(response.status_code))
        return

    labels = ['date', 'high', 'low', 'tomorrow', 'close', 'first', 'yesterday', 'value', 'volume', 'count']
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
    logger.debug("history of {} in {} days added.".format(share.ticker, days))


async def update_stock_list(batch_size=100):
    headers = HEADERS.copy()
    headers['Referer'] = 'http://www.tsetmc.com/Loader.aspx?ParTree=15131F'

    params = (
        ('h', '0'),
        ('r', '0'),
    )

    response = requests.get('http://www.tsetmc.com/tsev2/data/MarketWatchInit.aspx', headers=headers, params=params)

    if response.status_code != 200:
        return

    '''
        separated with @ text
        part 0: ?
        part 1: general info of bazaar  ['date', 'last_transaction_time', 'main_stock_status', 'main_stock_index', 
        'main_stock_index_diff', '?', 'main_stock_volume', 'main_stock_value', 'main_stock_count', 'other_stock_status', 
        'other_stock_volume', 'other_stock_value', 'other_stock_count', 'this_stock_status', 'this_stock_volume', 
        'third_stock_value', 'third_stock_count']
        part 2: ?
        part 3; ?
        part 4: ?
    '''

    df = pd.read_csv(StringIO(response.text.split("@")[2]), sep=',', lineterminator=';', header=None)
    df = df.where((pd.notnull(df)), None)

    share_list = []
    for index, row in df.iterrows():
        id = row[0]
        try:
            share = Share.objects.get(id=id)
        except Share.DoesNotExist:
            share = Share()

        share.eps = row[14]
        share.ticker = row[2]
        share.id = row[0]
        share.description = row[3]

        if share.id:
            share.save()
        else:
            share_list.append(share)

    Share.objects.bulk_create(share_list, batch_size=100)


async def get_day_price(share):
    headers = HEADERS.copy()
    headers['Referer'] = 'http://www.tsetmc.com/Loader.aspx?ParTree=151311&i={}'.format(share.id)
    headers['X-Requested-With'] = 'XMLHttpRequest'

    params = (
        ('i', share.id),
    )

    response = requests.get('http://www.tsetmc.com/tsev2/chart/data/IntraDayPrice.aspx', headers=headers, params=params)

    if response.status_code != 200:
        return

    labels = ['time', 'high', 'low', 'open', 'close', 'volume']
    df = pd.read_csv(StringIO(response.text), sep=',', lineterminator=';', names=labels)
    df = df.where((pd.notnull(df)), None)
    print(df)


async def get_current_info(share):
    headers = HEADERS.copy()
    headers['Referer'] = 'http://www.tsetmc.com/Loader.aspx?ParTree=151311&i={}'.format(share.id)
    headers['X-Requested-With'] = 'XMLHttpRequest'

    params = (
        ('i', share.id),
        ('c', '27 '),
    )

    response = requests.get('http://www.tsetmc.com/tsev2/data/instinfofast.aspx', headers=headers, params=params)

    if response.status_code != 200:
        return

    print(response.text, end='\n\n')
    print(response.text.split(";")[3])
    '''
        separated with ; text
        part 0: ['last_transaction_time', 'state', 'last', 'tomorrow', 'first', 'yesterday', 'max_range', 'min_range', 
        'count', 'volume', 'value', '?', 'date', 'time']
        part 1: general info of bazaar  ['date', 'last_transaction_time', 'main_stock_status', 'main_stock_index', 
        'main_stock_index_diff', '?', 'main_stock_volume', 'main_stock_value', 'main_stock_count', 'other_stock_status', 
        'other_stock_volume', 'other_stock_value', 'other_stock_count', 'this_stock_status', 'this_stock_volume', 
        'third_stock_value', 'third_stock_count']
        part 2: ['buy_count', 'buy_volume', 'buy_order', 'sell_order', 'sell_order', 'sell_count']
        part 3; ?
        part 4: ['buy_personal', 'buy_legal', '?', 'sell_personal', 'sell_legal', 'buy_count_personal', 
        'buy_count_legal', '?', 'sell_count_personal', 'sell_count_legal']
        part 5: stocks from same group ['last', 'tomorrow', 'yesterday', 'count', 'volume', 'value']
    '''
#    df = pd.read_csv(StringIO(response.text.split("@")[0]), sep=',', lineterminator=';', header=None)
#    df = df.where((pd.notnull(df)), None)
#    print(df)
