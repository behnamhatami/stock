import asyncio
import concurrent
import logging
from datetime import datetime, timezone
from io import StringIO
from persiantools import characters

import pandas as pd
from retry import retry
import requests

from crawler.models import Share, ShareDailyHistory

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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

def run_jobs(jobs, max_workers=100):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(job) for job in jobs]
        
        success, error = 0, 0
        for index, future in enumerate(concurrent.futures.as_completed(futures)):
            if future.exception():
                logger.exception(future.exception())
                error += 1
            else:
                success += 1
            if index % 100 == 99:
                logger.info("{}/{} out of {}({}%)".format(success, index+1, len(futures), round((index+1)/len(futures) * 100, 2)))
            
        logger.info(
            "{} tasks completed out of {}".format(success, len(futures)))


@retry(tries=4, delay=1, backoff=2)
def search_stock(keyword):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:74.0) Gecko/20100101 Firefox/74.0',
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.5',
            'X-Requested-With': 'XMLHttpRequest',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Referer': 'http://www.tsetmc.com/Loader.aspx?ParTree=15',
        }

        response = requests.get('http://www.tsetmc.com/tsev2/data/search.aspx?skey={}'.format(keyword), headers=headers)

        if response.status_code != 200:
            raise Exception("Http Error: {}".format(response.status_code))

        if len(response.text) == 0:
            return

        lables = ['ticker', 'description', 'id', '', '', '', 'bazaar type', 'enable', 'bazaar', 'bazaar']
        df = pd.read_csv(StringIO(response.text), sep=',', lineterminator=';', header=None)
        df = df.where((pd.notnull(df)), None)


        new_list, update_list = [], []
        for index, row in df.iterrows():
            id = row[2]
            try:
                share = Share.objects.get(id=id)
            except Share.DoesNotExist:
                share = Share()

            (update_list if share.id else new_list).append(share)
                
            share.ticker = characters.ar_to_fa(str(row[0]))
            share.description = characters.ar_to_fa(row[1])
            share.id = row[2]
            share.bazaar_type = row[6]
            share.enable = row[7]        

        if new_list:
            logger.info("new stocks: {}".format(new_list))
        Share.objects.bulk_create(new_list, batch_size=100)
        Share.objects.bulk_update(update_list, ['ticker', 'description', 'bazaar_type', 'enable'], batch_size=100)
        if new_list:
            logger.info("update stock list, {} added, {} updated.".format(len(new_list), len(update_list)))
    except Exception as e:
        logger.exception(e)
        raise e

@retry(tries=4, delay=1, backoff=2)
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
        raise Exception("Http Error: {}".format(response.status_code))


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
    if share_histories:
        logger.info("history of {} in {} days added.".format(share.ticker, len(share_histories)))
   

@retry(tries=4, delay=1, backoff=2)
def update_stock_list(batch_size=100):
    headers = HEADERS.copy()
    headers['Referer'] = 'http://www.tsetmc.com/Loader.aspx?ParTree=15131F'

    params = (
        ('h', '0'),
        ('r', '0'),
    )

    response = requests.get('http://www.tsetmc.com/tsev2/data/MarketWatchInit.aspx', headers=headers, params=params)

    if response.status_code != 200:
        raise Exception("Http Error: {}".format(response.status_code))


    '''
        separated with @ text
        part 0: ?
        part 1: general info of bazaar  ['date and time of last_transaction', 'boorse_status', 'boorse_index',
        'boorse_index_diff', 'boorse_market cap', 'boorse_volume', 'boorse_value', 'boorse_count', 'faraboorse_status',
        'faraboorse_volume', 'faraboorse_value', 'faraboorse_count', 'moshtaghe_status', 'moshtaghe_volume',
        'moshtaghe_value', 'moshtaghe_count']
        part 2: ['id', 'IR', 'ticker', 'description', '?', 'first', 'tomorrow', 'last', 'count', 'volume', 'value', 'low', 'high', 'yesterday', 'eps', 'base volume', '', 'bazaar type', 'group', 'max_price_possible', 'min_price_possible', 'number of stock', 'bazaar group']
        part 3; ['id', 'order', 'sell_count', 'buy_count', 'buy_price', 'sell_price', 'buy_volume', 'sell_volume']
        part 4: ?
    '''
    
    df = pd.read_csv(StringIO(response.text.split("@")[2]), sep=',', lineterminator=';', header=None)
    df = df.where((pd.notnull(df)), None)

    new_list, update_list = [], []
    for index, row in df.iterrows():
        try:
            share = Share.objects.get(id=row[0])
        except Share.DoesNotExist:
            share = Share()

        (update_list if share.id else new_list).append(share)
        
        share.enable = True
        share.id = row[0]
        share.ticker = characters.ar_to_fa(str(row[2]))
        share.description = characters.ar_to_fa(row[3])
        share.eps = row[14]
        share.base_volume = row[15]
        share.bazaar_type = row[17]
        share.group = row[18]
        share.total_count = row[21]
        share.bazaar_group = row[22]
        
    if new_list:
        logger.info("new stocks: {}".format(new_list))
    Share.objects.bulk_create(new_list, batch_size=100)
    Share.objects.bulk_update(update_list, ['enable', 'ticker', 'description', 'eps', 'base_volume', 'bazaar_type', 'group', 'total_count', 'bazaar_group'], batch_size=100)
    logger.info("update stock list, {} added, {} updated.".format(len(new_list), len(update_list)))


def get_day_price(share):
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


def get_current_info(share):
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
        part 2: ['buy_count', 'buy_volume', 'buy_order', 'sell_order', 'sell_volume', 'sell_count']
        part 3; ?
        part 4: ['buy_personal', 'buy_legal', '?', 'sell_personal', 'sell_legal', 'buy_count_personal',
        'buy_count_legal', '?', 'sell_count_personal', 'sell_count_legal']
        part 5: stocks from same group ['last', 'tomorrow', 'yesterday', 'count', 'volume', 'value']
    '''
#    df = pd.read_csv(StringIO(response.text.split("@")[0]), sep=',', lineterminator=';', header=None)
#    df = df.where((pd.notnull(df)), None)
#    print(df)
