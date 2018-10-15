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
    print(Share.objects.count())
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        await asyncio.gather(
            *(loop.run_in_executor(pool, partial(update_stock_history_item, share)) for share in Share.objects.all()))


def update_stock_history_item(share, days=None, batch_size=100):
    try:
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
    except Exception as e:
        logger.exception(e)


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
