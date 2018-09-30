import logging
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import requests

from crawler.models import Share, ShareHistory

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


def update_stock_history():
    for share in Share.objects.all():
        update_stock_history_item(share)


def update_stock_history_item(share, days=None):
    if days is None:
        days = (datetime.now(timezone.utc) - share.last_update).days + 1 if share.last_update else 999999

    print(share.__dict__, days)
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

    labels = ['date', 'high', 'low', 'close', 'last', 'first', 'yesterday', 'value', 'volume', 'count']
    df = pd.read_csv(StringIO(response.text), sep='@', lineterminator=';', names=labels, parse_dates=['date'])
    df = df.where((pd.notnull(df)), None)

    for index, row in df.iterrows():
        data = row.to_dict()
        data['share'] = share
        if ShareHistory.objects.filter(share=share, date=data['date']).exists():
            break

        ShareHistory(**data).save()

    share.save()


def update_stock_list():
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

        share.save()
