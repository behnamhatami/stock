import logging
import re
from datetime import timedelta, date
from statistics import mean, median
from string import digits
from typing import Any

from cachetools import cached, TTLCache
from django.db import models
from django.db.models import Q
from django.db.models.constraints import UniqueConstraint, CheckConstraint
from django.utils.functional import cached_property
from django_pandas.managers import DataFrameManager

from crawler.time_helper import convert_date_string_to_date

logger = logging.getLogger(__name__)


class ShareGroup(models.Model):
    id = models.IntegerField(null=False, blank=False, primary_key=True)
    name = models.CharField(null=False, blank=False, max_length=256)

    def __str__(self):
        return self.name


class HistoryHandler:
    BASE_DATE = date(1970, 1, 1)

    DAY_OFFSET_DEFAULT = 1

    @staticmethod
    def get_today_new(day_offset: int = DAY_OFFSET_DEFAULT):
        return date.today() - timedelta(days=day_offset)

    @cached(cache=TTLCache(maxsize=10 ** 5, ttl=60 * 60))
    def raw_daily_history(self):
        return self.history.all().order_by('date').to_dataframe(
            ['date', 'first', 'high', 'low', 'last', 'volume', 'value', 'open', 'close'])

    @cached(cache=TTLCache(maxsize=10 ** 5, ttl=60 * 60))
    def daily_history(self, day_offset: int = DAY_OFFSET_DEFAULT, normalize_strategy: str = 'scaler'):
        assert normalize_strategy in {'scaler', 'linear'}
        df = self.raw_daily_history().copy()
        df = df[df['date'] <= Share.get_today_new(day_offset)]
        if normalize_strategy == 'scaler':
            df['diff'] = df['close'] / df['open'].shift(-1)
            if df.shape[0] > 0:
                df.loc[df.shape[0] - 1, 'diff'] = 1
                assert (df.iloc[-1]['diff'] == 1)

                df['acc_diff'] = df['diff'][::-1].cumprod()[::-1]
                df['last'] /= df['acc_diff']
                df['first'] /= df['acc_diff']
                df['high'] /= df['acc_diff']
                df['low'] /= df['acc_diff']
                df['close'] /= df['acc_diff']
                df['open'] /= df['acc_diff']
        else:
            df['diff'] = df['close'] - df['open'].shift(-1)
            if df.shape[0] > 0:
                df.loc[df.shape[0] - 1, 'diff'] = 0
                assert (df.iloc[-1]['diff'] == 0)

                df['acc_diff'] = df['diff'][::-1].cumsum()[::-1]
                df['last'] -= df['acc_diff']
                df['first'] -= df['acc_diff']
                df['high'] -= df['acc_diff']
                df['low'] -= df['acc_diff']
                df['close'] -= df['acc_diff']
                df['open'] -= df['acc_diff']

        return df

    @cached(cache=TTLCache(maxsize=10 ** 5, ttl=24 * 60 * 60))
    def get_first_date_of_history(self):
        if self.history_size() > 0:
            return self.history.all().filter(date__lte=Share.get_today_new()).earliest('date').date
        else:
            return Share.get_today_new()

    def day_history(self, loc: int, day_offset: int = DAY_OFFSET_DEFAULT, normalize_strategy: str = 'scaler'):
        return self.daily_history(day_offset, normalize_strategy).iloc[loc]

    def history_of_date(self, d: date, day_offset: int = DAY_OFFSET_DEFAULT, normalize_strategy: str = 'scaler'):
        daily_history = self.daily_history(day_offset, normalize_strategy)
        return daily_history[daily_history['date'] <= d].iloc[-1]

    @cached(cache=TTLCache(maxsize=10 ** 5, ttl=60 * 60))
    def last_day_history(self, day_offset: int = DAY_OFFSET_DEFAULT):
        return self.history.all().filter(date__lte=Share.get_today_new(day_offset)).latest('date').__dict__

    @cached(cache=TTLCache(maxsize=10 ** 5, ttl=60 * 60))
    def history_size(self, day_offset: int = DAY_OFFSET_DEFAULT) -> int:
        return self.history.all().filter(date__lte=Share.get_today_new(day_offset)).count()

    @cached(cache=TTLCache(maxsize=10 ** 5, ttl=60 * 60))
    def get_average_trade_value(self, days: int) -> tuple[float, float]:
        if self.history_size() == 0:
            return 0, 0

        histories = list(self.history.all().filter(date__lte=Share.get_today_new()).order_by('date')[
                             max(self.history_size() - days, 0):].values_list('value', flat=True))

        return mean(histories), median(histories)


class Contract(models.Model, HistoryHandler):
    GOLD_COIN_ID: int = 42

    id = models.BigIntegerField(null=False, blank=False, primary_key=True)
    code = models.CharField(null=False, blank=False, max_length=256, db_index=True)
    description = models.CharField(max_length=256)
    size = models.IntegerField(null=False, blank=False)
    commodity_id = models.IntegerField(null=False, blank=False)
    commodity_name = models.CharField(null=False, blank=False, max_length=256, db_index=True)

    def compute_value(self, count, price):
        if self.id == Contract.GOLD_COIN_ID:
            ratio = 0.00125
        else:
            ratio = 0.0024
        return count * price * (1 + (-ratio if count < 0 else ratio))

    @staticmethod
    @cached(cache=TTLCache(maxsize=1, ttl=60 * 60))
    def gold_bar():
        return Contract.objects.get(id=41)

    @staticmethod
    @cached(cache=TTLCache(maxsize=1, ttl=60 * 60))
    def gold_coin():
        return Contract.objects.get(id=Contract.GOLD_COIN_ID)

    @staticmethod
    @cached(cache=TTLCache(maxsize=1, ttl=60 * 60))
    def silver_bar():
        return Contract.objects.get(id=43)

    @cached_property
    def synonym(self) -> dict[Any, str]:
        return {Contract.gold_bar(): 'CD1GOB0001',
                Contract.gold_coin(): 'CD1GOC0001',
                Contract.silver_bar(): 'CD1SIB0001'}.get(self, None)

    def __str__(self):
        return self.commodity_name


class Share(models.Model, HistoryHandler):
    class BazaarTypeChoices(models.IntegerChoices):
        NAGHDI = 1
        PURE = 2
        MOSHTAGHE = 3
        BASIC = 4
        BoorseKala = 7

    class BazaarChoices(models.IntegerChoices):
        Boors = 1
        FaraBoorse = 2

    id = models.BigIntegerField(null=False, blank=False, primary_key=True)
    isin = models.CharField(null=True, blank=False, max_length=256, db_index=True, unique=True)
    ticker = models.CharField(null=False, blank=False, max_length=256, db_index=True)
    description = models.CharField(max_length=256)

    enable = models.BooleanField(null=False, blank=False, default=True)
    bazaar = models.IntegerField(null=True, blank=False, choices=BazaarChoices.choices)
    bazaar_type = models.IntegerField(null=True, blank=False, choices=BazaarTypeChoices.choices)
    bazaar_group = models.IntegerField(null=True, blank=False)
    group = models.ForeignKey(ShareGroup, null=True, blank=False, default=None, on_delete=models.CASCADE,
                              related_name="shares")
    total_count = models.BigIntegerField(null=True, blank=False)
    base_volume = models.BigIntegerField(null=True, blank=False)

    strike_date = models.DateField(null=True, blank=False, default=None)
    base_share = models.ForeignKey("self", null=True, blank=False, default=None, on_delete=models.CASCADE,
                                   related_name="options")
    option_strike_price = models.BigIntegerField(null=True, blank=False, default=None)

    eps = models.IntegerField(null=True, blank=False)
    last_update = models.DateTimeField(null=True)
    extra_data = models.JSONField(null=True, blank=False)
    identity = models.JSONField(null=True, blank=False)

    def compute_value(self, count, price):
        # logger.info(f'{self.group.id}, {count}, {price}')
        if self.group_id == 59:  # maskan
            ratio = -0.0024 if count < 0 else 0.0049
        elif self.group_id == 68:  # etf
            if int(self.identity['subSector']['cSoSecVal']) == 6812:
                ratio = -0.00036875 if count < 0 else 0.00036875
            elif int(self.identity['subSector']['cSoSecVal']) == 6810:
                ratio = -0.001263 if count < 0 else 0.001175
            elif int(self.identity['subSector']['cSoSecVal']) == 6818:
                ratio = -0.0003625 if count < 0 else 0.0003625
            elif self.identity['subSector']['cSoSecVal'] in {6810, 6820} and self.identity['cgrValCot'] in {'QS', 'Q1'}:
                ratio = -0.001200 if count < 0 else 0.001200
            else:
                ratio = -0.0023 if count < 0 else 0.00245
        elif self.group_id == 69 or self.bazaar_group == 208:  # Akhza
            ratio = -0.000725 if count < 0 else 0.000725
        elif self.group_id == 56 and self.ticker.startswith('سکه'):  # gold
            ratio = -0.00125 if count < 0 else 0.00125
        elif self.group_id == 42 and self.ticker.startswith('زعف'):
            ratio = -0.0024 if count < 0 else 0.0024
        elif self.is_sell_option or self.is_buy_option:  # options
            ratio = -0.00103 if count < 0 else 0.00103
        else:  # TODO: Boors and fara boors could be separated
            ratio = -0.0088 if count < 0 else 0.003712

        return count * price * (1 + ratio)

    def parse_data(self):
        try:
            if self.is_buy_option or self.is_sell_option or 'اختیار' in self.description:
                parts = self.description.split('-')
                if len(parts) == 2:
                    parts = parts[0].rstrip(digits), parts[0][len(parts[0].rstrip(digits)):], parts[1]

                if len(parts) != 3:
                    if self.enable:
                        logger.warning(f"{self.ticker} description ignored as option ({self.description})")
                    return None, None, None

                if len(parts[2]) == 4:
                    parts[2] = parts[2] + '01'
                dt = convert_date_string_to_date(parts[2])
                ticker_parts = parts[0].replace('.', ' ').strip().split()

                dictionary = {
                    'ملی مس': 'فملی',
                    'حافرین': 'حآفرین',
                    'ص دارا': 'دارا یکم',
                    'ص آگاه': 'پتروآگاه',
                    'هم‌وزن': 'هم وزن',
                    'اخز101': 'اخزا101',
                    'معادن': 'ومعادن',
                    'بهمن': 'خبهمن',
                    'غدیر': 'وغدیر',
                    'تجارت': 'وتجارت',
                    'صندوق': 'وصندوق',
                    'امید': 'وامید',
                    'نارنج': 'نارنج اهرم',
                    'فارماکیان': 'فارما کیان',
                    'اگاس': 'آگاس',
                    'الکتروما': 'الکتروماد',
                }
                for ticker in [ticker_parts[-2] + ' ' + ticker_parts[-1], ticker_parts[-1]]:
                    ticker = dictionary.get(ticker, ticker)
                    candidates = [candidate for candidate in Share.objects.filter(ticker=ticker) if
                                  candidate.history_size() > 0]
                    if len(candidates) == 0:
                        continue
                    elif len(candidates) > 1:
                        candidates = sorted(candidates, key=lambda candidate: candidate.last_day_history()['date'],
                                            reverse=True)

                    if parts[1]:
                        return dt, int(parts[1]), candidates[0]
                    else:
                        return None, None, None
                else:
                    if self.enable:
                        logger.warning(f"{self.ticker} description ignored as option ({self.description})")
                    return None, None, None

            elif self.is_bond and self.identity and int(self.identity['subSector']['cSoSecVal']) == 6940:
                return convert_date_string_to_date(re.findall(r'\d+$', self.description)[0]), None, None
            elif self.is_rights_issue and self.identity:
                if Share.objects.filter(enable=True, ticker=self.ticker[:-1]).exists():
                    candidates = Share.objects.filter(enable=True, ticker=self.ticker[:-1])
                else:
                    candidates = Share.objects.filter(ticker=self.ticker[:-1])

                if len(candidates) == 0:
                    if self.enable:
                        logger.warning(f"{self.ticker} does not match any base share")
                    return None, None, None
                elif len(candidates) > 1:
                    candidates = sorted(candidates, reverse=True,
                                        key=lambda s: (
                                            s.last_day_history()['date'] if s.history_size() > 0 else Share.BASE_DATE,
                                            s.identity['cSocCSAC'] == self.identity['cSocCSAC']))

                return None, None, candidates[0]
            elif self.ticker[-1].isdigit() and self.identity:
                candidates = Share.objects.filter(enable=True, ticker=self.ticker.rstrip(digits))
                candidates = [candidate for candidate in candidates if candidate.identity and
                              candidate.identity['cSocCSAC'] == self.identity['cSocCSAC']]

                if len(candidates) == 0:
                    return None, None, None
                elif len(candidates) > 1:
                    candidates = sorted(candidates, reverse=True,
                                        key=lambda s: s.last_day_history()[
                                            'date'] if s.history_size() > 0 else Share.BASE_DATE)

                return None, None, candidates[0]
            else:
                return None, None, None
        except:
            logger.exception(f"parsing {self.ticker} description encounter error. ({self.__dict__})", exc_info=True)
            return None, None, None

    @cached_property
    def is_rights_issue(self):
        return self.ticker[-1] == 'ح'

    @cached_property
    def is_buy_option(self):
        return self.ticker[0] in ['ض', 'ظ'] and 'اختیار' in self.description

    @cached_property
    def is_sell_option(self):
        return self.ticker[0] in ['ط', 'ه'] and 'اختیار' in self.description

    @cached_property
    def is_bond(self):
        return self.group_id == 69

    @cached_property
    def is_special(self):
        return self.is_rights_issue or self.is_buy_option or self.is_sell_option

    @cached_property
    def is_gold_coin(self):
        return self.group_id == 56 and int(self.identity['subSector']['cSoSecVal']) == 5699 and self.identity[
            'cgrValCot'] == 'QS'

    def __str__(self):
        return self.ticker


class DailyHistory(models.Model):
    share = models.ForeignKey(Share, null=True, blank=True, on_delete=models.CASCADE, related_name="history")
    contract = models.ForeignKey(Contract, null=True, blank=True, on_delete=models.CASCADE, related_name="history")
    date = models.DateField(null=False, blank=False, db_index=True)

    first = models.IntegerField(null=False, blank=False)
    last = models.IntegerField(null=False, blank=False)
    close = models.IntegerField(null=False, blank=False)
    open = models.IntegerField(null=False, blank=False)

    high = models.IntegerField(null=False, blank=False)
    low = models.IntegerField(null=False, blank=False)

    volume = models.BigIntegerField(null=False, blank=False)
    count = models.BigIntegerField(null=False, blank=False)
    value = models.BigIntegerField(null=False, blank=False)

    objects = DataFrameManager()

    @cached_property
    def asset(self):
        return self.share if self.share else self.contract

    class Meta:
        constraints = [
            UniqueConstraint(
                fields=['date', 'share'],
                condition=Q(share__isnull=False),
                name='daily_history_date_share_unique'
            ),
            UniqueConstraint(
                fields=['date', 'contract'],
                condition=Q(contract__isnull=False),
                name='daily_history_date_contract_unique'
            ),
            CheckConstraint(
                condition=(
                        Q(share__isnull=False, contract__isnull=True) |
                        Q(share__isnull=True, contract__isnull=False)
                ),
                name='daily_history_share_contract_exclusion'
            ),
        ]

    def __str__(self):
        return f"{self.asset}: {self.date}"
