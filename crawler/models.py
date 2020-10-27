import json
import logging
import re
from datetime import timedelta, date

from django.core.serializers.json import DjangoJSONEncoder
from django.db import models
from django.utils.functional import cached_property
from django_pandas.managers import DataFrameManager

from crawler.time_helper import convert_date_string_to_date

logger = logging.getLogger(__name__)


class JSONField(models.TextField):
    """
    JSONField es un campo TextField que serializa/deserializa objetos JSON.
    Django snippet #1478

    Ejemplo:
        class Page(models.Model):
            data = JSONField(blank=True, null=True)

        page = Page.objects.get(pk=5)
        page.data = {'title': 'test', 'type': 3}
        page.save()
    """

    def to_python(self, value):
        if value == "":
            return None

        try:
            if isinstance(value, str):
                return json.loads(value)
        except ValueError:
            pass
        return value

    def from_db_value(self, value, *args):
        return self.to_python(value)

    def get_db_prep_save(self, value, *args, **kwargs):
        if value == "":
            return None
        if isinstance(value, dict):
            value = json.dumps(value, cls=DjangoJSONEncoder)
        return value


class ShareGroup(models.Model):
    id = models.IntegerField(null=False, blank=False, primary_key=True)
    name = models.CharField(null=False, blank=False, max_length=256)

    def __str__(self):
        return self.name


class Share(models.Model):
    class BazaarTypeChoices(models.IntegerChoices):
        NAGHDI = 1
        PURE = 2
        MOSHTAGHE = 3
        BASIC = 4
        BoorseKala = 7

    class BazaarChoices(models.IntegerChoices):
        Boors = 1
        FaraBoorse = 2

    DAY_OFFSET_DEFAULT = 1
    DAY_OFFSET = DAY_OFFSET_DEFAULT
    NORMALIZE_STRATEGY = "scaler"
    HISTORY_CACHE = {}
    CACHE_DATE = None

    INFO = dict()

    @staticmethod
    def get_today():
        return date.today() - timedelta(days=Share.DAY_OFFSET)

    id = models.BigIntegerField(null=False, blank=False, primary_key=True)
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

    def compute_value(self, count, price):
        # logger.info(f'{self.group.id}, {count}, {price}')
        if self.group_id == 59:  # maskan
            return count * price * ((1 - 0.0024) if count < 0 else (1 + 0.0049))
        elif self.group_id in [68, 69]:  # etf or akhza
            return count * price * ((1 - 0.000725) if count < 0 else (1 + 0.000725))
        elif self.group_id == 56 and self.ticker.startswith('سکه'):
            return count * price * ((1 - 0.00125) if count < 0 else (1 + 0.00125))
        else:  # TODO: Boors and fara boors could be separated
            return count * price * ((1 - 0.0064125) if count < 0 else (1 + 0.0064125))

    def parse_data(self):
        try:
            if self.is_buy_option or self.is_sell_option:
                parts = self.description.split('-')
                if len(parts) != 3:
                    logger.warning(f"{self.ticker} description ignored as option")
                    return None, None, None

                dt = convert_date_string_to_date(parts[2])

                ticker = parts[0].strip()[8:].strip()
                dictionary = {
                    'ملی مس': 'فملی'
                }
                ticker = dictionary.get(ticker, ticker)

                return dt, int(parts[1]), Share.objects.get(enable=True, ticker=ticker)
            elif self.is_bond and self.extra_data and self.extra_data['کد زیر گروه صنعت'] == '6940':
                return convert_date_string_to_date(re.findall(r'\d+$', self.description)[0]), None, None
            elif self.is_rights_issue:
                if Share.objects.filter(enable=True, ticker=self.ticker[:-1]).exists():
                    result = Share.objects.filter(enable=True, ticker=self.ticker[:-1])
                else:
                    result = Share.objects.filter(ticker=self.ticker[:-1])

                result = sorted(result,
                                key=lambda share: share.last_day_history['date'] if share.history_size else date(1970,
                                                                                                                 1, 1))

                if result:
                    return None, None, result[-1]
                else:
                    logger.warning(f"{self.ticker} does not match any base share")
                    return None, None, None
            else:
                return None, None, None

        except Exception as e:
            logger.exception(f"parsing {self.ticker} description encounter error. ({self.__dict__})")
            return None, None, None

    @cached_property
    def is_rights_issue(self):
        return self.ticker[-1] == 'ح'

    @cached_property
    def is_buy_option(self):
        return self.ticker[0] == 'ض' and (self.bazaar_group == 311 or self.bazaar_group is None)

    @cached_property
    def is_sell_option(self):
        return self.ticker[0] == 'ط' and (self.bazaar_group == 312 or self.bazaar_group is None)

    @cached_property
    def is_bond(self):
        return self.group_id == 69

    @cached_property
    def is_special(self):
        return self.is_rights_issue or self.is_buy_option or self.is_sell_option

    @property
    def raw_daily_history(self):
        return self.history.all().order_by('date').to_dataframe(
            ['date', 'first', 'high', 'low', 'last', 'volume', 'value', 'open', 'close'])

    @property
    def daily_history(self):
        if Share.CACHE_DATE != date.today():
            Share.CACHE_DATE = date.today()
            Share.HISTORY_CACHE.clear()
            logger.info("history cache reset!!")

        hash_key = (self, Share.get_today(), Share.NORMALIZE_STRATEGY)
        if hash_key in Share.HISTORY_CACHE and Share.HISTORY_CACHE[hash_key]['time'] == self.last_update:
            return Share.HISTORY_CACHE[hash_key]['value']

        df = self.raw_daily_history.copy()
        df = df[df['date'] <= Share.get_today()]
        if Share.NORMALIZE_STRATEGY == 'scaler':
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

        Share.HISTORY_CACHE[hash_key] = {'value': df, 'time': self.last_update}
        return df

    def get_first_date_of_history(self):
        return self.day_history(0)['date'] if self.history_size > 0 else Share.get_today()

    def day_history(self, loc):
        return self.daily_history.iloc[loc]

    def history_of_date(self, d):
        return self.daily_history[self.daily_history['date'] <= d].iloc[-1]

    @property
    def last_day_history(self):
        return self.day_history(-1)

    @property
    def history_size(self):
        return self.daily_history.shape[0]

    def __str__(self):
        return self.ticker


class ShareDailyHistory(models.Model):
    share = models.ForeignKey(Share, null=False, blank=False, on_delete=models.CASCADE, related_name="history")
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

    class Meta:
        unique_together = (("share", "date"),)

    def __str__(self):
        return f"{self.share}: {self.date}"
