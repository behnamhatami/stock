import logging
from datetime import timedelta, date

# Create your models here.
from django.utils.functional import cached_property
from django_pandas.managers import DataFrameManager
from persiantools.jdatetime import JalaliDate

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

from django.db import models
from django.core.serializers.json import DjangoJSONEncoder
import json


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

    DAY_OFFSET = 0

    @staticmethod
    def get_today():
        return date.today() - timedelta(days=Share.DAY_OFFSET)

    id = models.BigIntegerField(null=False, blank=False, primary_key=True)
    ticker = models.CharField(null=False, blank=False, max_length=256)
    description = models.CharField(max_length=256)

    enable = models.BooleanField(null=False, blank=False, default=True)
    bazaar = models.IntegerField(null=True, blank=False, choices=BazaarChoices.choices)
    bazaar_type = models.IntegerField(null=True, blank=False, choices=BazaarTypeChoices.choices)
    bazaar_group = models.IntegerField(null=True, blank=False)
    group = models.ForeignKey(ShareGroup, null=True, blank=False, default=None, on_delete=models.CASCADE,
                              related_name="shares")
    total_count = models.BigIntegerField(null=True, blank=False)
    base_volume = models.BigIntegerField(null=True, blank=False)

    option_strike_price = models.BigIntegerField(null=True, blank=False, default=None)
    option_strike_date = models.DateField(null=True, blank=False, default=None)
    option_base_share = models.ForeignKey("self", null=True, blank=False, default=None, on_delete=models.CASCADE,
                                          related_name="options")

    eps = models.IntegerField(null=True, blank=False)
    last_update = models.DateTimeField(null=True)
    extra_data = JSONField(null=True, blank=False)

    def compute_value(self, count, price):
        if self.group.id == 59:  # maskan
            return count * price * ((1 - 0.0024) if count < 0 else (1 + 0.0049))
        elif self.group.id in [68, 69]:  # etf or akhza
            return count * price * ((1 - 0.000725) if count < 0 else (1 + 0.000725))
        elif self.group.id == 68:
            return count * price * ((1 - 0.000725) if count < 0 else (1 + 0.000725))
        else:  # TODO: Boors and fara boors could be seprated
            return count * price * ((1 - 0.00975) if count < 0 else (1 + 0.00454))

    def parse_description(self):
        parts = self.description.split('-')
        try:
            if len(parts) != 3:
                logger.warning("{} description ignored as option".format(self.ticker))
                return None, None, None
            date_parts = list(map(int, parts[2].split('/')))
            if 0 <= date_parts[0] <= 31 < date_parts[2]:
                date_parts.reverse()

            if date_parts[0] <= 100:
                date_parts[0] += 1400 if date_parts[0] <= 50 else 1300

            return int(parts[1]), JalaliDate(*date_parts).to_gregorian(), Share.objects.get(enable=True,
                                                                                            ticker=parts[0].strip()[8:])
        except Exception as e:
            logger.exception("parsing {} encounter error. ({})".format(self.ticker, self.description))
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
    def is_special(self):
        return self.is_rights_issue or self.is_buy_option or self.is_sell_option

    @cached_property
    def raw_daily_history(self):
        df = self.history.filter(
            date__lt=Share.get_today()).all().order_by(
            'date').to_dataframe(['date', 'open', 'high', 'low', 'close', 'volume', 'value', 'yesterday', 'tomorrow'])
        return df

    @cached_property
    def daily_history(self):
        df = self.raw_daily_history.copy()
        df['diff'] = df['tomorrow'] / df['yesterday'].shift(-1)
        if df.shape[0] > 0:
            df.loc[df.shape[0] - 1, 'diff'] = 1
            assert (df.iloc[-1]['diff'] == 1)

            df['acc_diff'] = df['diff'][::-1].cumprod()[::-1]
            df['close'] /= df['acc_diff']
            df['open'] /= df['acc_diff']
            df['high'] /= df['acc_diff']
            df['low'] /= df['acc_diff']
            df['tomorrow'] /= df['acc_diff']
            df['yesterday'] /= df['acc_diff']

        return df

    def day_history(self, loc):
        return self.daily_history.iloc[loc]

    @cached_property
    def last_day_history(self):
        return self.day_history(-1)

    @cached_property
    def history_size(self):
        return self.daily_history.shape[0]

    def __str__(self):
        return self.ticker


class ShareDailyHistory(models.Model):
    share = models.ForeignKey(Share, null=False, blank=False, on_delete=models.CASCADE, related_name="history")
    date = models.DateField(null=False, blank=False)

    open = models.IntegerField(null=False, blank=False)
    close = models.IntegerField(null=False, blank=False)
    tomorrow = models.IntegerField(null=False, blank=False)
    yesterday = models.IntegerField(null=False, blank=False)

    high = models.IntegerField(null=False, blank=False)
    low = models.IntegerField(null=False, blank=False)

    volume = models.BigIntegerField(null=False, blank=False)
    count = models.BigIntegerField(null=False, blank=False)
    value = models.BigIntegerField(null=False, blank=False)

    objects = DataFrameManager()

    class Meta:
        unique_together = (("share", "date"),)

    def __str__(self):
        return "{}: {}".format(self.share, self.date)
