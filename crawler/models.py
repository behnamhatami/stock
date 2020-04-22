from datetime import timedelta, date
from persiantools.jdatetime import JalaliDate

import logging
from django.db import models

# Create your models here.
from django.utils.functional import cached_property
from django_pandas.managers import DataFrameManager

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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

    def get_today():
        return date.today() - timedelta(days=Share.DAY_OFFSET)

    id = models.BigIntegerField(null=False, blank=False, primary_key=True)
    ticker = models.CharField(null=False, blank=False, max_length=256)
    description = models.CharField(max_length=256)

    enable = models.BooleanField(null=False, blank=False, default=True)
    bazaar = models.IntegerField(null=True, blank=False, choices=BazaarChoices.choices)
    bazaar_type = models.IntegerField(null=True, blank=False, choices=BazaarTypeChoices.choices)
    bazaar_group = models.IntegerField(null=True, blank=False)
    group = models.ForeignKey(ShareGroup, null=True, blank=False, default=None, on_delete=models.CASCADE, related_name="shares")
    total_count = models.BigIntegerField(null=True, blank=False)
    base_volume = models.BigIntegerField(null=True, blank=False)

    option_strike_price = models.BigIntegerField(null=True, blank=False, default=None)
    option_strike_date = models.DateField(null=True, blank=False, default=None)
    option_base_share = models.ForeignKey("self", null=True, blank=False, default=None, on_delete=models.CASCADE, related_name="options")

    eps = models.IntegerField(null=True, blank=False)
    last_update = models.DateTimeField(null=True)

    def parse_description(self):
        parts = self.description.split('-')
        try:
            if len(parts) != 3:
                logger.warning("{} description ignored as option".format(self.ticker))
                return None, None, None
            date_parts = list(map(int, parts[2].split('/')))
            if 0 <= date_parts[0] <= 31 and date_parts[2] > 31:
                date_parts.reverse()

            if date_parts[0] <= 100:
                date_parts[0] += 1400 if date_parts[0] <= 50 else 1300

            return int(parts[1]), JalaliDate(*date_parts).to_gregorian(), Share.objects.get(enable=True, ticker=parts[0].strip()[8:])
        except Exception as e:
            logger.exception(e)        
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
            'date').to_dataframe(
            ['date', 'first', 'high', 'low', 'close', 'volume', 'value', 'yesterday', 'tomorrow'])
        df.rename(columns={"close": "Close", "first": "Open", "date": "Date", "high": "High", "low": "Low",
                           "volume": "Volume", "value": 'Value', "yesterday": "Yesterday", 'tomorrow': "Tomorrow"}, inplace=True)
        return df

    @cached_property
    def daily_history(self):
        df = self.raw_daily_history.copy()
        df['diff'] = df['Tomorrow'] / df['Yesterday'].shift(-1)
        if(df.shape[0] > 0):
            df.loc[df.shape[0] - 1, 'diff'] = 1
            assert(df.iloc[-1]['diff'] == 1)

            df['acc_diff'] = df['diff'][::-1].cumprod()[::-1]
            df['Close'] /= df['acc_diff']
            df['Open'] /= df['acc_diff']
            df['High'] /= df['acc_diff']
            df['Low'] /=  df['acc_diff']
            df['Tomorrow'] /= df['acc_diff']
            df['Yesterday'] /= df['acc_diff']

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

    first = models.IntegerField(null=False, blank=False)
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

def bulk_update(objects, fields, batch_size=None):
    for object in objects:
        object.save()

Share.objects.bulk_update = bulk_update
