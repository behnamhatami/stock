from datetime import timedelta, date

from django.db import models

# Create your models here.
from django.utils.functional import cached_property
from django_pandas.managers import DataFrameManager


class Share(models.Model):
    DAY_OFFSET = 0

    def get_today():
        return date.today() - timedelta(days=Share.DAY_OFFSET)

    id = models.BigIntegerField(null=False, blank=False, primary_key=True)
    ticker = models.CharField(unique=True, max_length=256)
    description = models.CharField(max_length=256)

    eps = models.IntegerField(null=True, blank=False)
    last_update = models.DateTimeField(null=True)

    @cached_property
    def is_rights_issue(self):
        return self.ticker[-1] == 'ح'

    @cached_property
    def is_buy_option(self):
        return self.ticker[0] == 'ض'

    @cached_property
    def is_sell_option(self):
        return self.ticker[0] == 'ه'

    @cached_property
    def is_special(self):
        return self.is_rights_issue or self.is_buy_option or self.is_sell_option

    @cached_property
    def raw_daily_history(self):
        df = self.history.filter(
            date__lt=Share.get_today()).all().order_by(
            'date').to_dataframe(
            ['date', 'first', 'high', 'low', 'close', 'volume', 'yesterday', 'tomorrow'])
        df.rename(columns={"close": "Close", "first": "Open", "date": "Date", "high": "High", "low": "Low",
                           "volume": "Volume", "yesterday": "Yesterday", 'tomorrow': "Tomorrow"}, inplace=True)
        return df

    @cached_property
    def daily_history(self):
        df = self.raw_daily_history.copy()
        df['diff'] = df['Tomorrow'] / df['Yesterday'].shift(-1)
        if(df.shape[0] > 0):
            df['diff'].iloc[-1] = 1

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
