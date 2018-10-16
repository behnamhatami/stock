from datetime import timedelta, date

from django.db import models

# Create your models here.
from django.utils.functional import cached_property
from django_pandas.managers import DataFrameManager


class Share(models.Model):
    DAY_OFFSET = 0

    id = models.BigIntegerField(null=False, blank=False, primary_key=True)
    ticker = models.CharField(max_length=256)
    description = models.CharField(max_length=256)

    eps = models.IntegerField(null=True, blank=False)
    last_update = models.DateTimeField(null=True)

    @cached_property
    def is_rights_issue(self):
        return self.ticker[-1] == 'ح'

    @cached_property
    def daily_history(self):
        df = self.history.filter(
            date__lt=date.today() - timedelta(days=Share.DAY_OFFSET)).all().order_by(
            'date').to_dataframe(
            ['date', 'first', 'high', 'low', 'close', 'volume', 'yesterday', 'tomorrow'])
        df.rename(columns={"close": "Close", "first": "Open", "date": "Date", "high": "High", "low": "Low",
                           "volume": "Volume", "yesterday": "Yesterday", 'tomorrow': "Tomorrow"}, inplace=True)
        return df

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
