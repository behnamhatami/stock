from django.db import models

# Create your models here.
from django_pandas.managers import DataFrameManager


class Share(models.Model):
    id = models.BigIntegerField(null=False, blank=False, primary_key=True)
    ticker = models.CharField(max_length=256)
    description = models.CharField(max_length=256)

    eps = models.IntegerField(null=True, blank=False)
    last_update = models.DateTimeField(null=True)


class ShareHistory(models.Model):
    share = models.ForeignKey(Share, null=False, blank=False, on_delete=models.CASCADE, related_name="history")
    date = models.DateField(null=False, blank=False)

    first = models.IntegerField(null=False, blank=False)
    last = models.IntegerField(null=False, blank=False)
    close = models.IntegerField(null=False, blank=False)
    yesterday = models.IntegerField(null=False, blank=False)

    high = models.IntegerField(null=False, blank=False)
    low = models.IntegerField(null=False, blank=False)

    volume = models.BigIntegerField(null=False, blank=False)
    count = models.BigIntegerField(null=False, blank=False)
    value = models.BigIntegerField(null=False, blank=False)

    @staticmethod
    def get_historical_data(share):
        df = share.sharehistory_set.all().to_dataframe(['date', 'first', 'high', 'low', 'last', 'volume'],
                                                       index='date')
        df.rename(columns={"last": "Close", "first": "Open", "date": "Date", "high": "High", "low": "Low",
                           "volume": "Volume"}, inplace=True)
        return df

    objects = DataFrameManager()

    class Meta:
        unique_together = (("share", "date"),)
