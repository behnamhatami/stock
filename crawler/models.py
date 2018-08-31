from django.db import models


# Create your models here.

class Share(models.Model):
    id = models.BigIntegerField(null=False, blank=False, primary_key=True)
    symbol = models.CharField(max_length=256)
    description = models.CharField(max_length=256)

    eps = models.IntegerField(null=True, blank=False)
    last_update = models.DateTimeField(null=True)


class ShareHistory(models.Model):
    share = models.ForeignKey(Share, null=False, blank=False, on_delete=models.CASCADE)
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

