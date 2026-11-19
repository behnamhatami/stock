# (share, date) / (contract, date) instead of (date, share): per-share history queries use the index
# for both the filter and the ORDER BY date.

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('crawler', '0017_remove_share_extra_data'),
    ]

    operations = [
        migrations.RemoveConstraint(
            model_name='dailyhistory',
            name='daily_history_date_share_unique',
        ),
        migrations.RemoveConstraint(
            model_name='dailyhistory',
            name='daily_history_date_contract_unique',
        ),
        migrations.AddConstraint(
            model_name='dailyhistory',
            constraint=models.UniqueConstraint(condition=models.Q(('share__isnull', False)), fields=('share', 'date'), name='daily_history_share_date_unique'),
        ),
        migrations.AddConstraint(
            model_name='dailyhistory',
            constraint=models.UniqueConstraint(condition=models.Q(('contract__isnull', False)), fields=('contract', 'date'), name='daily_history_contract_date_unique'),
        ),
    ]
