import logging

from django.core import management
from django_cron import CronJobBase, Schedule

logger = logging.getLogger(__name__)


class UpdateShareInfoCronJob(CronJobBase):
    RUN_ON_DAYS = [4]
    RUN_AT_TIMES = ['06:00']
    RETRY_AFTER_FAILURE_MINS = 60

    schedule = Schedule(run_on_days=RUN_ON_DAYS, run_at_times=RUN_AT_TIMES,
                        retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS)
    code = 'crawler.daily_update_share_info_cron_job'  # a unique code

    def do(self):
        try:
            management.call_command("update_share_info")
        except Exception as e:
            logger.exception(e)
            raise
