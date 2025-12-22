import logging

from django.core import management
from django_cron import CronJobBase, Schedule

logger = logging.getLogger(__name__)


class UpdateShareHistoryCronJob(CronJobBase):
    RUN_AT_TIMES = ['18:00', '22:00', '02:00', '06:00', '08:00']
    RETRY_AFTER_FAILURE_MINS = 30

    schedule = Schedule(run_at_times=RUN_AT_TIMES, retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS)
    code = 'crawler.daily_update_share_history_cron_job'  # a unique code

    def do(self):
        try:
            management.call_command("update_share_history")
        except Exception as e:
            logger.exception(e)
            raise
