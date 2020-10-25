import logging

from django.core import management
from django_cron import CronJobBase, Schedule

logger = logging.getLogger(__name__)


class UpdateShareListBySearchCronJob(CronJobBase):
    RUN_AT_TIMES = ['6:00', '18:00']
    RETRY_AFTER_FAILURE_MINS = 5

    schedule = Schedule(run_at_times=RUN_AT_TIMES, retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS)
    code = 'crawler.daily_update_share_list_by_search_cron_job'  # a unique code

    def do(self):
        try:
            management.call_command("update_share_list_by_search")
        except Exception as e:
            logger.exception(e)
            raise
