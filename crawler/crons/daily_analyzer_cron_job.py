import logging

from django.core import management
from django_cron import CronJobBase, Schedule

logger = logging.getLogger(__name__)


class DailyAnalyzerCronJob(CronJobBase):
    RUN_AT_TIMES = ['6:00', '7:00']
    RETRY_AFTER_FAILURE_MINS = 5

    schedule = Schedule(run_at_times=RUN_AT_TIMES, retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS)
    code = 'crawler.daily_analyzer_cron_job'  # a unique code

    def do(self):
        try:
            management.call_command("daily_analyze", days=1)
        except Exception as e:
            logger.exception(e)
            raise
