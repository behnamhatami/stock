from django.core import management
from django_cron import CronJobBase, Schedule


class DailyAnalyzerCronJob(CronJobBase):
    RUN_AT_TIMES = ['5:00', '7:00']
    RETRY_AFTER_FAILURE_MINS = 5

    schedule = Schedule(run_at_times=RUN_AT_TIMES, retry_after_failure_mins=RETRY_AFTER_FAILURE_MINS)
    code = 'crawler.daily_analyzer_cron_job'  # a unique code

    def do(self):
        management.call_command("update_share_list")
        management.call_command("update_share_list_by_search")
        management.call_command("update_share_history")
        management.call_command("daily_analyze", days=1)
