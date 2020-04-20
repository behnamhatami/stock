from django_cron import CronJobBase, Schedule
from django.core import management


class DailyCronJob(CronJobBase):
    RUN_AT_TIMES = ['01:00', ]

    schedule = Schedule(run_at_times=RUN_AT_TIMES)
    code = 'crawler.daily_analyzer'  # a unique code

    def do(self):
        management.call_command("update_share_list")
        management.call_command("update_share_history")
        management.call_command("daily_analyze")

