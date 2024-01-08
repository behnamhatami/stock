from crawler.analyzers.analyzer import Analyzer
from crawler.models import Share


class CheapRightIssueAnalyzer(Analyzer):
    def analyze(self, share: Share, day_offset: int):
        if share.is_rights_issue and share.last_day_history(day_offset)['close'] < 300:
            return {"cheap right issue": {"price": share.last_day_history(day_offset)['close']}}
