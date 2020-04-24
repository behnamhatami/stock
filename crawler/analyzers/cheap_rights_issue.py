from crawler.analyzers.analyzer import Analyzer


class CheapRightIssueAnalyzer(Analyzer):
    def analyze(self, share):
        if share.is_rights_issue and share.last_day_history['tomorrow'] < 300:
            return {"cheap right issue": {"price": share.last_day_history['tomorrow']}}
