from crawler.analyzers.analyzer import Analyzer


class CheapRightIssue(Analyzer):
    def analyze(self, share):
        if share.is_rights_issue and share.last_day_history['Tomorrow'] < 300:
            return {"cheap right issue": {"price": share.last_day_history['Tomorrow']}}
