from crawler.analyzers.analyzer import Analyzer


class CheapRightIssue(Analyzer):
    def analyze(self, share, daily_history, today_history):
        if share.is_rights_issue and daily_history.iloc[-1]['Tomorrow'] < 300:
            return {"cheap right Issue": {"price": daily_history.iloc[-1]['Tomorrow']}}
