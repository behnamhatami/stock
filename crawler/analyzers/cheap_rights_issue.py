from crawler.analyzers.analyzer import Analyzer


class CheapRightIssue(Analyzer):
    def analyze(self, share):
        if share.is_rights_issue and share.daily_history.iloc[-1]['Tomorrow'] < 300:
            return "{} is a cheap hagh taghadom with price {}".format(share, share.daily_history.iloc[-1]['Tomorrow'])
