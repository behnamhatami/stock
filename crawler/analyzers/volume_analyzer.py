from crawler.analyzers.analyzer import Analyzer
from crawler.models import Share


class VolumeAnalyzer(Analyzer):
    def __init__(self, threshold=10):
        self.threshold = threshold

    def analyze(self, share: Share, day_offset: int):
        if share.history_size(day_offset) < self.threshold + 1 or (
                share.base_share and not (share.is_sell_option or share.is_buy_option or share.is_rights_issue)):
            return None

        last_month_volume = share.daily_history(day_offset)[-self.threshold - 1: -1]['volume'].mean()
        last_day_volume = share.last_day_history(day_offset)['volume']

        if last_month_volume * 2 < last_day_volume and share.last_day_history(day_offset)['value'] > 1_000_000_000:
            return {"high volume": {"last_month_volume": last_month_volume, "last_day_volume": last_day_volume}}
