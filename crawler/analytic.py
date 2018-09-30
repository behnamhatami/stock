import bulbea as bb

from crawler.helper import update_stock_history_item
from crawler.models import Share, ShareHistory


class IranShare(bb.Share):
    def update(self, start=None, end=None, latest=None, cache=False):
        '''
        Update the share with the latest available data.

        :Example:

        >>> import bulbea as bb
        >>> share = bb.Share(source = 'YAHOO', ticker = 'AAPL')
        >>> share.update()
        '''

        print(self.ticker)
        share = Share.objects.get(ticker=self.ticker)
        update_stock_history_item(share)

        self.data = ShareHistory.get_historical_data(share)
        self.length = len(self.data)
        self.attrs = list(self.data.columns)
