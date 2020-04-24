from django.contrib import admin

from .models import Share, ShareDailyHistory


@admin.register(Share)
class ShareAdmin(admin.ModelAdmin):
    list_display = ('ticker', 'description', 'eps', 'last_update')
    readonly_fields = ('id', 'ticker', 'description', 'eps', 'last_update')
    search_fields = ('ticker', 'description')


@admin.register(ShareDailyHistory)
class ShareDailyHistoryAdmin(admin.ModelAdmin):
    list_display = ('share', 'date', 'tomorrow', 'volume', 'count', 'value')
    list_filter = ('date',)
    search_fields = ('share__ticker', 'share__description')
