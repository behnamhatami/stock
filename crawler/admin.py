from django.contrib import admin

from .models import Share, ShareDailyHistory, ShareGroup


@admin.register(Share)
class ShareAdmin(admin.ModelAdmin):
    list_display = ('ticker', 'description', 'eps', 'last_update')
    readonly_fields = ('id', 'ticker', 'description', 'eps', 'last_update')
    list_filter = ('group',)
    search_fields = ('ticker', 'description')


@admin.register(ShareDailyHistory)
class ShareDailyHistoryAdmin(admin.ModelAdmin):
    list_display = ('share', 'date', 'tomorrow', 'volume', 'count', 'value')
    list_filter = ('date',)
    search_fields = ('share__ticker', 'share__description')


@admin.register(ShareGroup)
class ShareGroupAdmin(admin.ModelAdmin):
    list_display = ('name',)
    readonly_fields = ('id', 'name')
    search_fields = ('name',)
