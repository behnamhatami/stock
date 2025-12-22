from django.contrib import admin

from .models import Share, DailyHistory, ShareGroup, Contract


@admin.register(Contract)
class ContractAdmin(admin.ModelAdmin):
    list_display = ('code', 'description', 'commodity_name')
    readonly_fields = ('id', 'code', 'description', 'size', 'commodity_id', 'commodity_name')
    search_fields = ('code', 'description', 'commodity_name')


@admin.register(Share)
class ShareAdmin(admin.ModelAdmin):
    list_display = ('ticker', 'description', 'eps', 'last_update')
    readonly_fields = ('id', 'ticker', 'description', 'eps', 'last_update')
    list_filter = ('group',)
    search_fields = ('ticker', 'description')


@admin.register(DailyHistory)
class ShareDailyHistoryAdmin(admin.ModelAdmin):
    list_display = ('asset', 'date', 'close', 'volume', 'count', 'value')
    list_filter = ('date',)
    search_fields = ('share__ticker', 'contract__code', 'share__description', 'contract__description')


@admin.register(ShareGroup)
class ShareGroupAdmin(admin.ModelAdmin):
    list_display = ('name',)
    readonly_fields = ('id', 'name')
    search_fields = ('name',)
