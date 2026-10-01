from django.contrib import admin, messages

from . import redis_client as rc
from . import services
from .models import FlashSale, Order, Product


@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "price")
    search_fields = ("name",)


@admin.register(FlashSale)
class FlashSaleAdmin(admin.ModelAdmin):
    list_display = ("id", "product", "starts_at", "ends_at", "total_stock", "remaining", "redis_stock")
    list_select_related = ("product",)
    actions = ["start_sale", "end_sale"]

    @admin.display(description="Redis stock (live)")
    def redis_stock(self, sale):
        value = rc.get_stock(sale.id)
        return "not live" if value is None else value

    @admin.action(description="Start sale: load stock into Redis")
    def start_sale(self, request, queryset):
        for sale in queryset:
            if services.start_sale(sale):
                self.message_user(request, f"Sale #{sale.id} is live with {sale.remaining} units.", messages.SUCCESS)
            else:
                self.message_user(request, f"Sale #{sale.id} is already live; nothing changed.", messages.WARNING)

    @admin.action(description="End sale: stop accepting buys")
    def end_sale(self, request, queryset):
        for sale in queryset:
            services.end_sale(sale)
            self.message_user(request, f"Sale #{sale.id} ended. Start it again to resync Redis from the DB.")


@admin.register(Order)
class OrderAdmin(admin.ModelAdmin):
    list_display = ("id", "sale", "user", "status", "created_at", "updated_at")
    list_filter = ("status", "sale")
    list_select_related = ("sale__product", "user")
    readonly_fields = ("id", "created_at", "updated_at")
    search_fields = ("id", "user__username")
