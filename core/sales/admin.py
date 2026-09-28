from django.contrib import admin

from .models import FlashSale, Order, Product


@admin.register(Product)
class ProductAdmin(admin.ModelAdmin):
    list_display = ("id", "name", "price")
    search_fields = ("name",)


@admin.register(FlashSale)
class FlashSaleAdmin(admin.ModelAdmin):
    list_display = ("id", "product", "starts_at", "ends_at", "total_stock", "remaining")
    list_select_related = ("product",)


@admin.register(Order)
class OrderAdmin(admin.ModelAdmin):
    list_display = ("id", "sale", "user", "status", "created_at", "updated_at")
    list_filter = ("status", "sale")
    list_select_related = ("sale__product", "user")
    readonly_fields = ("id", "created_at", "updated_at")
    search_fields = ("id", "user__username")
