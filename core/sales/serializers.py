from rest_framework import serializers

from .models import FlashSale, Order


class FlashSaleSerializer(serializers.ModelSerializer):
    product_name = serializers.CharField(source="product.name", read_only=True)
    price = serializers.DecimalField(source="product.price", max_digits=10, decimal_places=2, read_only=True)

    class Meta:
        model = FlashSale
        fields = ("id", "product_name", "price", "starts_at", "ends_at", "total_stock", "remaining")


class OrderSerializer(serializers.ModelSerializer):
    class Meta:
        model = Order
        fields = ("id", "sale", "status", "created_at", "updated_at")
