from rest_framework import generics, permissions, viewsets

from .models import FlashSale, Order
from .serializers import FlashSaleSerializer, OrderSerializer


class FlashSaleViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = FlashSale.objects.select_related("product").order_by("starts_at")
    serializer_class = FlashSaleSerializer


class MyOrdersView(generics.ListAPIView):
    serializer_class = OrderSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        return Order.objects.filter(user=self.request.user).order_by("-created_at")
