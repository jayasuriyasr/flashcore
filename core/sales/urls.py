from django.urls import path
from rest_framework.routers import SimpleRouter

from .api import FlashSaleViewSet, MyOrdersView

router = SimpleRouter()
router.register("sales", FlashSaleViewSet, basename="sale")

urlpatterns = [
    path("orders/mine/", MyOrdersView.as_view(), name="my-orders"),
    *router.urls,
]
