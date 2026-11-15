from django.contrib import admin
from django.db import connection
from django.http import HttpResponse, JsonResponse
from django.urls import include, path
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

from sales.metrics import SalesCollector

_metrics = CollectorRegistry()
_metrics.register(SalesCollector())


def health(request):
    connection.ensure_connection()  # raises (→ 500) if PostgreSQL is unreachable
    return JsonResponse({"status": "ok"})


def metrics(request):
    # Scraped by Prometheus over the compose network; nginx doesn't route /metrics/.
    return HttpResponse(generate_latest(_metrics), content_type=CONTENT_TYPE_LATEST)


urlpatterns = [
    path("health/", health),
    path("metrics/", metrics),
    path("admin/", admin.site.urls),
    path("api/auth/token/", TokenObtainPairView.as_view(), name="token_obtain_pair"),
    path("api/auth/token/refresh/", TokenRefreshView.as_view(), name="token_refresh"),
    path("api/", include("sales.urls")),
]
