"""Deployment settings (docs/DEPLOY.md): the React app on Vercel calls this API from another origin."""
import pytest
from django.test import override_settings

from core.settings import _csv

FRONTEND = "https://flashcore.vercel.app"


@override_settings(CORS_ALLOWED_ORIGINS=[FRONTEND])
def test_login_preflight_from_the_frontend_is_allowed(client):
    res = client.options(
        "/api/auth/token/",
        HTTP_ORIGIN=FRONTEND,
        HTTP_ACCESS_CONTROL_REQUEST_METHOD="POST",
        HTTP_ACCESS_CONTROL_REQUEST_HEADERS="content-type",
    )
    assert res.status_code == 200
    assert res["Access-Control-Allow-Origin"] == FRONTEND
    assert "content-type" in res["Access-Control-Allow-Headers"]


@pytest.mark.django_db
@override_settings(CORS_ALLOWED_ORIGINS=[FRONTEND])
def test_only_the_configured_origin_gets_cors_headers(client):
    assert client.get("/api/sales/", HTTP_ORIGIN=FRONTEND)["Access-Control-Allow-Origin"] == FRONTEND
    assert "Access-Control-Allow-Origin" not in client.get("/api/sales/", HTTP_ORIGIN="https://evil.example")


def test_empty_env_var_means_default_not_nothing(monkeypatch):
    # An empty DJANGO_ALLOWED_HOSTS= line in .env must not turn into ALLOWED_HOSTS = [] (every request 400).
    monkeypatch.setenv("DJANGO_ALLOWED_HOSTS", "")
    assert _csv("DJANGO_ALLOWED_HOSTS", "*") == ["*"]
    monkeypatch.setenv("DJANGO_ALLOWED_HOSTS", "1-2-3-4.sslip.io, localhost")
    assert _csv("DJANGO_ALLOWED_HOSTS", "*") == ["1-2-3-4.sslip.io", "localhost"]
