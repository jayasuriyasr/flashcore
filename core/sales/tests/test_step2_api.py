import os

import jwt
import pytest

from sales.models import Order

from .conftest import PASSWORD

pytestmark = pytest.mark.django_db


def obtain(api, username, password=PASSWORD):
    return api.post("/api/auth/token/", {"username": username, "password": password}, format="json")


def test_token_is_hs256_signed_with_shared_key(api, make_user):
    user = make_user()
    resp = obtain(api, user.username)
    assert resp.status_code == 200
    # Decode exactly the way the FastAPI gateway will: shared env key, HS256.
    payload = jwt.decode(resp.data["access"], os.environ["JWT_SIGNING_KEY"], algorithms=["HS256"])
    assert int(payload["user_id"]) == user.id
    assert "exp" in payload


def test_wrong_password_is_401(api, make_user):
    user = make_user()
    assert obtain(api, user.username, "wrong-password").status_code == 401


def test_sales_list_is_public(api, make_sale):
    make_sale(total_stock=10, name="Watch")
    resp = api.get("/api/sales/")
    assert resp.status_code == 200
    assert resp.data[0]["product_name"] == "Watch"
    assert resp.data[0]["remaining"] == 10


def test_sale_detail_and_404(api, make_sale):
    sale = make_sale()
    assert api.get(f"/api/sales/{sale.id}/").status_code == 200
    assert api.get("/api/sales/999999/").status_code == 404


def test_my_orders_requires_token(api):
    assert api.get("/api/orders/mine/").status_code == 401


def test_my_orders_returns_only_my_orders(api, make_user, make_sale):
    alice, bob = make_user(), make_user()
    sale = make_sale()
    mine = Order.objects.create(sale=sale, user=alice)
    Order.objects.create(sale=sale, user=bob)

    token = obtain(api, alice.username).data["access"]
    api.credentials(HTTP_AUTHORIZATION=f"Bearer {token}")
    resp = api.get("/api/orders/mine/")

    assert resp.status_code == 200
    assert [o["id"] for o in resp.data] == [str(mine.id)]
