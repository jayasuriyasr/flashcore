import pytest

from sales import redis_client as rc
from sales import services
from sales.models import Order

pytestmark = pytest.mark.django_db


def test_start_sale_loads_db_remaining_into_redis(make_sale, redis_db):
    sale = make_sale(total_stock=100)
    assert services.start_sale(sale) is True
    assert redis_db.get(rc.stock_key(sale.id)) == "100"


def test_start_sale_twice_does_not_reset_a_live_sale(make_sale, redis_db):
    sale = make_sale(total_stock=100)
    services.start_sale(sale)
    redis_db.decr(rc.stock_key(sale.id))  # someone bought a unit
    redis_db.sadd(rc.buyers_key(sale.id), 12345)

    assert services.start_sale(sale) is False  # SET NX
    assert redis_db.get(rc.stock_key(sale.id)) == "99"
    assert redis_db.sismember(rc.buyers_key(sale.id), 12345)


def test_start_sale_rebuilds_buyers_from_live_orders(make_sale, make_user, redis_db):
    sale = make_sale(total_stock=10)
    pending, confirmed, failed = make_user(), make_user(), make_user()
    Order.objects.create(sale=sale, user=pending, status=Order.Status.PENDING)
    Order.objects.create(sale=sale, user=confirmed, status=Order.Status.CONFIRMED)
    Order.objects.create(sale=sale, user=failed, status=Order.Status.FAILED)
    redis_db.sadd(rc.buyers_key(sale.id), 999)  # stale member from an earlier run

    services.start_sale(sale)

    assert redis_db.smembers(rc.buyers_key(sale.id)) == {str(pending.id), str(confirmed.id)}


def test_start_sale_sets_stock_and_buyers_in_one_step(redis_db):
    # start_sale.lua: a buyer can never reserve between "stock loaded" and "buyers rebuilt"
    # (that gap used to wipe their reservation from the set and let them reserve twice).
    buyers = list(range(1, 20_001))
    assert rc.start_sale(424242, 7, buyers) is True
    assert redis_db.get(rc.stock_key(424242)) == "7"
    assert redis_db.scard(rc.buyers_key(424242)) == 20_000
    assert rc.start_sale(424242, 99, []) is False  # live: untouched
    assert redis_db.get(rc.stock_key(424242)) == "7"
    assert redis_db.scard(rc.buyers_key(424242)) == 20_000


def test_end_sale_removes_stock_key(make_sale, redis_db):
    sale = make_sale(total_stock=10)
    services.start_sale(sale)
    assert services.end_sale(sale) is True
    assert redis_db.get(rc.stock_key(sale.id)) is None
    assert rc.get_stock(sale.id) is None


def test_end_then_start_resyncs_redis_to_db(make_sale, redis_db):
    sale = make_sale(total_stock=10, remaining=7)
    services.start_sale(sale)
    redis_db.set(rc.stock_key(sale.id), 3)  # drifted
    services.end_sale(sale)
    services.start_sale(sale)
    assert redis_db.get(rc.stock_key(sale.id)) == "7"


def test_admin_start_and_end_actions(admin_client, make_sale, redis_db):
    sale = make_sale(total_stock=25)
    url = "/admin/sales/flashsale/"

    resp = admin_client.post(url, {"action": "start_sale", "_selected_action": [sale.id]}, follow=True)
    assert resp.status_code == 200
    assert redis_db.get(rc.stock_key(sale.id)) == "25"

    admin_client.post(url, {"action": "end_sale", "_selected_action": [sale.id]}, follow=True)
    assert redis_db.get(rc.stock_key(sale.id)) is None


def test_admin_changelist_shows_live_redis_stock(admin_client, make_sale, redis_db):
    sale = make_sale(total_stock=25)
    services.start_sale(sale)
    resp = admin_client.get("/admin/sales/flashsale/")
    assert resp.status_code == 200
    assert b"not live" not in resp.content
