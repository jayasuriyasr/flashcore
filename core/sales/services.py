from . import redis_client as rc
from .models import FlashSale, Order


def start_sale(sale: FlashSale) -> bool:
    """Make a sale live: Redis stock = DB remaining, buyers = users with a live order.

    Rebuilding the buyers set from the DB means a sale can be ended and started again
    (e.g. to resync Redis with PostgreSQL) without letting anyone buy twice.
    Returns False if the sale was already live (nothing is changed).
    """
    sale.refresh_from_db(fields=["remaining"])
    buyers = list(
        Order.objects.filter(sale=sale)
        .exclude(status=Order.Status.FAILED)
        .values_list("user_id", flat=True)
    )
    return rc.start_sale(sale.id, sale.remaining, buyers)


def end_sale(sale: FlashSale) -> bool:
    return rc.end_sale(sale.id)
