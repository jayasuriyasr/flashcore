"""Verify one sale after a load or chaos run, against the running stack.

    docker compose exec web python manage.py verify_sale --sale-id 7 --accepted 27000 --errors 12 --wait 300
"""
import time
from functools import partial

from django.core.management.base import BaseCommand, CommandError
from django.db import connection

from sales import redis_client as rc
from sales.db import run_repeatable_read
from sales.models import FlashSale, Order
from sales.tasks import _snapshot


class Command(BaseCommand):
    help = "Wait for the pipeline to drain, then check orders, invariant, duplicates, payments and Redis."

    def add_arguments(self, parser):
        parser.add_argument("--sale-id", type=int, required=True)
        parser.add_argument("--accepted", type=int, help="number of 202 responses the load tool saw")
        parser.add_argument("--errors", type=int, default=0,
                            help="503s/timeouts: a timed-out produce may still have been stored by Kafka")
        parser.add_argument("--expect-sold", type=int, help="exact number of non-FAILED orders expected")
        parser.add_argument("--sold-out", action="store_true",
                            help="hot-item run: far more buyers than stock, so every unit must be sold or, if its "
                                 "payment failed after the rush, back on sale; never more sold than stock")
        parser.add_argument("--wait", type=float, default=300, help="seconds to wait for PENDING orders to finish")
        parser.add_argument("--baseline", action="store_true",
                            help="sale bought through the PostgreSQL-only baseline: orders are written synchronously "
                                 "and never paid or put in Redis, so only the order, stock and duplicate checks apply")

    def handle(self, *args, **opts):
        sale_id, accepted = opts["sale_id"], opts["accepted"]
        if not FlashSale.objects.filter(pk=sale_id).exists():
            raise CommandError(f"sale {sale_id} not found")
        orders = Order.objects.filter(sale_id=sale_id)

        started = time.monotonic()
        while True:
            total = orders.count()
            pending = orders.filter(status=Order.Status.PENDING).count()
            caught_up = accepted is None or total >= accepted
            if opts["baseline"] or (pending == 0 and caught_up) or time.monotonic() - started > opts["wait"]:
                break
            self.stdout.write(f"  waiting: {total} orders stored, {pending} pending ...")
            time.sleep(5)
        drain_s = time.monotonic() - started

        checks = []

        def check(name, ok, detail):
            checks.append((name, ok, detail))

        if not opts["baseline"]:
            check("pipeline drained", pending == 0 and caught_up,
                  f"{total} orders, {pending} pending after {drain_s:.0f}s")
        if accepted is not None:
            extra = total - accepted
            check("orders stored == 202s", 0 <= extra <= opts["errors"],
                  f"{total} stored vs {accepted} accepted" + (f" (+{extra} from timed-out requests)" if extra > 0 else ""))

        total_stock, remaining, sold = run_repeatable_read(partial(_snapshot, sale_id))
        check("invariant remaining + sold == total", remaining >= 0 and remaining + sold == total_stock,
              f"{remaining} + {sold} vs {total_stock}")
        if opts["expect_sold"] is not None:
            check("sold == expected", sold == opts["expect_sold"], f"{sold} vs {opts['expect_sold']}")
        if opts["sold_out"]:
            failed = orders.filter(status=Order.Status.FAILED).count()
            # A unit whose payment fails goes back on sale. During the rush it is bought again at once;
            # if fulfilment finishes after the buyers have gone it stays in stock. Both are correct.
            check("no oversell: sold <= stock", sold <= total_stock,
                  f"{sold} of {total_stock} sold, {remaining} back on sale ({failed} payments failed)")
            check("every unit is sold or on sale", remaining <= failed, f"{remaining} unsold vs {failed} failed payments")

        with connection.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM (SELECT user_id FROM orders WHERE sale_id = %s AND status <> 'FAILED' "
                "GROUP BY user_id HAVING count(*) > 1) d", [sale_id])
            dupes = cur.fetchone()[0]
        check("no duplicate live orders per user", dupes == 0, f"{dupes} users with >1 live order")

        if opts["baseline"]:
            return self._report(checks)

        ids = [str(i) for i in orders.values_list("id", flat=True)]
        worst, charged_twice = 0, 0
        r = rc.client()
        for i in range(0, len(ids), 1000):
            values = r.mget([f"payments:{oid}" for oid in ids[i:i + 1000]])
            for v in values:
                n = int(v or 0)
                worst = max(worst, n)
                charged_twice += n > 1
        check("nobody charged twice", charged_twice == 0, f"max payment attempts per order = {worst}")

        redis_stock = rc.get_stock(sale_id)
        if redis_stock is None:
            check("redis <= db remaining", True, "sale not live in Redis")
        else:
            check("redis <= db remaining (safe direction)", redis_stock <= remaining,
                  f"redis {redis_stock} vs db {remaining} (diff {remaining - redis_stock})")
        self._report(checks)

    def _report(self, checks):
        self.stdout.write("")
        for name, ok, detail in checks:
            self.stdout.write(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")
        if not all(ok for _, ok, _ in checks):
            raise CommandError("verification failed")
        self.stdout.write(self.style.SUCCESS("ALL CHECKS PASSED"))
