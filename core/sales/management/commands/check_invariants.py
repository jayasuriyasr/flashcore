from django.core.management.base import BaseCommand, CommandError

from sales.db import repeatable_read


class Command(BaseCommand):
    help = "Verify, for every sale: remaining >= 0 and remaining + live orders == total_stock."

    def handle(self, *args, **options):
        # Two separate SELECTs on purpose. Under READ COMMITTED an order-writer commit landing
        # between them would make the numbers disagree (a phantom read); REPEATABLE READ
        # gives both queries the same snapshot.
        with repeatable_read() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, total_stock, remaining FROM flash_sales ORDER BY id")
                sales = cur.fetchall()
                cur.execute("SELECT sale_id, count(*) FROM orders WHERE status <> 'FAILED' GROUP BY sale_id")
                sold = dict(cur.fetchall())
                cur.execute(
                    "SELECT sale_id, user_id, count(*) FROM orders WHERE status <> 'FAILED' "
                    "GROUP BY sale_id, user_id HAVING count(*) > 1"
                )
                duplicates = cur.fetchall()

        bad = []
        self.stdout.write(f"{'sale':>6} {'total':>10} {'remaining':>10} {'sold':>10}  result")
        for sale_id, total, remaining in sales:
            s = sold.get(sale_id, 0)
            ok = remaining >= 0 and remaining + s == total
            self.stdout.write(f"{sale_id:>6} {total:>10} {remaining:>10} {s:>10}  {'OK' if ok else 'VIOLATION'}")
            if not ok:
                bad.append(sale_id)

        if duplicates:
            self.stdout.write(f"duplicate live orders (sale, user, count): {duplicates}")
        if bad or duplicates:
            raise CommandError(f"invariant violated — sales: {bad}, duplicate buyers: {len(duplicates)}")
        self.stdout.write(self.style.SUCCESS(f"OK: all {len(sales)} sale(s) consistent"))
