"""End-to-end check against the RUNNING stack (real DB, gateway, Kafka, order writer, Celery).

    docker compose exec web python manage.py smoke_test --buys 200
"""
import asyncio
import time
from datetime import timedelta
from decimal import Decimal
from io import StringIO

import httpx
from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from rest_framework_simplejwt.tokens import AccessToken

from sales import services
from sales.models import FlashSale, Order, Product


class Command(BaseCommand):
    help = "Buy through the gateway, wait for fulfilment, then check the invariants."

    def add_arguments(self, parser):
        parser.add_argument("--buys", type=int, default=200)
        parser.add_argument("--stock", type=int, default=1000)
        parser.add_argument("--gateway", default="http://gateway:8001")
        parser.add_argument("--timeout", type=float, default=30)

    def handle(self, *args, **opts):
        stamp = int(time.time())
        product = Product.objects.create(name=f"Smoke {stamp}", price=Decimal("1.00"))
        now = timezone.now()
        sale = FlashSale.objects.create(
            product=product, starts_at=now, ends_at=now + timedelta(hours=1), total_stock=opts["stock"]
        )
        services.start_sale(sale)

        User = get_user_model()
        users = User.objects.bulk_create(
            [User(username=f"smoke{stamp}_{i}", password="!") for i in range(opts["buys"])]
        )
        tokens = [str(AccessToken.for_user(u)) for u in users]

        codes = asyncio.run(self._buy_all(opts["gateway"], sale.id, tokens))
        accepted = codes.count(202)
        self.stdout.write(f"sale #{sale.id}: {len(codes)} buys -> " +
                          ", ".join(f"{c}: {codes.count(c)}" for c in sorted(set(codes))))

        deadline = time.monotonic() + opts["timeout"]
        while time.monotonic() < deadline:
            total = Order.objects.filter(sale=sale).count()
            pending = Order.objects.filter(sale=sale, status=Order.Status.PENDING).count()
            if total >= accepted and pending == 0:
                break
            time.sleep(0.5)
        total = Order.objects.filter(sale=sale).count()
        pending = Order.objects.filter(sale=sale, status=Order.Status.PENDING).count()
        confirmed = Order.objects.filter(sale=sale, status=Order.Status.CONFIRMED).count()
        failed = Order.objects.filter(sale=sale, status=Order.Status.FAILED).count()
        self.stdout.write(f"orders: {total} (confirmed {confirmed}, failed {failed}, pending {pending})")

        out = StringIO()
        call_command("check_invariants", stdout=out)
        self.stdout.write(out.getvalue().strip().splitlines()[-1])

        if total != accepted:
            raise CommandError(f"{accepted} buys accepted but {total} orders stored")
        if pending:
            raise CommandError(f"{pending} orders still PENDING after {opts['timeout']}s")
        self.stdout.write(self.style.SUCCESS("SMOKE TEST PASSED"))

    @staticmethod
    async def _buy_all(gateway, sale_id, tokens):
        limit = asyncio.Semaphore(100)
        async with httpx.AsyncClient(base_url=gateway, timeout=30) as client:
            async def buy(token):
                async with limit:
                    resp = await client.post(f"/api/sales/{sale_id}/buy", headers={"Authorization": f"Bearer {token}"})
                    return resp.status_code

            return await asyncio.gather(*(buy(t) for t in tokens))
