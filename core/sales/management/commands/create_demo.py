"""Create a demo sale and demo users for the browser demo and scripts/smoke.py.

    docker compose exec web python manage.py create_demo --stock 100 --users 20
"""
from datetime import timedelta
from decimal import Decimal

from django.contrib.auth import get_user_model
from django.core.management.base import BaseCommand
from django.utils import timezone

from sales import services
from sales.models import FlashSale, Product


class Command(BaseCommand):
    help = "Create a sale (live in Redis unless it starts later) and users demo1..demoN."

    def add_arguments(self, parser):
        parser.add_argument("--name", default="Flash Phone X")
        parser.add_argument("--price", default="9999.00")
        parser.add_argument("--stock", type=int, default=100)
        parser.add_argument("--users", type=int, default=20)
        parser.add_argument("--password", default="demo12345")
        parser.add_argument("--starts-in", type=int, default=0, help="seconds from now; >0 shows a countdown")
        parser.add_argument("--minutes", type=int, default=60, help="how long the sale runs")

    def handle(self, *args, **opts):
        now = timezone.now()
        starts_at = now + timedelta(seconds=opts["starts_in"])
        product = Product.objects.create(name=opts["name"], price=Decimal(opts["price"]))
        sale = FlashSale.objects.create(
            product=product,
            starts_at=starts_at,
            ends_at=starts_at + timedelta(minutes=opts["minutes"]),
            total_stock=opts["stock"],
        )

        if opts["starts_in"] == 0:
            services.start_sale(sale)
            live = "live now"
        else:
            live = f"starts at {starts_at:%H:%M:%S} UTC: use the admin action 'Start sale' then"

        User = get_user_model()
        created = 0
        for i in range(1, opts["users"] + 1):
            user, was_created = User.objects.get_or_create(username=f"demo{i}")
            if was_created:
                user.set_password(opts["password"])
                user.save(update_fields=["password"])
                created += 1

        self.stdout.write(self.style.SUCCESS(f"Sale #{sale.id} '{product.name}' with {sale.total_stock} units ({live})."))
        self.stdout.write(f"Users demo1..demo{opts['users']} (password: {opts['password']}), {created} new.")
        self.stdout.write(f"Open http://localhost/#/sales/{sale.id}")
