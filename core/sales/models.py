import uuid

from django.conf import settings
from django.db import models
from django.db.models import Q
from django.db.models.functions import Now


class Product(models.Model):
    name = models.CharField(max_length=200)
    price = models.DecimalField(max_digits=10, decimal_places=2)

    class Meta:
        db_table = "products"

    def __str__(self):
        return self.name


class FlashSale(models.Model):
    product = models.ForeignKey(Product, on_delete=models.PROTECT, related_name="sales")
    starts_at = models.DateTimeField()
    ends_at = models.DateTimeField()
    total_stock = models.PositiveIntegerField()
    # Left blank in the admin → defaults to total_stock on create.
    remaining = models.IntegerField(blank=True)

    class Meta:
        db_table = "flash_sales"
        constraints = [
            # Last line of defence against overselling: the DB itself refuses negative stock.
            models.CheckConstraint(condition=Q(remaining__gte=0), name="flash_sale_remaining_gte_0"),
        ]

    def save(self, *args, **kwargs):
        if self.remaining is None:
            self.remaining = self.total_stock
        super().save(*args, **kwargs)

    def __str__(self):
        return f"Sale #{self.pk} — {self.product}"


class Order(models.Model):
    class Status(models.TextChoices):
        PENDING = "PENDING"
        CONFIRMED = "CONFIRMED"
        FAILED = "FAILED"

    # The gateway generates the id (uuid4) before the order exists in the DB,
    # so a Kafka redelivery of the same event hits ON CONFLICT and is skipped.
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    sale = models.ForeignKey(FlashSale, on_delete=models.PROTECT, related_name="orders")
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.PROTECT, related_name="orders")
    status = models.CharField(max_length=10, choices=Status.choices, default=Status.PENDING)
    # db_default so the order writer's raw-SQL batch INSERTs don't have to send timestamps.
    created_at = models.DateTimeField(db_default=Now())
    updated_at = models.DateTimeField(db_default=Now())

    class Meta:
        db_table = "orders"
        constraints = [
            # One live order per user per sale. Partial, so a user whose payment FAILED can buy again.
            models.UniqueConstraint(
                fields=["sale", "user"],
                condition=~Q(status="FAILED"),
                name="one_live_order_per_user_per_sale",
            ),
        ]
        indexes = [
            models.Index(fields=["sale", "status"], name="orders_sale_status_idx"),
            models.Index(fields=["status", "created_at"], name="orders_status_created_idx"),
        ]

    def __str__(self):
        return f"{self.id} ({self.status})"
