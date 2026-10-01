import uuid

import psycopg
import pytest
from django.db import OperationalError, connection, transaction

from sales.db import run_repeatable_read

pytestmark = pytest.mark.django_db(transaction=True)


def serialization_failure():
    exc = OperationalError("could not serialize access due to concurrent update")
    exc.__cause__ = psycopg.errors.SerializationFailure("40001")
    return exc


def test_retries_serialization_failures_then_succeeds():
    calls = []

    def fn():
        calls.append(1)
        if len(calls) < 3:
            raise serialization_failure()
        return "done"

    assert run_repeatable_read(fn) == "done"
    assert len(calls) == 3


def test_gives_up_after_max_attempts():
    calls = []

    def fn():
        calls.append(1)
        raise serialization_failure()

    with pytest.raises(OperationalError):
        run_repeatable_read(fn, attempts=5)
    assert len(calls) == 5


def test_other_errors_are_not_retried():
    calls = []

    def fn():
        calls.append(1)
        raise OperationalError("connection refused")

    with pytest.raises(OperationalError):
        run_repeatable_read(fn)
    assert len(calls) == 1


def test_concurrent_update_raises_40001_and_retry_succeeds(make_sale, other_db):
    sale = make_sale(total_stock=100)
    attempts = []

    def decrement():
        attempts.append(1)
        with connection.cursor() as cur:
            cur.execute("SELECT remaining FROM flash_sales WHERE id = %s", [sale.id])  # snapshot taken here
            if len(attempts) == 1:
                # Another session updates the same row and commits while we hold our snapshot.
                other_db.execute("UPDATE flash_sales SET remaining = remaining - 1 WHERE id = %s", [sale.id])
            # Under REPEATABLE READ this UPDATE on a row changed after our snapshot fails with 40001.
            cur.execute("UPDATE flash_sales SET remaining = remaining - 1 WHERE id = %s", [sale.id])

    run_repeatable_read(decrement)

    assert len(attempts) == 2  # first attempt hit 40001, second succeeded
    sale.refresh_from_db()
    assert sale.remaining == 98  # both decrements applied, none lost


def _two_reads_with_commit_between(level, sale, user_id, other_db):
    """Read remaining, let another session commit a sale, then count orders — in one transaction."""
    with transaction.atomic():
        with connection.cursor() as cur:
            cur.execute(f"SET TRANSACTION ISOLATION LEVEL {level}")
            cur.execute("SELECT remaining FROM flash_sales WHERE id = %s", [sale.id])
            remaining = cur.fetchone()[0]

            with other_db.transaction():
                other_db.execute(
                    "INSERT INTO orders (id, sale_id, user_id, status) VALUES (%s, %s, %s, 'PENDING')",
                    [uuid.uuid4(), sale.id, user_id],
                )
                other_db.execute("UPDATE flash_sales SET remaining = remaining - 1 WHERE id = %s", [sale.id])

            cur.execute("SELECT count(*) FROM orders WHERE sale_id = %s AND status <> 'FAILED'", [sale.id])
            sold = cur.fetchone()[0]
    return remaining, sold


def test_phantom_read_under_read_committed_but_not_repeatable_read(make_sale, make_users, other_db):
    """A phantom read: the same transaction runs a query twice (or two related queries) and
    rows committed by someone else in between show up in the second one. The invariant check
    `remaining + sold == total` then reports a false violation.

    READ COMMITTED takes a new snapshot per statement → it sees the phantom order.
    REPEATABLE READ uses one snapshot for the whole transaction → both reads agree.
    """
    sale = make_sale(total_stock=10)
    first, second = make_users(2)

    remaining, sold = _two_reads_with_commit_between("READ COMMITTED", sale, first.id, other_db)
    assert (remaining, sold) == (10, 1)
    assert remaining + sold != sale.total_stock  # phantom: looks like a unit appeared from nowhere

    remaining, sold = _two_reads_with_commit_between("REPEATABLE READ", sale, second.id, other_db)
    assert (remaining, sold) == (9, 1)  # the order committed mid-transaction is invisible
    assert remaining + sold == sale.total_stock  # consistent snapshot
