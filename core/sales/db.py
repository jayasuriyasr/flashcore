from contextlib import contextmanager

from django.db import connections, transaction


@contextmanager
def repeatable_read(using="default"):
    """Run the block as one REPEATABLE READ transaction.

    Every read inside sees the same snapshot (no phantom reads in PostgreSQL), and a
    concurrent update to a row we also update raises SQLSTATE 40001 instead of silently
    losing an update. It must be the outermost transaction: SET TRANSACTION has to be
    the first statement.
    """
    conn = connections[using]
    if conn.in_atomic_block:
        raise RuntimeError("repeatable_read() must be the outermost transaction")
    with transaction.atomic(using=using):
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        yield conn
