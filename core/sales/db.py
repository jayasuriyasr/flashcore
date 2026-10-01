import logging
import random
import time
from contextlib import contextmanager

from django.db import OperationalError, connections, transaction

log = logging.getLogger("sales.db")

# serialization_failure (concurrent update under REPEATABLE READ) and deadlock_detected:
# both mean "nothing was written, run the whole transaction again".
RETRYABLE_SQLSTATES = {"40001", "40P01"}


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


def is_retryable(exc: BaseException) -> bool:
    # Django wraps the psycopg error; the original (with .sqlstate) is the __cause__.
    return getattr(exc.__cause__, "sqlstate", None) in RETRYABLE_SQLSTATES


def run_repeatable_read(fn, attempts: int = 5, using: str = "default"):
    """Call fn() inside a REPEATABLE READ transaction, retrying the whole thing on 40001/40P01."""
    for attempt in range(1, attempts + 1):
        try:
            with repeatable_read(using):
                return fn()
        except OperationalError as exc:
            if attempt == attempts or not is_retryable(exc):
                raise
            log.info("serialization conflict, retrying transaction (attempt %d/%d)", attempt + 1, attempts)
            time.sleep(random.uniform(0, 0.005 * 2 ** attempt))  # jittered backoff
