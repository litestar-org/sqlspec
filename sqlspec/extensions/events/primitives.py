"""Table-queue SQL generation and claim verification primitives."""

from datetime import datetime
from typing import Any

from sqlspec.extensions.events._payload import parse_event_timestamp

__all__ = ("claim_verified", "lock_clause", "row_limit_clause", "select_limit_prefix")


def lock_clause(*, select_for_update: bool, skip_locked: bool, dialect: str | None = None) -> str:
    """Render the row locking clause for candidate selection.

    Db2 (dialect name ``db2``) locks through the isolation clause
    ``WITH RS USE AND KEEP UPDATE LOCKS``, which is valid on a read-only cursor and so composes
    with ``ORDER BY`` and ``FETCH FIRST``; the selected row stays update-locked until commit.

    Args:
        select_for_update: Whether to lock selected rows with FOR UPDATE.
        skip_locked: Whether to skip already locked rows when locking is enabled.
        dialect: Optional SQL dialect identifier.

    Returns:
        Locking clause SQL fragment with leading space, or empty string.
    """
    if not select_for_update:
        return ""
    if dialect and dialect.lower() == "db2":
        if skip_locked:
            return " WITH RS USE AND KEEP UPDATE LOCKS SKIP LOCKED DATA"
        return " WITH RS USE AND KEEP UPDATE LOCKS"
    if skip_locked:
        return " FOR UPDATE SKIP LOCKED"
    return " FOR UPDATE"


def row_limit_clause(dialect: str, n: int) -> str:
    """Render trailing row limit clause for candidate selection.

    Args:
        dialect: SQL dialect identifier.
        n: Maximum number of rows to return.

    Returns:
        Trailing limit clause SQL fragment with leading space, or empty string.
    """
    normalized = dialect.lower()
    if normalized in {"mssql", "tsql"} or "sql server" in normalized:
        return ""
    if "oracle" in normalized or normalized == "db2":
        return f" FETCH FIRST {n} ROWS ONLY"
    return f" LIMIT {n}"


def select_limit_prefix(dialect: str, n: int) -> str:
    """Render leading SELECT limit prefix for dialects that limit before columns.

    Args:
        dialect: SQL dialect identifier.
        n: Maximum number of rows to return.

    Returns:
        Limit prefix clause with trailing space, or empty string.
    """
    normalized = dialect.lower()
    if normalized in {"mssql", "tsql"} or "sql server" in normalized:
        return f"TOP {n} "
    return ""


def claim_verified(row: dict[str, Any] | None, leased_until: datetime) -> bool:
    """Confirm claim ownership by matching the stored lease against the claimer's token.

    Drivers that cannot report rows affected return zero for a successful
    claim UPDATE, so a zero rowcount alone cannot distinguish a won claim
    from a lost race. The persisted ``lease_expires_at`` value identifies
    the winning claimer.

    Args:
        row: Row dictionary retrieved from the table queue, or None.
        leased_until: Expiration timestamp expected for the winning claim.

    Returns:
        True if the row contains a lease matching leased_until, False otherwise.
    """
    if row is None:
        return False
    lease_value = row.get("lease_expires_at")
    if lease_value is None:
        return False
    return parse_event_timestamp(lease_value) == leased_until
