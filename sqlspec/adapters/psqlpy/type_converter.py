"""PostgreSQL-specific helpers for the psqlpy adapter."""

from typing import TYPE_CHECKING, Any

from sqlspec.typing import PGVECTOR_INSTALLED

if TYPE_CHECKING:
    from sqlspec.adapters.psqlpy._typing import PsqlpyConnection as Connection

__all__ = ("coerce_pgvector", "register_pgvector")


def coerce_pgvector(value: Any) -> Any:
    """Coerce sequence or numpy array to psqlpy PgVector."""
    if value is None or not PGVECTOR_INSTALLED:
        return value
    try:
        from psqlpy.extra_types import PgVector

        if isinstance(value, PgVector):
            return value
        if isinstance(value, (list, tuple)):
            return PgVector(list(value))
        if hasattr(value, "tolist"):
            return PgVector(value.tolist())
    except (ImportError, Exception):
        return value
    return value


def register_pgvector(connection: "Connection") -> None:
    """Register pgvector type handlers on psqlpy connection.

    Args:
        connection: Psqlpy connection instance.
    """
    if not PGVECTOR_INSTALLED:
        return
