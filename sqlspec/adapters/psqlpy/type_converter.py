"""PostgreSQL-specific helpers for the psqlpy adapter."""

from typing import TYPE_CHECKING, Any

from sqlspec.typing import PGVECTOR_INSTALLED, import_optional_attr

if TYPE_CHECKING:
    from sqlspec.adapters.psqlpy._typing import PsqlpyConnection as Connection

__all__ = ("coerce_pgvector", "register_pgvector")


def coerce_pgvector(value: Any) -> Any:
    """Coerce sequence or numpy array to psqlpy PgVector."""
    if value is None or not PGVECTOR_INSTALLED:
        return value
    pg_vector_cls = import_optional_attr("psqlpy.extra_types", "PgVector")
    if pg_vector_cls is None:
        return value
    try:
        if isinstance(value, pg_vector_cls):
            return value
        if isinstance(value, (list, tuple)):
            return pg_vector_cls(list(value))
        if hasattr(value, "tolist"):
            return pg_vector_cls(value.tolist())
    except Exception:
        return value
    return value


def register_pgvector(connection: "Connection") -> None:
    """Register pgvector type handlers on psqlpy connection.

    Args:
        connection: Psqlpy connection instance.
    """
    if not PGVECTOR_INSTALLED:
        return
