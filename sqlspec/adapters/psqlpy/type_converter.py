"""PostgreSQL-specific helpers for the psqlpy adapter."""

from typing import Any

from psqlpy.extra_types import PgVector

__all__ = ("coerce_pgvector",)


def coerce_pgvector(value: Any) -> Any:
    """Wrap supported dense vector values using psqlpy's native vector encoder."""
    if value is None or isinstance(value, PgVector):
        return value
    if isinstance(value, (list, tuple)):
        return PgVector(list(value))
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        raw_list = tolist()
        if isinstance(raw_list, list):
            return PgVector(raw_list)
    return value
